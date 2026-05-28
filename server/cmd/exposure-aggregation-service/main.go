package main

import (
	"context"
	"errors"
	"fmt"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/example/rms/server/internal/cache"
	"github.com/example/rms/server/internal/lock"
	"github.com/example/rms/server/internal/postgresx"
	"github.com/example/rms/server/internal/redisx"
	"github.com/example/rms/server/internal/retry"
	"github.com/example/rms/server/internal/state"
	"github.com/example/rms/server/internal/worker"
	"github.com/example/rms/shared/platform/config"
	platformgrpc "github.com/example/rms/shared/platform/grpcx"
	platformhttp "github.com/example/rms/shared/platform/httpx"
	"github.com/example/rms/shared/platform/logging"
	"github.com/example/rms/shared/platform/messaging"
	"github.com/example/rms/shared/platform/observability"
	"github.com/example/rms/shared/platform/storage"
	pb "github.com/example/rms/shared/proto"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type exposureAggregationService struct {
	pb.UnimplementedExposureAggregationServiceServer

	logger      *slog.Logger
	registry    *observability.Registry
	repo        *state.Repository
	producer    *messaging.Producer
	consumer    *messaging.Consumer
	pool        *worker.Pool
	mu          sync.RWMutex
	subscribers map[int]chan *pb.AggregatedExposure
	nextSubID   int
	closeOnce   sync.Once
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loader := config.New("RMS")
	logger := logging.New("exposure-aggregation-service", loader.String("ENVIRONMENT", "development"))
	registry := observability.NewRegistry()

	db, err := postgresx.Open(exposurePostgresConfig(loader))
	if err != nil {
		logger.Error("failed to open postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	redisClient := redisx.New(
		loader.Endpoint("REDIS_ADDR", "127.0.0.1", 6379),
		redisx.WithTimeouts(250*time.Millisecond, 250*time.Millisecond, 250*time.Millisecond),
	)
	defer redisClient.Close()

	riskCache := cache.NewRiskCache(redisClient, "rms")
	locks := lock.New(redisClient, "rms")
	brokers := splitCSV(loader.String("KAFKA_BROKERS", "127.0.0.1:9092"))
	producer := messaging.NewProducer(brokers, loader.String("KAFKA_TOPIC_PREFIX", "rms"))
	consumer := messaging.NewConsumer(brokers, "rms.positions.updated.v1", loader.String("KAFKA_GROUP_ID", "exposure-aggregation-service"), 1, 10*1024*1024)
	repo := state.NewRepository(db, riskCache, locks, logger, loader.Int("SNAPSHOT_EVERY", 25))

	svc := &exposureAggregationService{
		logger:      logger,
		registry:    registry,
		repo:        repo,
		producer:    producer,
		consumer:    consumer,
		pool:        worker.New(loader.Int("EXPOSURE_WORKERS", 8), loader.Int("EXPOSURE_QUEUE", 512), logger, retry.Policy{Attempts: 3}),
		subscribers: map[int]chan *pb.AggregatedExposure{},
	}

	go svc.consumePositionUpdates(ctx)
	svc.pool.Start(ctx)

	httpAddr := loader.String("HTTP_ADDR", ":8086")
	go serveHTTP(ctx, svc, httpAddr)

	grpcAddr := loader.String("GRPC_ADDR", ":50055")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to open grpc listener", "error", err)
		os.Exit(1)
	}
	grpcServer := platformgrpc.NewServer(platformgrpc.ServerConfig{ServiceName: "exposure-aggregation-service", Logger: logger, Metrics: registry}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterExposureAggregationServiceServer(grpcServer, svc)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("exposure-aggregation-service", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("starting exposure aggregation grpc server", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("exposure aggregation grpc server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = shutdownCtx
	grpcServer.GracefulStop()
	svc.close()
	logger.Info("exposure aggregation service stopped")
}

func (s *exposureAggregationService) UpdatePosition(ctx context.Context, req *pb.PositionUpdate) (*pb.AggregatedExposure, error) {
	if req == nil || req.Position == nil {
		return nil, fmt.Errorf("position update is required")
	}
	if req.Position.TenantId == "" {
		req.Position.TenantId = firstNonEmpty(req.TenantId, req.Position.TenantId)
	}
	exposure, err := s.repo.ApplyExposureFromPosition(ctx, req.Position)
	if err != nil {
		s.registry.Counter("rms_exposure_errors_total", "exposure update errors").Inc()
		return nil, err
	}
	s.registry.Counter("rms_exposure_updates_total", "exposure updates").Inc()
	if exposure != nil {
		if err := s.publishExposureUpdate(ctx, exposure); err != nil {
			s.logger.Warn("exposure publish failed", "error", err.Error())
		}
		s.broadcast(exposure)
	}
	return cloneExposure(exposure), nil
}

func (s *exposureAggregationService) GetAccountExposure(ctx context.Context, req *pb.AccountExposureRequest) (*pb.AccountExposureResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account exposure request is required")
	}
	tenantID := defaultTenant(req.TenantId)
	exposures, err := s.repo.GetAccountExposure(ctx, tenantID, req.AccountId)
	if err != nil {
		return nil, err
	}
	return &pb.AccountExposureResponse{
		Exposures:       cloneExposureList(exposures),
		Timestamp:       time.Now().UTC().UnixMilli(),
		SnapshotVersion: snapshotVersionFromExposures(exposures),
	}, nil
}

func (s *exposureAggregationService) GetSymbolExposure(ctx context.Context, req *pb.SymbolExposureRequest) (*pb.SymbolExposureResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("symbol exposure request is required")
	}
	tenantID := defaultTenant(req.TenantId)
	exposures, err := s.repo.GetSymbolExposure(ctx, tenantID, req.Symbol)
	if err != nil {
		return nil, err
	}
	return &pb.SymbolExposureResponse{
		Exposures:       cloneExposureList(exposures),
		Timestamp:       time.Now().UTC().UnixMilli(),
		SnapshotVersion: snapshotVersionFromExposures(exposures),
	}, nil
}

func (s *exposureAggregationService) GetExposureSnapshot(ctx context.Context, req *pb.ExposureSnapshotRequest) (*pb.ExposureSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("exposure snapshot request is required")
	}
	snapshot, err := s.repo.LoadExposureSnapshot(ctx, defaultTenant(req.TenantID), req.AccountID, req.Symbol, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		exposures, err := s.repo.GetAccountExposure(ctx, defaultTenant(req.TenantID), req.AccountID)
		if err != nil {
			return nil, err
		}
		if len(exposures) == 0 {
			return &pb.ExposureSnapshotResponse{Timestamp: time.Now().UTC().UnixMilli()}, nil
		}
		latest := latestExposure(exposures)
		if latest == nil {
			return &pb.ExposureSnapshotResponse{Timestamp: time.Now().UTC().UnixMilli()}, nil
		}
		snapshot = &pb.ExposureSnapshot{
			SnapshotID:      fmt.Sprintf("exposure:%s:%s:%d", defaultTenant(req.TenantID), req.AccountID, time.Now().UTC().UnixMilli()),
			TenantID:        defaultTenant(req.TenantID),
			AccountID:       req.AccountID,
			Symbol:          latest.Symbol,
			Sector:          latest.Sector,
			Version:         latest.Version,
			EventOffset:     latest.Sequence,
			SnapshotVersion: firstNonEmpty(req.SnapshotVersion, "live"),
			Exposure:        cloneExposure(latest),
			Checksum:        checksumExposure(latest),
			CreatedAt:       time.Now().UTC().UnixMilli(),
			Metadata:        map[string]string{"source": "exposure-aggregation-service"},
		}
		if _, err := s.repo.SaveExposureSnapshot(ctx, snapshot.Exposure, snapshot.SnapshotVersion); err != nil {
			return nil, err
		}
	}
	return &pb.ExposureSnapshotResponse{Snapshot: snapshot, Timestamp: time.Now().UTC().UnixMilli(), SnapshotVersion: snapshot.SnapshotVersion}, nil
}

func (s *exposureAggregationService) ReplayExposureState(ctx context.Context, req *pb.ExposureReplayRequest) (*pb.ExposureReplayResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("exposure replay request is required")
	}
	exposures, replayed, err := s.repo.ReplayExposureState(ctx, defaultTenant(req.TenantID), req.AccountID, req.Symbol, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	for _, exposure := range exposures {
		if exposure == nil {
			continue
		}
		s.broadcast(exposure)
	}
	return &pb.ExposureReplayResponse{
		Replayed:        replayed,
		Recovered:       replayed > 0,
		SnapshotVersion: firstNonEmpty(req.SnapshotVersion, "live"),
		Timestamp:       time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *exposureAggregationService) ReconcileExposureState(ctx context.Context, req *pb.ExposureReconciliationRequest) (*pb.ExposureReconciliationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("exposure reconciliation request is required")
	}
	reconciled, driftDetected, driftCount, err := s.repo.ReconcileExposureState(ctx, defaultTenant(req.TenantID), req.AccountID, req.Symbol, req.Force)
	if err != nil {
		return nil, err
	}
	return &pb.ExposureReconciliationResponse{
		Reconciled:    reconciled,
		DriftDetected: driftDetected,
		DriftCount:    driftCount,
		Details: map[string]string{
			"account_id": req.AccountID,
			"symbol":     req.Symbol,
		},
		Timestamp: time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *exposureAggregationService) StreamAccountExposure(req *pb.AccountExposureRequest, stream pb.ExposureAggregationService_StreamAccountExposureServer) error {
	subID, ch := s.subscribe()
	defer s.unsubscribe(subID)

	if req != nil {
		if resp, err := s.GetAccountExposure(stream.Context(), req); err == nil {
			for _, exposure := range resp.Exposures {
				if exposure == nil {
					continue
				}
				if req.TenantId != "" && !strings.EqualFold(req.TenantId, exposure.TenantId) {
					continue
				}
				if err := stream.Send(exposure); err != nil {
					return err
				}
			}
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case exposure := <-ch:
			if exposure == nil {
				continue
			}
			if req != nil && req.TenantId != "" && !strings.EqualFold(req.TenantId, exposure.TenantId) {
				continue
			}
			if req != nil && req.AccountId != "" && !strings.EqualFold(req.AccountId, exposure.AccountId) {
				continue
			}
			if err := stream.Send(exposure); err != nil {
				return err
			}
		}
	}
}

func (s *exposureAggregationService) consumePositionUpdates(ctx context.Context) {
	if s.consumer == nil {
		return
	}
	for {
		msg, err := s.consumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("position update consume failed", "error", err.Error())
			time.Sleep(250 * time.Millisecond)
			continue
		}
		message := msg
		for {
			if err := s.pool.Submit(func(jobCtx context.Context) error {
				return s.handleKafkaMessage(jobCtx, message)
			}); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}

func (s *exposureAggregationService) handleKafkaMessage(ctx context.Context, msg kafka.Message) error {
	var update pb.PositionUpdate
	if err := json.Unmarshal(msg.Value, &update); err != nil {
		return err
	}
	if update.Position == nil {
		return nil
	}
	if update.Position.TenantId == "" {
		update.Position.TenantId = firstNonEmpty(update.TenantId, update.Position.TenantId)
	}
	exposure, err := s.repo.ApplyExposureFromPosition(ctx, update.Position)
	if err != nil {
		return err
	}
	if exposure != nil {
		s.broadcast(exposure)
	}
	return nil
}

func (s *exposureAggregationService) publishExposureUpdate(ctx context.Context, exposure *pb.AggregatedExposure) error {
	if s.producer == nil || exposure == nil {
		return nil
	}
	trace := observability.TraceFromContext(ctx)
	tenantID := defaultTenant(exposure.TenantId)
	key := messaging.PartitionKey(tenantID, exposure.AccountId, normalizeSymbol(exposure.Symbol))
	envelope := messaging.EventEnvelope{
		EventID:       fmt.Sprintf("%s:%s:%s:%d", tenantID, exposure.AccountId, normalizeSymbol(exposure.Symbol), time.Now().UTC().UnixMilli()),
		EventType:     "exposure.updated",
		AggregateID:   keyString(tenantID, exposure.AccountId, exposure.Symbol),
		TenantID:      tenantID,
		Source:        "exposure-aggregation-service",
		TraceID:       trace.TraceID,
		CorrelationID: trace.RequestID,
		SchemaVersion: "v1",
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"account_id": exposure.AccountId,
			"symbol":     exposure.Symbol,
		},
	}
	return s.producer.PublishJSON(ctx, "rms.exposures.updated.v1", key, envelope, exposure)
}

func (s *exposureAggregationService) subscribe() (int, chan *pb.AggregatedExposure) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSubID++
	id := s.nextSubID
	ch := make(chan *pb.AggregatedExposure, 128)
	s.subscribers[id] = ch
	return id, ch
}

func (s *exposureAggregationService) unsubscribe(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

func (s *exposureAggregationService) broadcast(exposure *pb.AggregatedExposure) {
	if exposure == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- cloneExposure(exposure):
		default:
		}
	}
}

func (s *exposureAggregationService) close() {
	s.closeOnce.Do(func() {
		if s.producer != nil {
			_ = s.producer.Close()
		}
		if s.consumer != nil {
			_ = s.consumer.Close()
		}
		if s.pool != nil {
			s.pool.Close()
		}
	})
}

func serveHTTP(ctx context.Context, svc *exposureAggregationService, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"service": "exposure-aggregation-service",
			"status":  "ok",
		})
	})
	mux.HandleFunc("/metrics", platformhttp.MetricsHandler(svc.registry))
	mux.HandleFunc("/v1/exposures", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			accountID := r.URL.Query().Get("account_id")
			symbol := r.URL.Query().Get("symbol")
			if symbol != "" {
				resp, err := svc.GetSymbolExposure(r.Context(), &pb.SymbolExposureRequest{Symbol: symbol})
				if err != nil {
					platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
					return
				}
				platformhttp.JSON(w, http.StatusOK, resp)
				return
			}
			resp, err := svc.GetAccountExposure(r.Context(), &pb.AccountExposureRequest{AccountId: accountID})
			if err != nil {
				platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			platformhttp.JSON(w, http.StatusOK, resp)
		case http.MethodPost:
			var req pb.PositionUpdate
			if err := platformhttp.DecodeJSON(r, &req); err != nil {
				platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
				return
			}
			resp, err := svc.UpdatePosition(r.Context(), &req)
			if err != nil {
				platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			platformhttp.JSON(w, http.StatusOK, resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/exposures/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		resp, err := svc.GetExposureSnapshot(r.Context(), &pb.ExposureSnapshotRequest{
			AccountID:       r.URL.Query().Get("account_id"),
			Symbol:          r.URL.Query().Get("symbol"),
			SnapshotVersion: r.URL.Query().Get("snapshot_version"),
		})
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		platformhttp.JSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/exposures/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req pb.ExposureReplayRequest
		if err := platformhttp.DecodeJSON(r, &req); err != nil {
			platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := svc.ReplayExposureState(r.Context(), &req)
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		platformhttp.JSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/exposures/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req pb.ExposureReconciliationRequest
		if err := platformhttp.DecodeJSON(r, &req); err != nil {
			platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := svc.ReconcileExposureState(r.Context(), &req)
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		platformhttp.JSON(w, http.StatusOK, resp)
	})
	handler := platformhttp.Chain(mux,
		platformhttp.Tracing,
		platformhttp.RequestID,
		platformhttp.Logging(svc.logger),
		platformhttp.SecurityHeaders,
		platformhttp.Recovery(svc.logger),
	)
	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		svc.logger.Info("starting exposure aggregation http server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			svc.logger.Error("exposure aggregation http server stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func cloneExposure(exposure *pb.AggregatedExposure) *pb.AggregatedExposure {
	if exposure == nil {
		return nil
	}
	copy := *exposure
	copy.Metadata = cloneStringMap(exposure.Metadata)
	return &copy
}

func cloneExposureList(exposures []*pb.AggregatedExposure) []*pb.AggregatedExposure {
	if len(exposures) == 0 {
		return nil
	}
	result := make([]*pb.AggregatedExposure, 0, len(exposures))
	for _, exposure := range exposures {
		result = append(result, cloneExposure(exposure))
	}
	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for k, v := range values {
		result[k] = v
	}
	return result
}

func defaultTenant(raw string) string {
	if value := strings.TrimSpace(raw); value != "" {
		return value
	}
	return "default"
}

func normalizeSymbol(raw string) string {
	return strings.ToUpper(strings.TrimSpace(raw))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func keyString(parts ...string) string {
	return strings.Join(parts, ":")
}

func exposurePostgresConfig(loader config.Loader) storage.PostgresConfig {
	return storage.PostgresConfig{
		Host:            loader.String("POSTGRES_HOST", "127.0.0.1"),
		Port:            loader.Int("POSTGRES_PORT", 5432),
		Database:        loader.String("POSTGRES_DB", "rms_db"),
		Username:        loader.String("POSTGRES_USER", "rms_user"),
		Password:        loader.String("POSTGRES_PASSWORD", "rms_password"),
		SSLMode:         loader.String("POSTGRES_SSLMODE", "disable"),
		ApplicationName: "exposure-aggregation-service",
		Schema:          "public",
	}
}

func splitCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}
