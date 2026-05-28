package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type portfolioRiskService struct {
	pb.UnimplementedPortfolioRiskServiceServer

	logger      *slog.Logger
	registry    *observability.Registry
	repo        *state.Repository
	producer    *messaging.Producer
	consumer    *messaging.Consumer
	pool        *worker.Pool
	mu          sync.RWMutex
	subscribers map[int]chan *pb.PortfolioRiskResponse
	nextSubID   int
	closeOnce   sync.Once
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loader := config.New("RMS")
	logger := logging.New("portfolio-risk-engine", loader.String("ENVIRONMENT", "development"))
	registry := observability.NewRegistry()

	db, err := postgresx.Open(portfolioPostgresConfig(loader))
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
	consumer := messaging.NewConsumer(brokers, "rms.exposures.updated.v1", loader.String("KAFKA_GROUP_ID", "portfolio-risk-engine"), 1, 10*1024*1024)
	repo := state.NewRepository(db, riskCache, locks, logger, loader.Int("SNAPSHOT_EVERY", 25))

	svc := &portfolioRiskService{
		logger:      logger,
		registry:    registry,
		repo:        repo,
		producer:    producer,
		consumer:    consumer,
		pool:        worker.New(loader.Int("PORTFOLIO_WORKERS", 8), loader.Int("PORTFOLIO_QUEUE", 512), logger, retry.Policy{Attempts: 3}),
		subscribers: map[int]chan *pb.PortfolioRiskResponse{},
	}

	go svc.consumeExposureUpdates(ctx)
	svc.pool.Start(ctx)

	httpAddr := loader.String("HTTP_ADDR", ":8087")
	go serveHTTP(ctx, svc, httpAddr)

	grpcAddr := loader.String("GRPC_ADDR", ":50058")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to open grpc listener", "error", err)
		os.Exit(1)
	}
	grpcServer := platformgrpc.NewServer(platformgrpc.ServerConfig{ServiceName: "portfolio-risk-engine", Logger: logger, Metrics: registry}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterPortfolioRiskServiceServer(grpcServer, svc)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("portfolio-risk-engine", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("starting portfolio risk grpc server", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("portfolio risk grpc server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = shutdownCtx
	grpcServer.GracefulStop()
	svc.close()
	logger.Info("portfolio risk service stopped")
}

func (s *portfolioRiskService) CalculatePortfolioRisk(ctx context.Context, req *pb.PortfolioRiskRequest) (*pb.PortfolioRiskResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("portfolio risk request is required")
	}
	summary, err := s.repo.CalculatePortfolioRisk(ctx, req)
	if err != nil {
		s.registry.Counter("rms_portfolio_errors_total", "portfolio risk errors").Inc()
		return nil, err
	}
	resp := &pb.PortfolioRiskResponse{Summary: clonePortfolioSummary(summary), Timestamp: time.Now().UTC().UnixMilli()}
	if err := s.publishPortfolioRisk(ctx, resp); err != nil {
		s.logger.Warn("portfolio risk publish failed", "error", err.Error())
	}
	s.broadcast(resp)
	return resp, nil
}

func (s *portfolioRiskService) GetPortfolioSnapshot(ctx context.Context, req *pb.PortfolioSnapshotRequest) (*pb.PortfolioSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("portfolio snapshot request is required")
	}
	posSnapshot, expSnapshot, err := s.repo.BuildPortfolioSnapshot(ctx, defaultTenant(req.TenantID), req.AccountID, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	return &pb.PortfolioSnapshotResponse{
		Snapshot:  posSnapshot,
		Exposure:  expSnapshot,
		Timestamp: time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *portfolioRiskService) ReplayPortfolioState(ctx context.Context, req *pb.PortfolioReplayRequest) (*pb.PortfolioReplayResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("portfolio replay request is required")
	}
	resp, replayed, err := s.repo.ReplayPortfolioState(ctx, defaultTenant(req.TenantID), req.AccountID, req.FromSequence, req.ToSequence, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	if resp != nil && resp.Snapshot != nil && resp.Snapshot.AccountID != "" {
		summary, err := s.repo.CalculatePortfolioRisk(ctx, &pb.PortfolioRiskRequest{
			TenantID:            defaultTenant(req.TenantID),
			AccountID:           req.AccountID,
			IncludeCrossAccount: true,
			IncludeSector:       true,
			SnapshotVersion:     firstNonEmpty(req.SnapshotVersion, "live"),
		})
		if err == nil {
			s.broadcast(&pb.PortfolioRiskResponse{Summary: summary, Timestamp: time.Now().UTC().UnixMilli()})
		}
	}
	return &pb.PortfolioReplayResponse{
		Replayed:        replayed,
		Recovered:       replayed > 0,
		SnapshotVersion: firstNonEmpty(req.SnapshotVersion, "live"),
		Timestamp:       time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *portfolioRiskService) ReconcilePortfolioState(ctx context.Context, req *pb.PortfolioReconciliationRequest) (*pb.PortfolioReconciliationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("portfolio reconciliation request is required")
	}
	reconciled, driftDetected, driftCount, err := s.repo.ReconcilePortfolioState(ctx, defaultTenant(req.TenantID), req.AccountID, req.Strict, req.Force)
	if err != nil {
		return nil, err
	}
	return &pb.PortfolioReconciliationResponse{
		Reconciled:    reconciled,
		DriftDetected: driftDetected,
		DriftCount:    driftCount,
		Details: map[string]string{
			"account_id": req.AccountID,
		},
		Timestamp: time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *portfolioRiskService) StreamPortfolioRisk(req *pb.PortfolioRiskSubscriptionRequest, stream pb.PortfolioRiskService_StreamPortfolioRiskServer) error {
	subID, ch := s.subscribe()
	defer s.unsubscribe(subID)

	if req != nil {
		if resp, err := s.CalculatePortfolioRisk(stream.Context(), &pb.PortfolioRiskRequest{
			TenantID:            req.TenantID,
			AccountID:           req.AccountID,
			IncludeCrossAccount: true,
			IncludeSector:       true,
			Metadata:            req.Metadata,
		}); err == nil && resp != nil {
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case update := <-ch:
			if update == nil {
				continue
			}
			if req != nil && req.AccountID != "" && !strings.EqualFold(req.AccountID, update.Summary.AccountID) {
				continue
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}
}

func (s *portfolioRiskService) consumeExposureUpdates(ctx context.Context) {
	if s.consumer == nil {
		return
	}
	for {
		msg, err := s.consumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("exposure update consume failed", "error", err.Error())
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

func (s *portfolioRiskService) handleKafkaMessage(ctx context.Context, msg kafka.Message) error {
	var exposure pb.AggregatedExposure
	if err := json.Unmarshal(msg.Value, &exposure); err != nil {
		return err
	}
	req := &pb.PortfolioRiskRequest{
		TenantID:            defaultTenant(exposure.TenantId),
		AccountID:           exposure.AccountId,
		IncludeCrossAccount: true,
		IncludeSector:       true,
	}
	summary, err := s.repo.CalculatePortfolioRisk(ctx, req)
	if err != nil {
		return err
	}
	s.broadcast(&pb.PortfolioRiskResponse{Summary: summary, Timestamp: time.Now().UTC().UnixMilli()})
	return nil
}

func (s *portfolioRiskService) publishPortfolioRisk(ctx context.Context, resp *pb.PortfolioRiskResponse) error {
	if s.producer == nil || resp == nil || resp.Summary == nil {
		return nil
	}
	trace := observability.TraceFromContext(ctx)
	tenantID := defaultTenant(resp.Summary.TenantID)
	key := messaging.PartitionKey(tenantID, resp.Summary.AccountID)
	envelope := messaging.EventEnvelope{
		EventID:       fmt.Sprintf("%s:%s:%d", tenantID, resp.Summary.AccountID, time.Now().UTC().UnixMilli()),
		EventType:     "portfolio.risk.updated",
		AggregateID:   keyString(tenantID, resp.Summary.AccountID),
		TenantID:      tenantID,
		Source:        "portfolio-risk-engine",
		TraceID:       trace.TraceID,
		CorrelationID: trace.RequestID,
		SchemaVersion: "v1",
		OccurredAt:    time.Now().UTC(),
	}
	return s.producer.PublishJSON(ctx, "rms.portfolio.risk.updated.v1", key, envelope, resp)
}

func (s *portfolioRiskService) subscribe() (int, chan *pb.PortfolioRiskResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSubID++
	id := s.nextSubID
	ch := make(chan *pb.PortfolioRiskResponse, 128)
	s.subscribers[id] = ch
	return id, ch
}

func (s *portfolioRiskService) unsubscribe(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

func (s *portfolioRiskService) broadcast(resp *pb.PortfolioRiskResponse) {
	if resp == nil || resp.Summary == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- clonePortfolioRiskResponse(resp):
		default:
		}
	}
}

func (s *portfolioRiskService) close() {
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

func serveHTTP(ctx context.Context, svc *portfolioRiskService, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"service": "portfolio-risk-engine",
			"status":  "ok",
		})
	})
	mux.HandleFunc("/metrics", platformhttp.MetricsHandler(svc.registry))
	mux.HandleFunc("/v1/portfolio/risk", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			resp, err := svc.CalculatePortfolioRisk(r.Context(), &pb.PortfolioRiskRequest{
				TenantID:            r.URL.Query().Get("tenant_id"),
				AccountID:           r.URL.Query().Get("account_id"),
				IncludeCrossAccount: true,
				IncludeSector:       true,
				SnapshotVersion:     r.URL.Query().Get("snapshot_version"),
			})
			if err != nil {
				platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			platformhttp.JSON(w, http.StatusOK, resp)
		case http.MethodPost:
			var req pb.PortfolioRiskRequest
			if err := platformhttp.DecodeJSON(r, &req); err != nil {
				platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
				return
			}
			resp, err := svc.CalculatePortfolioRisk(r.Context(), &req)
			if err != nil {
				platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			platformhttp.JSON(w, http.StatusOK, resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/portfolio/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		resp, err := svc.GetPortfolioSnapshot(r.Context(), &pb.PortfolioSnapshotRequest{
			TenantID:        r.URL.Query().Get("tenant_id"),
			AccountID:       r.URL.Query().Get("account_id"),
			SnapshotVersion: r.URL.Query().Get("snapshot_version"),
		})
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		platformhttp.JSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/portfolio/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req pb.PortfolioReplayRequest
		if err := platformhttp.DecodeJSON(r, &req); err != nil {
			platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := svc.ReplayPortfolioState(r.Context(), &req)
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		platformhttp.JSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/portfolio/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req pb.PortfolioReconciliationRequest
		if err := platformhttp.DecodeJSON(r, &req); err != nil {
			platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := svc.ReconcilePortfolioState(r.Context(), &req)
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
		svc.logger.Info("starting portfolio risk http server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			svc.logger.Error("portfolio risk http server stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func clonePortfolioSummary(summary *pb.PortfolioRiskSummary) *pb.PortfolioRiskSummary {
	if summary == nil {
		return nil
	}
	copy := *summary
	copy.SymbolExposure = cloneFloatMap(summary.SymbolExposure)
	copy.SectorExposure = cloneFloatMap(summary.SectorExposure)
	copy.CrossAccountExposure = cloneFloatMap(summary.CrossAccountExposure)
	copy.Metadata = cloneStringMap(summary.Metadata)
	return &copy
}

func clonePortfolioRiskResponse(resp *pb.PortfolioRiskResponse) *pb.PortfolioRiskResponse {
	if resp == nil {
		return nil
	}
	copy := *resp
	copy.Summary = clonePortfolioSummary(resp.Summary)
	return &copy
}

func cloneFloatMap(values map[string]float64) map[string]float64 {
	if values == nil {
		return nil
	}
	result := make(map[string]float64, len(values))
	for k, v := range values {
		result[k] = v
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

func portfolioPostgresConfig(loader config.Loader) storage.PostgresConfig {
	return storage.PostgresConfig{
		Host:            loader.String("POSTGRES_HOST", "127.0.0.1"),
		Port:            loader.Int("POSTGRES_PORT", 5432),
		Database:        loader.String("POSTGRES_DB", "rms_db"),
		Username:        loader.String("POSTGRES_USER", "rms_user"),
		Password:        loader.String("POSTGRES_PASSWORD", "rms_password"),
		SSLMode:         loader.String("POSTGRES_SSLMODE", "disable"),
		ApplicationName: "portfolio-risk-engine",
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

