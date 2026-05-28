package main

import (
	"context"
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
	"github.com/example/rms/server/internal/state"
	"github.com/example/rms/shared/platform/config"
	platformgrpc "github.com/example/rms/shared/platform/grpcx"
	platformhttp "github.com/example/rms/shared/platform/httpx"
	"github.com/example/rms/shared/platform/logging"
	"github.com/example/rms/shared/platform/messaging"
	"github.com/example/rms/shared/platform/observability"
	"github.com/example/rms/shared/platform/storage"
	pb "github.com/example/rms/shared/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type positionTrackingService struct {
	pb.UnimplementedPositionTrackingServiceServer

	logger      *slog.Logger
	registry    *observability.Registry
	repo        *state.Repository
	producer    *messaging.Producer
	closeOnce   sync.Once
	mu          sync.RWMutex
	subscribers map[int]chan *pb.PositionUpdateResponse
	nextSubID   int
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loader := config.New("RMS")
	logger := logging.New("position-tracking-service", loader.String("ENVIRONMENT", "development"))
	registry := observability.NewRegistry()

	db, err := postgresx.Open(positionPostgresConfig(loader))
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
	repo := state.NewRepository(db, riskCache, locks, logger, loader.Int("SNAPSHOT_EVERY", 25))

	svc := &positionTrackingService{
		logger:      logger,
		registry:    registry,
		repo:        repo,
		producer:    producer,
		subscribers: map[int]chan *pb.PositionUpdateResponse{},
	}

	httpAddr := loader.String("HTTP_ADDR", ":8085")
	go serveHTTP(ctx, svc, httpAddr)

	grpcAddr := loader.String("GRPC_ADDR", ":50056")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to open grpc listener", "error", err)
		os.Exit(1)
	}
	grpcServer := platformgrpc.NewServer(platformgrpc.ServerConfig{ServiceName: "position-tracking-service", Logger: logger, Metrics: registry}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterPositionTrackingServiceServer(grpcServer, svc)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("position-tracking-service", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("starting position tracking grpc server", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("position tracking grpc server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = shutdownCtx
	grpcServer.GracefulStop()
	svc.close()
	logger.Info("position tracking service stopped")
}

func (s *positionTrackingService) UpdatePosition(ctx context.Context, req *pb.PositionUpdateRequest) (*pb.PositionUpdateResponse, error) {
	if req == nil || req.Position == nil {
		return nil, fmt.Errorf("position update is required")
	}
	event := positionEventFromRequest(ctx, req)
	position, err := s.repo.ApplyPositionEvent(ctx, event)
	if err != nil {
		s.registry.Counter("rms_position_errors_total", "position update errors").Inc()
		return nil, err
	}
	s.registry.Counter("rms_position_updates_total", "position updates").Inc()
	if err := s.publishPositionEvent(ctx, event, position); err != nil {
		s.logger.Warn("position update publish failed", "error", err.Error())
	}
	resp := &pb.PositionUpdateResponse{
		Position:        clonePosition(position),
		Timestamp:       time.Now().UTC().UnixMilli(),
		SnapshotVersion: snapshotVersionFromPosition(position),
	}
	s.broadcast(resp)
	return resp, nil
}

func (s *positionTrackingService) GetPosition(ctx context.Context, req *pb.PositionRequest) (*pb.PositionResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("position request is required")
	}
	tenantID := defaultTenant(req.TenantId)
	position, ok, err := s.repo.GetPosition(ctx, tenantID, req.AccountId, req.Symbol)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &pb.PositionResponse{
			Position: &pb.Position{
				TenantId:  tenantID,
				AccountId: req.AccountId,
				Symbol:    req.Symbol,
				Timestamp: time.Now().UTC().UnixMilli(),
			},
			Timestamp:       time.Now().UTC().UnixMilli(),
			SnapshotVersion: "live",
		}, nil
	}
	return &pb.PositionResponse{
		Position:        clonePosition(position),
		Timestamp:       time.Now().UTC().UnixMilli(),
		SnapshotVersion: snapshotVersionFromPosition(position),
	}, nil
}

func (s *positionTrackingService) GetPositionsForAccount(ctx context.Context, req *pb.AccountPositionRequest) (*pb.AccountPositionResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account position request is required")
	}
	tenantID := defaultTenant(req.TenantId)
	positions, err := s.repo.ListPositions(ctx, tenantID, req.AccountId)
	if err != nil {
		return nil, err
	}
	if !req.IncludeClosed {
		positions = filterOpenPositions(positions)
	}
	return &pb.AccountPositionResponse{
		Positions:       clonePositions(positions),
		Timestamp:       time.Now().UTC().UnixMilli(),
		SnapshotVersion: snapshotVersionFromPositions(positions),
	}, nil
}

func (s *positionTrackingService) GetPositionSnapshot(ctx context.Context, req *pb.PositionSnapshotRequest) (*pb.PositionSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("position snapshot request is required")
	}
	snapshot, err := s.repo.LoadPositionSnapshot(ctx, defaultTenant(req.TenantID), req.AccountID, req.SnapshotVersion)
	if err != nil {
		if snapshot, saveErr := s.repo.SavePositionSnapshot(ctx, defaultTenant(req.TenantID), req.AccountID, firstNonEmpty(req.SnapshotVersion, "live")); saveErr == nil {
			return &pb.PositionSnapshotResponse{Snapshot: snapshot, Timestamp: time.Now().UTC().UnixMilli(), SnapshotVersion: snapshot.SnapshotVersion}, nil
		}
		return nil, err
	}
	if snapshot == nil {
		snapshot, err = s.repo.SavePositionSnapshot(ctx, defaultTenant(req.TenantID), req.AccountID, firstNonEmpty(req.SnapshotVersion, "live"))
		if err != nil {
			return nil, err
		}
	}
	return &pb.PositionSnapshotResponse{Snapshot: snapshot, Timestamp: time.Now().UTC().UnixMilli(), SnapshotVersion: snapshot.SnapshotVersion}, nil
}

func (s *positionTrackingService) ReplayPositionState(ctx context.Context, req *pb.PositionReplayRequest) (*pb.PositionReplayResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("position replay request is required")
	}
	snapshot, replayed, err := s.repo.ReplayPositionState(ctx, defaultTenant(req.TenantID), req.AccountID, req.FromSequence, req.ToSequence, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	version := ""
	if snapshot != nil {
		version = snapshot.SnapshotVersion
	}
	return &pb.PositionReplayResponse{
		Replayed:        replayed,
		Recovered:       replayed > 0,
		SnapshotVersion: version,
		Timestamp:       time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *positionTrackingService) ReconcilePositionState(ctx context.Context, req *pb.PositionReconciliationRequest) (*pb.PositionReconciliationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("position reconciliation request is required")
	}
	reconciled, driftDetected, driftCount, err := s.repo.ReconcilePositionState(ctx, defaultTenant(req.TenantID), req.AccountID, req.Symbol, req.Force)
	if err != nil {
		return nil, err
	}
	return &pb.PositionReconciliationResponse{
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

func (s *positionTrackingService) StreamPositionUpdates(req *pb.PositionRequest, stream pb.PositionTrackingService_StreamPositionUpdatesServer) error {
	subID, ch := s.subscribe()
	defer s.unsubscribe(subID)

	if req != nil {
		if resp, err := s.GetPosition(stream.Context(), req); err == nil && resp.Position != nil {
			if err := stream.Send(&pb.PositionUpdateResponse{Position: resp.Position, Timestamp: resp.Timestamp, SnapshotVersion: resp.SnapshotVersion}); err != nil {
				return err
			}
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case update := <-ch:
			if update == nil || update.Position == nil {
				continue
			}
			if req != nil && req.TenantId != "" && !strings.EqualFold(req.TenantId, update.Position.TenantId) {
				continue
			}
			if req != nil && req.AccountId != "" && !strings.EqualFold(req.AccountId, update.Position.AccountId) {
				continue
			}
			if req != nil && req.Symbol != "" && !strings.EqualFold(req.Symbol, update.Position.Symbol) {
				continue
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}
}

func (s *positionTrackingService) publishPositionEvent(ctx context.Context, event *pb.PositionEvent, position *pb.Position) error {
	if s.producer == nil || event == nil || position == nil {
		return nil
	}
	trace := observability.TraceFromContext(ctx)
	tenantID := defaultTenant(event.TenantID)
	key := messaging.PartitionKey(tenantID, event.AccountID, normalizeSymbol(event.Symbol))
	envelope := messaging.EventEnvelope{
		EventID:       event.EventID,
		EventType:     "position.updated",
		AggregateID:   keyString(tenantID, event.AccountID, event.Symbol),
		TenantID:      tenantID,
		Source:        "position-tracking-service",
		TraceID:       firstNonEmpty(event.TraceID, trace.TraceID),
		CorrelationID: firstNonEmpty(event.CorrelationID, trace.RequestID),
		SchemaVersion: "v1",
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"account_id": event.AccountID,
			"symbol":     event.Symbol,
		},
	}
	if err := s.producer.PublishJSON(ctx, "rms.positions.events.v1", key, envelope, event); err != nil {
		return err
	}
	return s.producer.PublishJSON(ctx, "rms.positions.updated.v1", key, envelope, &pb.PositionUpdate{
		Position:      clonePosition(position),
		IsNew:         strings.EqualFold(event.EventType, "OPEN"),
		TenantId:      tenantID,
		CorrelationID: firstNonEmpty(event.CorrelationID, trace.RequestID),
		TraceID:       firstNonEmpty(event.TraceID, trace.TraceID),
		Metadata:      cloneStringMap(position.Metadata),
	})
}

func (s *positionTrackingService) subscribe() (int, chan *pb.PositionUpdateResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSubID++
	id := s.nextSubID
	ch := make(chan *pb.PositionUpdateResponse, 128)
	s.subscribers[id] = ch
	return id, ch
}

func (s *positionTrackingService) unsubscribe(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

func (s *positionTrackingService) broadcast(update *pb.PositionUpdateResponse) {
	if update == nil || update.Position == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- clonePositionUpdateResponse(update):
		default:
		}
	}
}

func (s *positionTrackingService) close() {
	s.closeOnce.Do(func() {
		if s.producer != nil {
			_ = s.producer.Close()
		}
	})
}

func serveHTTP(ctx context.Context, svc *positionTrackingService, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"service": "position-tracking-service",
			"status":  "ok",
		})
	})
	mux.HandleFunc("/metrics", platformhttp.MetricsHandler(svc.registry))
	mux.HandleFunc("/v1/positions", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var req pb.PositionUpdateRequest
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
		case http.MethodGet:
			tenantID := r.URL.Query().Get("tenant_id")
			accountID := r.URL.Query().Get("account_id")
			symbol := r.URL.Query().Get("symbol")
			if symbol != "" {
				resp, err := svc.GetPosition(r.Context(), &pb.PositionRequest{TenantId: tenantID, AccountId: accountID, Symbol: symbol})
				if err != nil {
					platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
					return
				}
				platformhttp.JSON(w, http.StatusOK, resp)
				return
			}
			resp, err := svc.GetPositionsForAccount(r.Context(), &pb.AccountPositionRequest{TenantId: tenantID, AccountId: accountID, IncludeClosed: r.URL.Query().Get("include_closed") == "true"})
			if err != nil {
				platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			platformhttp.JSON(w, http.StatusOK, resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/positions/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		resp, err := svc.GetPositionSnapshot(r.Context(), &pb.PositionSnapshotRequest{
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
	mux.HandleFunc("/v1/positions/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req pb.PositionReplayRequest
		if err := platformhttp.DecodeJSON(r, &req); err != nil {
			platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := svc.ReplayPositionState(r.Context(), &req)
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		platformhttp.JSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/positions/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req pb.PositionReconciliationRequest
		if err := platformhttp.DecodeJSON(r, &req); err != nil {
			platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := svc.ReconcilePositionState(r.Context(), &req)
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
		svc.logger.Info("starting position tracking http server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			svc.logger.Error("position tracking http server stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func clonePositions(positions []*pb.Position) []*pb.Position {
	if len(positions) == 0 {
		return nil
	}
	result := make([]*pb.Position, 0, len(positions))
	for _, position := range positions {
		result = append(result, clonePosition(position))
	}
	return result
}

func clonePosition(position *pb.Position) *pb.Position {
	if position == nil {
		return nil
	}
	copy := *position
	copy.Metadata = cloneStringMap(position.Metadata)
	return &copy
}

func clonePositionUpdateResponse(resp *pb.PositionUpdateResponse) *pb.PositionUpdateResponse {
	if resp == nil {
		return nil
	}
	copy := *resp
	copy.Position = clonePosition(resp.Position)
	return &copy
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

func positionEventFromRequest(ctx context.Context, req *pb.PositionUpdateRequest) *pb.PositionEvent {
	trace := observability.TraceFromContext(ctx)
	position := req.Position
	tenantID := defaultTenant(firstNonEmpty(req.TenantId, position.TenantId))
	metadata := cloneStringMap(position.Metadata)
	if req.Metadata != nil {
		if metadata == nil {
			metadata = map[string]string{}
		}
		for key, value := range req.Metadata {
			metadata[key] = value
		}
	}
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata["tenant_id"] = tenantID
	metadata["source"] = firstNonEmpty(metadata["source"], "position-tracking-service")
	eventType := "UPDATE"
	if req.IsNew {
		eventType = "OPEN"
	}
	price := position.AveragePrice
	if price <= 0 {
		price = position.MarketPrice
	}
	occurredAt := time.Now().UTC().UnixMilli()
	return &pb.PositionEvent{
		EventID:       fmt.Sprintf("%s:%s:%s:%d", tenantID, position.AccountId, normalizeSymbol(position.Symbol), occurredAt),
		TenantID:      tenantID,
		AccountID:     position.AccountId,
		Symbol:        position.Symbol,
		Sector:        position.Sector,
		EventType:     eventType,
		QuantityDelta: position.Quantity,
		Price:         price,
		MarketPrice:   position.MarketPrice,
		Source:        firstNonEmpty(metadata["source"], "position-tracking-service"),
		OccurredAt:    occurredAt,
		CorrelationID: firstNonEmpty(req.CorrelationID, trace.RequestID),
		TraceID:       firstNonEmpty(req.TraceID, trace.TraceID),
		Metadata:      metadata,
	}
}

func snapshotVersionFromPosition(position *pb.Position) string {
	if position == nil {
		return "live"
	}
	return firstNonEmpty(position.SnapshotVersion, "live")
}

func snapshotVersionFromPositions(positions []*pb.Position) string {
	for _, position := range positions {
		if position == nil {
			continue
		}
		if version := strings.TrimSpace(position.SnapshotVersion); version != "" {
			return version
		}
	}
	return "live"
}

func filterOpenPositions(positions []*pb.Position) []*pb.Position {
	if len(positions) == 0 {
		return nil
	}
	result := make([]*pb.Position, 0, len(positions))
	for _, position := range positions {
		if position == nil || position.Quantity == 0 {
			continue
		}
		result = append(result, position)
	}
	return result
}

func normalizeSymbol(raw string) string {
	return strings.ToUpper(strings.TrimSpace(raw))
}

func positionPostgresConfig(loader config.Loader) storage.PostgresConfig {
	return storage.PostgresConfig{
		Host:            loader.String("POSTGRES_HOST", "127.0.0.1"),
		Port:            loader.Int("POSTGRES_PORT", 5432),
		Database:        loader.String("POSTGRES_DB", "rms_db"),
		Username:        loader.String("POSTGRES_USER", "rms_user"),
		Password:        loader.String("POSTGRES_PASSWORD", "rms_password"),
		SSLMode:         loader.String("POSTGRES_SSLMODE", "disable"),
		ApplicationName: "position-tracking-service",
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func defaultTenant(raw string) string {
	if value := strings.TrimSpace(raw); value != "" {
		return value
	}
	return "default"
}

func keyString(parts ...string) string {
	return strings.Join(parts, ":")
}
