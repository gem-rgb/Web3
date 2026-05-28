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
	"github.com/example/rms/server/internal/risk"
	"github.com/example/rms/server/internal/redisx"
	"github.com/example/rms/server/internal/ruleengine"
	"github.com/example/rms/server/internal/store"
	"github.com/example/rms/shared/platform/config"
	platformgrpc "github.com/example/rms/shared/platform/grpcx"
	platformhttp "github.com/example/rms/shared/platform/httpx"
	"github.com/example/rms/shared/platform/logging"
	"github.com/example/rms/shared/platform/messaging"
	"github.com/example/rms/shared/platform/observability"
	pb "github.com/example/rms/shared/proto"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type orderIngestionService struct {
	pb.UnimplementedOrderIngestionServiceServer

	logger      *slog.Logger
	registry    *observability.Registry
	engine      *risk.Engine
	cache       *cache.RiskCache
	store       *store.DecisionStore
	producer    *messaging.Producer
	ruleClient  *ruleengine.Client
	ruleCatalog  *ruleengine.Catalog
	closeOnce   sync.Once
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loader := config.New("RMS")
	logger := logging.New("order-ingestion-service", loader.String("ENVIRONMENT", "development"))
	registry := observability.NewRegistry()

	redisClient := redisx.New(
		loader.Endpoint("REDIS_ADDR", "127.0.0.1", 6379),
		redisx.WithTimeouts(250*time.Millisecond, 250*time.Millisecond, 250*time.Millisecond),
	)
	riskCache := cache.NewRiskCache(redisClient, "rms")
	decisionStore, err := store.NewDecisionStore(loader.String("DECISION_STORE_PATH", "data/risk-decisions.jsonl"))
	if err != nil {
		logger.Error("failed to open decision store", "error", err)
		os.Exit(1)
	}
	brokers := splitCSV(loader.String("KAFKA_BROKERS", "127.0.0.1:9092"))
	producer := messaging.NewProducer(brokers, "")
	ruleCatalog := ruleengine.NewCatalog(ruleengine.DefaultRules(), "built-in-v1")
	ruleClient := ruleengine.NewClient(loader.String("RULE_ENGINE_ADDR", "127.0.0.1:50064"))

	engine := &risk.Engine{
		Cache:               riskCache,
		Rules:               ruleCatalog,
		Store:               nil,
		Events:              producer,
		Source:              "order-ingestion-service",
		Version:             "v1",
		MaxOrderSize:        int32(loader.Int("MAX_ORDER_SIZE", 10000)),
		MaxLeverage:         parseFloatConfig(loader.String("MAX_LEVERAGE_LIMIT", "4.0"), 4.0),
		MaxOrdersPerMinute:  loader.Int("MAX_ORDERS_PER_MINUTE", 240),
		FatFingerPct:        parseFloatConfig(loader.String("FAT_FINGER_PCT", "0.08"), 0.08),
		DuplicateWindow:     loader.Duration("DUPLICATE_WINDOW", 30*time.Second),
		FrequencyWindow:     loader.Duration("FREQUENCY_WINDOW", time.Minute),
		DecisionTTL:         loader.Duration("DECISION_TTL", pb.DecisionTTL()),
		MarketHoursRequired:  loader.Bool("MARKET_HOURS_REQUIRED", true),
	}

	svc := &orderIngestionService{
		logger:     logger,
		registry:   registry,
		engine:     engine,
		cache:      riskCache,
		store:      decisionStore,
		producer:   producer,
		ruleClient: ruleClient,
		ruleCatalog: ruleCatalog,
	}

	go svc.refreshRulesLoop(ctx)

	httpAddr := loader.String("HTTP_ADDR", ":8082")
	go serveHTTP(ctx, svc, httpAddr)

	grpcAddr := loader.String("GRPC_ADDR", ":50062")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to open grpc listener", "error", err)
		os.Exit(1)
	}
	grpcServer := platformgrpc.NewServer(platformgrpc.ServerConfig{ServiceName: "order-ingestion-service", Logger: logger, Metrics: registry})
	pb.RegisterOrderIngestionServiceServer(grpcServer, svc)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("order-ingestion-service", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("starting order ingestion grpc server", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("order ingestion grpc server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = shutdownCtx
	grpcServer.GracefulStop()
	svc.close()
	logger.Info("order ingestion service stopped")
}

func (s *orderIngestionService) SubmitOrder(ctx context.Context, req *pb.OrderIngestionRequest) (*pb.OrderIngestionResponse, error) {
	if req == nil || req.Order == nil {
		return nil, errors.New("order request is required")
	}

	start := time.Now().UTC()
	order := cloneOrder(req.Order)
	account := cloneAccount(req.Account)
	if account == nil {
		account = syntheticAccount(order.AccountId)
	}
	if order.Metadata == nil {
		order.Metadata = map[string]string{}
	}
	if account.Metadata == nil {
		account.Metadata = map[string]string{}
	}
	tenantID := firstNonEmpty(req.TenantID, tenantFromMetadata(order.Metadata, account.Metadata), "default")
	trace := observability.TraceFromContext(ctx)
	correlationID := firstNonEmpty(req.CorrelationID, trace.RequestID, order.Metadata["correlation_id"], account.Metadata["correlation_id"])
	idempotencyKey := firstNonEmpty(req.IdempotencyKey, order.Metadata["idempotency_key"], order.OrderId, orderFingerprint(order))

	order.Metadata["tenant_id"] = tenantID
	order.Metadata["correlation_id"] = correlationID
	order.Metadata["idempotency_key"] = idempotencyKey
	account.Metadata["tenant_id"] = tenantID
	account.Metadata["correlation_id"] = correlationID

	if err := s.publishIngressEvents(ctx, req, order, account, tenantID, correlationID, trace.TraceID, idempotencyKey); err != nil {
		s.logger.Warn("ingress event publish failed", "order_id", order.OrderId, "error", err.Error())
	}

	decision, err := s.engine.Evaluate(ctx, &pb.OrderRequest{Order: order, Account: account}, risk.Options{
		TenantID:        tenantID,
		CorrelationID:   correlationID,
		TraceID:         trace.TraceID,
		IdempotencyKey:  idempotencyKey,
		Source:          "order-ingestion-service",
		RecordState:     true,
		CacheDecision:   true,
		PersistDecision: false,
		PublishEvents:   false,
	})
	if err != nil {
		s.registry.Counter("rms_order_ingestion_errors_total", "order ingestion errors").Inc()
		return nil, err
	}

	s.registry.Counter("rms_order_ingestion_requests_total", "order ingestion requests").Inc()
	if !decision.Approved {
		s.registry.Counter("rms_order_ingestion_rejections_total", "order ingestion rejections").Inc()
	}
	s.registry.Histogram("rms_order_ingestion_latency_micros", "order ingestion latency", []float64{250, 500, 1000, 2500, 5000, 10000}).Observe(float64(decision.LatencyMicros))

	state := pb.OrderStateEvaluated
	if decision.Approved {
		state = pb.OrderStateApproved
	} else {
		state = pb.OrderStateRejected
	}

	return &pb.OrderIngestionResponse{
		OrderID:       order.OrderId,
		State:         state,
		Accepted:      true,
		Decision:      decision,
		CorrelationID: correlationID,
		TraceID:       trace.TraceID,
		Timestamp:     time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *orderIngestionService) GetOrderDecision(ctx context.Context, req *pb.OrderDecisionQueryRequest) (*pb.OrderDecisionQueryResponse, error) {
	if req == nil || req.OrderID == "" {
		return nil, errors.New("order id is required")
	}
	tenantID := firstNonEmpty(req.TenantID, "default")
	if decision, ok, err := s.cache.LoadDecision(ctx, tenantID, req.OrderID); err == nil && ok {
		if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
			return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
		}
	}
	if req.CorrelationID != "" {
		if decision, ok, err := s.cache.LoadDecisionByCorrelation(ctx, tenantID, req.CorrelationID); err == nil && ok {
			if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
				return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
			}
		}
	}
	if decision, ok := s.store.GetByOrderID(ctx, req.OrderID); ok {
		if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
			return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
		}
	}
	if req.CorrelationID != "" {
		if decision, ok := s.store.GetByCorrelationID(ctx, req.CorrelationID); ok {
			if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
				return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
			}
		}
	}
	return &pb.OrderDecisionQueryResponse{Found: false, Timestamp: time.Now().UTC().UnixMilli()}, nil
}

func (s *orderIngestionService) StreamOrderDecisions(req *pb.OrderDecisionQueryRequest, stream pb.OrderIngestionService_StreamOrderDecisionsServer) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	seen := map[string]struct{}{}
	tenantID := ""
	if req != nil {
		tenantID = firstNonEmpty(req.TenantID)
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			decisions := s.store.ListRecent(stream.Context(), 32)
			for _, decision := range decisions {
				if decision == nil {
					continue
				}
				if req != nil && req.OrderID != "" && !strings.EqualFold(req.OrderID, decision.OrderID) {
					continue
				}
				if req != nil && req.CorrelationID != "" && !strings.EqualFold(req.CorrelationID, decision.CorrelationID) {
					continue
				}
				if tenantID != "" && decision.TenantID != "" && !strings.EqualFold(tenantID, decision.TenantID) {
					continue
				}
				if _, ok := seen[decision.DecisionID]; ok {
					continue
				}
				seen[decision.DecisionID] = struct{}{}
				resp := &pb.OrderIngestionResponse{
					OrderID:       decision.OrderID,
					State:         orderStateFromDecision(decision),
					Accepted:      true,
					Decision:      decision,
					CorrelationID: decision.CorrelationID,
					TraceID:       decision.TraceID,
					Timestamp:     time.Now().UTC().UnixMilli(),
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
		}
	}
}

func (s *orderIngestionService) refreshRulesLoop(ctx context.Context) {
	if s.ruleClient == nil || s.ruleCatalog == nil {
		return
	}
	refresh := func() {
		snapshot, err := s.ruleClient.FetchSnapshot(ctx, "", "", "")
		if err != nil {
			s.logger.Warn("rule refresh failed", "error", err.Error())
			return
		}
		s.ruleCatalog.Replace(snapshot)
		_ = s.cache.StoreRuleVersion(context.Background(), "default", snapshot.Version, pb.DecisionTTL())
	}
	refresh()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func (s *orderIngestionService) publishIngressEvents(ctx context.Context, req *pb.OrderIngestionRequest, order *pb.Order, account *pb.Account, tenantID, correlationID, traceID, idempotencyKey string) error {
	if s.producer == nil {
		return nil
	}
	base := messaging.EventEnvelope{
		EventID:       fmt.Sprintf("%s-received", order.OrderId),
		EventType:     "order.received",
		AggregateID:   order.OrderId,
		TenantID:      tenantID,
		Source:        "order-ingestion-service",
		TraceID:       traceID,
		CorrelationID: correlationID,
		SchemaVersion: "v1",
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"order_id":        order.OrderId,
			"account_id":      order.AccountId,
			"idempotency_key": idempotencyKey,
		},
	}
	received := pb.OrderReceivedEvent{
		EventID:        base.EventID,
		OrderID:        order.OrderId,
		AccountID:      order.AccountId,
		TenantID:       tenantID,
		CorrelationID:  correlationID,
		TraceID:        traceID,
		IdempotencyKey: idempotencyKey,
		ReceivedAt:     time.Now().UTC().UnixMilli(),
		Source:         "order-ingestion-service",
		Order:          cloneOrder(order),
		Account:        cloneAccount(account),
		Metadata: map[string]string{
			"requested_by": req.RequestedBy,
		},
	}
	if err := s.producer.PublishJSON(ctx, "rms.orders.received.v1", messaging.PartitionKey(tenantID, order.AccountId, order.OrderId), base, received); err != nil {
		return err
	}
	requested := pb.RiskEvaluationRequestedEvent{
		EventID:        fmt.Sprintf("%s-evaluation-requested", order.OrderId),
		OrderID:        order.OrderId,
		AccountID:      order.AccountId,
		TenantID:       tenantID,
		CorrelationID:  correlationID,
		TraceID:        traceID,
		IdempotencyKey: idempotencyKey,
		RequestedAt:    time.Now().UTC().UnixMilli(),
		Reason:         "pre-trade order ingestion",
		Order:          cloneOrder(order),
		Account:        cloneAccount(account),
		Metadata: map[string]string{
			"source": "order-ingestion-service",
		},
	}
	return s.producer.PublishJSON(ctx, "rms.risk.evaluation.requested.v1", messaging.PartitionKey(tenantID, order.AccountId, order.OrderId), messaging.EventEnvelope{
		EventID:       requested.EventID,
		EventType:     "risk.evaluation.requested",
		AggregateID:   order.OrderId,
		TenantID:      tenantID,
		Source:        "order-ingestion-service",
		TraceID:       traceID,
		CorrelationID: correlationID,
		SchemaVersion: "v1",
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"source": "order-ingestion-service",
		},
	}, requested)
}

func (s *orderIngestionService) close() {
	s.closeOnce.Do(func() {
		if s.ruleClient != nil {
			_ = s.ruleClient.Close()
		}
		if s.producer != nil {
			_ = s.producer.Close()
		}
	})
}

func serveHTTP(ctx context.Context, svc *orderIngestionService, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"service": "order-ingestion-service",
			"status":  "ok",
		})
	})
	mux.HandleFunc("/metrics", platformhttp.MetricsHandler(svc.registry))
	mux.HandleFunc("/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var req pb.OrderIngestionRequest
			if err := platformhttp.DecodeJSON(r, &req); err != nil {
				platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
				return
			}
			resp, err := svc.SubmitOrder(r.Context(), &req)
			if err != nil {
				platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			platformhttp.JSON(w, http.StatusOK, resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/orders/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		orderID := strings.TrimPrefix(r.URL.Path, "/v1/orders/")
		if orderID == "" {
			http.NotFound(w, r)
			return
		}
		resp, err := svc.GetOrderDecision(r.Context(), &pb.OrderDecisionQueryRequest{OrderID: orderID})
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !resp.Found {
			http.NotFound(w, r)
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
		svc.logger.Info("starting order ingestion http server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			svc.logger.Error("order ingestion http server stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func cloneOrder(order *pb.Order) *pb.Order {
	if order == nil {
		return nil
	}
	copy := *order
	if order.Metadata != nil {
		copy.Metadata = make(map[string]string, len(order.Metadata))
		for k, v := range order.Metadata {
			copy.Metadata[k] = v
		}
	}
	return &copy
}

func cloneAccount(account *pb.Account) *pb.Account {
	if account == nil {
		return nil
	}
	copy := *account
	if account.Metadata != nil {
		copy.Metadata = make(map[string]string, len(account.Metadata))
		for k, v := range account.Metadata {
			copy.Metadata[k] = v
		}
	}
	return &copy
}

func syntheticAccount(accountID string) *pb.Account {
	if accountID == "" {
		accountID = "synthetic-account"
	}
	base := 300_000.0 + float64(len(accountID))*15_000.0
	return &pb.Account{
		AccountId:               accountID,
		UserId:                  "synthetic-user",
		Status:                  "ACTIVE",
		BuyingPower:             base * 2,
		CashBalance:             base,
		MarketValue:             base * 0.75,
		DayTradingBuyingPower:   base * 3,
		MaintenanceMarginExcess: base * 0.2,
		Metadata: map[string]string{
			"source": "synthetic",
			"desk":   "ingestion",
		},
	}
}

func orderStateFromDecision(decision *pb.RiskDecision) pb.OrderState {
	if decision == nil {
		return pb.OrderStateFailed
	}
	if decision.Approved {
		return pb.OrderStateApproved
	}
	return pb.OrderStateRejected
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func tenantFromMetadata(maps ...map[string]string) string {
	for _, values := range maps {
		if values == nil {
			continue
		}
		for _, key := range []string{"tenant_id", "tenant", "tenant-id"} {
			if value := strings.TrimSpace(values[key]); value != "" {
				return value
			}
		}
	}
	return ""
}

func traceOrRequestID(ctx context.Context) string {
	trace := observability.TraceFromContext(ctx)
	if trace.RequestID != "" {
		return trace.RequestID
	}
	return trace.TraceID
}

func orderFingerprint(order *pb.Order) string {
	if order == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%d|%.4f|%s|%s|%s", order.AccountId, order.Symbol, order.Quantity, order.Price, strings.ToUpper(order.Side), strings.ToUpper(order.OrderType), strings.ToUpper(order.TimeInForce))
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

func parseFloatConfig(raw string, fallback float64) float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	var parsed float64
	if _, err := fmt.Sscanf(raw, "%f", &parsed); err == nil {
		return parsed
	}
	return fallback
}
