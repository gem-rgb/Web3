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
	"github.com/example/rms/server/internal/redisx"
	"github.com/example/rms/server/internal/risk"
	"github.com/example/rms/server/internal/retry"
	"github.com/example/rms/server/internal/ruleengine"
	"github.com/example/rms/server/internal/store"
	"github.com/example/rms/server/internal/worker"
	"github.com/example/rms/shared/platform/config"
	platformgrpc "github.com/example/rms/shared/platform/grpcx"
	platformhttp "github.com/example/rms/shared/platform/httpx"
	"github.com/example/rms/shared/platform/logging"
	"github.com/example/rms/shared/platform/messaging"
	"github.com/example/rms/shared/platform/observability"
	pb "github.com/example/rms/shared/proto"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type riskEvaluationService struct {
	pb.UnimplementedRiskEvaluationServiceServer

	logger      *slog.Logger
	registry    *observability.Registry
	engine      *risk.Engine
	cache       *cache.RiskCache
	store       *store.DecisionStore
	producer    *messaging.Producer
	consumer    *messaging.Consumer
	ruleClient  *ruleengine.Client
	ruleCatalog *ruleengine.Catalog
	pool        *worker.Pool
	closeOnce   sync.Once
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loader := config.New("RMS")
	logger := logging.New("risk-evaluation-service", loader.String("ENVIRONMENT", "development"))
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
	consumer := messaging.NewConsumer(brokers, "rms.orders.received.v1", loader.String("KAFKA_GROUP_ID", "risk-evaluation-service"), 1, 10*1024*1024)
	ruleCatalog := ruleengine.NewCatalog(ruleengine.DefaultRules(), "built-in-v1")
	ruleClient := ruleengine.NewClient(loader.String("RULE_ENGINE_ADDR", "127.0.0.1:50064"))

	engine := &risk.Engine{
		Cache:               riskCache,
		Rules:               ruleCatalog,
		Store:               decisionStore,
		Events:              producer,
		Source:              "risk-evaluation-service",
		Version:             "v1",
		MaxOrderSize:        int32(loader.Int("MAX_ORDER_SIZE", 10000)),
		MaxLeverage:         parseFloatConfig(loader.String("MAX_LEVERAGE_LIMIT", "4.0"), 4.0),
		MaxOrdersPerMinute:  loader.Int("MAX_ORDERS_PER_MINUTE", 240),
		FatFingerPct:        parseFloatConfig(loader.String("FAT_FINGER_PCT", "0.08"), 0.08),
		DuplicateWindow:     loader.Duration("DUPLICATE_WINDOW", 30*time.Second),
		FrequencyWindow:     loader.Duration("FREQUENCY_WINDOW", time.Minute),
		DecisionTTL:         loader.Duration("DECISION_TTL", pb.DecisionTTL()),
		MarketHoursRequired: loader.Bool("MARKET_HOURS_REQUIRED", true),
	}

	svc := &riskEvaluationService{
		logger:      logger,
		registry:    registry,
		engine:      engine,
		cache:       riskCache,
		store:       decisionStore,
		producer:    producer,
		consumer:    consumer,
		ruleClient:  ruleClient,
		ruleCatalog: ruleCatalog,
		pool:        worker.New(loader.Int("RISK_WORKERS", 8), loader.Int("RISK_QUEUE", 512), logger, retry.Policy{Attempts: 1}),
	}

	go svc.refreshRulesLoop(ctx)
	svc.pool.Start(ctx)
	go svc.consumeOrders(ctx)

	httpAddr := loader.String("HTTP_ADDR", ":8083")
	go serveHTTP(ctx, svc, httpAddr)

	grpcAddr := loader.String("GRPC_ADDR", ":50063")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to open grpc listener", "error", err)
		os.Exit(1)
	}
	grpcServer := platformgrpc.NewServer(platformgrpc.ServerConfig{ServiceName: "risk-evaluation-service", Logger: logger, Metrics: registry})
	pb.RegisterRiskEvaluationServiceServer(grpcServer, svc)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("risk-evaluation-service", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("starting risk evaluation grpc server", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("risk evaluation grpc server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = shutdownCtx
	grpcServer.GracefulStop()
	svc.close()
	logger.Info("risk evaluation service stopped")
}

func (s *riskEvaluationService) EvaluateOrder(ctx context.Context, req *pb.OrderRequest) (*pb.OrderResponse, error) {
	if req == nil || req.Order == nil {
		return nil, errors.New("order request is required")
	}
	tenantID := tenantFromMetadata(req.Order.Metadata, metadataFromAccount(req.Account))
	correlationID := firstNonEmpty(req.Order.Metadata["correlation_id"], traceOrRequestID(ctx))
	decision, err := s.engine.Evaluate(ctx, req, risk.Options{
		TenantID:        tenantID,
		CorrelationID:   correlationID,
		TraceID:         observability.TraceFromContext(ctx).TraceID,
		IdempotencyKey:  firstNonEmpty(req.Order.Metadata["idempotency_key"], req.Order.OrderId, orderFingerprint(req.Order)),
		Source:          "risk-evaluation-service",
		RecordState:     true,
		CacheDecision:   true,
		PersistDecision: true,
		PublishEvents:   true,
	})
	if err != nil {
		s.registry.Counter("rms_risk_errors_total", "risk evaluation errors").Inc()
		return nil, err
	}
	s.registry.Counter("rms_risk_requests_total", "risk evaluation requests").Inc()
	if decision.Approved {
		s.registry.Counter("rms_risk_approved_total", "approved risk decisions").Inc()
	} else {
		s.registry.Counter("rms_risk_rejected_total", "rejected risk decisions").Inc()
	}
	s.registry.Histogram("rms_risk_latency_micros", "risk evaluation latency", []float64{250, 500, 1000, 2500, 5000, 10000}).Observe(float64(decision.LatencyMicros))
	return &pb.OrderResponse{
		OrderId:      decision.OrderID,
		Approved:     decision.Approved,
		RejectReason: decision.RejectReason,
		Violations:   decision.Violations,
		Timestamp:    time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *riskEvaluationService) BatchEvaluateOrders(ctx context.Context, req *pb.BatchOrderRequest) (*pb.BatchOrderResponse, error) {
	if req == nil {
		return nil, errors.New("batch request is required")
	}
	responses := make([]*pb.OrderResponse, 0, len(req.Orders))
	for i, order := range req.Orders {
		var account *pb.Account
		if i < len(req.Accounts) {
			account = req.Accounts[i]
		}
		resp, err := s.EvaluateOrder(ctx, &pb.OrderRequest{Order: order, Account: account})
		if err != nil {
			return nil, err
		}
		responses = append(responses, resp)
	}
	return &pb.BatchOrderResponse{
		Responses: responses,
		Timestamp: time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *riskEvaluationService) GetDecision(ctx context.Context, req *pb.OrderDecisionQueryRequest) (*pb.OrderDecisionQueryResponse, error) {
	if req == nil {
		return nil, errors.New("decision query request is required")
	}
	tenantID := firstNonEmpty(req.TenantID, "default")
	if req.OrderID != "" {
		if decision, ok, err := s.cache.LoadDecision(ctx, tenantID, req.OrderID); err == nil && ok {
			if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
				return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
			}
		}
	}
	if req.CorrelationID != "" {
		if decision, ok, err := s.cache.LoadDecisionByCorrelation(ctx, tenantID, req.CorrelationID); err == nil && ok {
			if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
				return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
			}
		}
	}
	if req.OrderID != "" {
		if decision, ok := s.store.GetByOrderID(ctx, req.OrderID); ok {
			if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
				return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
			}
		}
	}
	if req.CorrelationID != "" {
		if decision, ok := s.store.GetByCorrelationID(ctx, req.CorrelationID); ok {
			if tenantID == "" || decision.TenantID == "" || strings.EqualFold(decision.TenantID, tenantID) {
				return &pb.OrderDecisionQueryResponse{Decision: decision, Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
			}
		}
	}
	if req.AccountID != "" {
		if decisions := s.store.ListByAccount(ctx, req.AccountID, 1); len(decisions) > 0 {
			if tenantID == "" || decisions[0].TenantID == "" || strings.EqualFold(decisions[0].TenantID, tenantID) {
				return &pb.OrderDecisionQueryResponse{Decision: decisions[0], Found: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
			}
		}
	}
	return &pb.OrderDecisionQueryResponse{Found: false, Timestamp: time.Now().UTC().UnixMilli()}, nil
}

func (s *riskEvaluationService) StreamOrderEvaluations(stream pb.RiskEvaluationService_StreamOrderEvaluationsServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		resp, err := s.EvaluateOrder(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *riskEvaluationService) consumeOrders(ctx context.Context) {
	for {
		msg, err := s.consumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("kafka consumer read failed", "error", err.Error())
			time.Sleep(250 * time.Millisecond)
			continue
		}
		message := msg
		for {
			if err := s.pool.Submit(func(jobCtx context.Context) error {
				return s.handleKafkaOrder(jobCtx, message)
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

func (s *riskEvaluationService) handleKafkaOrder(ctx context.Context, msg kafka.Message) error {
	var envelope messaging.EventEnvelope
	if raw := headerValue(msg.Headers, "event-envelope"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
			return s.publishDLQ(ctx, msg, envelope, fmt.Errorf("decode envelope: %w", err))
		}
	}
	if envelope.EventID == "" {
		envelope.EventID = fmt.Sprintf("%s-%d", string(msg.Key), time.Now().UTC().UnixMilli())
	}
	var received pb.OrderReceivedEvent
	if err := json.Unmarshal(msg.Value, &received); err != nil {
		return s.publishDLQ(ctx, msg, envelope, fmt.Errorf("decode order event: %w", err))
	}

	policy := retry.DefaultPolicy()
	policy.Attempts = 3
	err := retry.Do(ctx, policy, func() error {
		_, evalErr := s.engine.Evaluate(ctx, &pb.OrderRequest{Order: received.Order, Account: received.Account}, risk.Options{
			TenantID:        firstNonEmpty(received.TenantID, tenantFromMetadata(orderMetadata(received.Order), metadataFromAccount(received.Account))),
			CorrelationID:   firstNonEmpty(received.CorrelationID, envelope.CorrelationID, traceOrRequestID(ctx)),
			TraceID:         firstNonEmpty(received.TraceID, envelope.TraceID, observability.TraceFromContext(ctx).TraceID),
			IdempotencyKey:  firstNonEmpty(received.IdempotencyKey, received.Order.OrderId, orderFingerprint(received.Order)),
			Source:          "risk-evaluation-service",
			RecordState:     false,
			CacheDecision:   true,
			PersistDecision: true,
			PublishEvents:   true,
		})
		return evalErr
	})
	if err != nil {
		return s.publishDLQ(ctx, msg, envelope, err)
	}
	return nil
}

func (s *riskEvaluationService) publishDLQ(ctx context.Context, msg kafka.Message, envelope messaging.EventEnvelope, cause error) error {
	if s.producer == nil {
		return cause
	}
	eventID := envelope.EventID
	if eventID == "" {
		eventID = fmt.Sprintf("%s-%d", string(msg.Key), msg.Offset)
	}
	aggregateID := envelope.AggregateID
	if aggregateID == "" {
		aggregateID = string(msg.Key)
	}
	payload := map[string]any{
		"source_topic":  msg.Topic,
		"partition":     msg.Partition,
		"offset":        msg.Offset,
		"key":           string(msg.Key),
		"error":         cause.Error(),
		"envelope":      envelope,
		"payload":       json.RawMessage(msg.Value),
		"published_at":  time.Now().UTC().Format(time.RFC3339Nano),
		"service":       "risk-evaluation-service",
	}
	dlqEnvelope := messaging.EventEnvelope{
		EventID:       fmt.Sprintf("%s-dlq", eventID),
		EventType:     "order.received.dlq",
		AggregateID:   aggregateID,
		TenantID:      envelope.TenantID,
		Source:        "risk-evaluation-service",
		TraceID:       envelope.TraceID,
		CorrelationID: envelope.CorrelationID,
		SchemaVersion: "v1",
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"reason": cause.Error(),
		},
	}
	if err := s.producer.PublishJSON(ctx, "rms.orders.received.v1.dlq", messaging.PartitionKey(envelope.TenantID, envelope.AggregateID, fmt.Sprintf("%d", msg.Offset)), dlqEnvelope, payload); err != nil {
		return err
	}
	return cause
}

func (s *riskEvaluationService) close() {
	s.closeOnce.Do(func() {
		if s.ruleClient != nil {
			_ = s.ruleClient.Close()
		}
		if s.producer != nil {
			_ = s.producer.Close()
		}
		if s.consumer != nil {
			_ = s.consumer.Close()
		}
	})
}

func (s *riskEvaluationService) refreshRulesLoop(ctx context.Context) {
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

func serveHTTP(ctx context.Context, svc *riskEvaluationService, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"service": "risk-evaluation-service",
			"status":  "ok",
		})
	})
	mux.HandleFunc("/metrics", platformhttp.MetricsHandler(svc.registry))
	mux.HandleFunc("/v1/decisions/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		orderID := strings.TrimPrefix(r.URL.Path, "/v1/decisions/")
		if orderID == "" {
			http.NotFound(w, r)
			return
		}
		if decision, ok := svc.store.GetByOrderID(r.Context(), orderID); ok {
			platformhttp.JSON(w, http.StatusOK, decision)
			return
		}
		http.NotFound(w, r)
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
		svc.logger.Info("starting risk evaluation http server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			svc.logger.Error("risk evaluation http server stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func headerValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func orderMetadata(order *pb.Order) map[string]string {
	if order == nil {
		return nil
	}
	return order.Metadata
}

func metadataFromAccount(account *pb.Account) map[string]string {
	if account == nil {
		return nil
	}
	return account.Metadata
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func orderFingerprint(order *pb.Order) string {
	if order == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%d|%.4f|%s|%s|%s", order.AccountId, order.Symbol, order.Quantity, order.Price, strings.ToUpper(order.Side), strings.ToUpper(order.OrderType), strings.ToUpper(order.TimeInForce))
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
