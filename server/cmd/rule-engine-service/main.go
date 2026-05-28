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
	"github.com/example/rms/server/internal/redisx"
	"github.com/example/rms/server/internal/risk"
	"github.com/example/rms/server/internal/ruleengine"
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

type ruleEngineService struct {
	pb.UnimplementedRuleEngineServiceServer

	logger      *slog.Logger
	registry    *observability.Registry
	cache       *cache.RiskCache
	producer    *messaging.Producer
	redisClient *redisx.Client
	catalog     *ruleengine.Catalog
	engine      *risk.Engine
	closeOnce   sync.Once
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loader := config.New("RMS")
	logger := logging.New("rule-engine-service", loader.String("ENVIRONMENT", "development"))
	registry := observability.NewRegistry()

	redisClient := redisx.New(
		loader.Endpoint("REDIS_ADDR", "127.0.0.1", 6379),
		redisx.WithTimeouts(250*time.Millisecond, 250*time.Millisecond, 250*time.Millisecond),
	)
	riskCache := cache.NewRiskCache(redisClient, "rms")
	brokers := splitCSV(loader.String("KAFKA_BROKERS", "127.0.0.1:9092"))
	producer := messaging.NewProducer(brokers, "")
	catalog := ruleengine.NewCatalog(ruleengine.DefaultRules(), "built-in-v1")

	engine := &risk.Engine{
		Cache:               riskCache,
		Rules:               catalog,
		Store:               nil,
		Events:              nil,
		Source:              "rule-engine-service",
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

	svc := &ruleEngineService{
		logger:      logger,
		registry:    registry,
		cache:       riskCache,
		producer:    producer,
		redisClient: redisClient,
		catalog:     catalog,
		engine:      engine,
	}

	go svc.refreshLoop(ctx, loader)

	httpAddr := loader.String("HTTP_ADDR", ":8084")
	go serveHTTP(ctx, svc, httpAddr)

	grpcAddr := loader.String("GRPC_ADDR", ":50064")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to open grpc listener", "error", err)
		os.Exit(1)
	}
	grpcServer := platformgrpc.NewServer(platformgrpc.ServerConfig{ServiceName: "rule-engine-service", Logger: logger, Metrics: registry})
	pb.RegisterRuleEngineServiceServer(grpcServer, svc)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("rule-engine-service", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("starting rule engine grpc server", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("rule engine grpc server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = shutdownCtx
	grpcServer.GracefulStop()
	svc.close()
	logger.Info("rule engine service stopped")
}

func (s *ruleEngineService) GetActiveRules(ctx context.Context, req *pb.GetActiveRulesRequest) (*pb.GetActiveRulesResponse, error) {
	if req == nil {
		return nil, errors.New("active rules request is required")
	}
	var rules []ruleengine.RuleConfig
	if strings.TrimSpace(req.TenantId) == "" && strings.TrimSpace(req.AccountId) == "" && strings.TrimSpace(req.Symbol) == "" {
		snapshot := s.catalog.Snapshot()
		rules = make([]ruleengine.RuleConfig, 0, len(snapshot.Rules))
		for _, rule := range snapshot.Rules {
			if rule.Enabled {
				rules = append(rules, rule)
			}
		}
	} else {
		rules = s.catalog.ActiveRules(req.TenantId, req.AccountId, req.Symbol)
	}
	results := make([]*pb.Rule, 0, len(rules))
	for _, rule := range rules {
		if req.RuleType != "" && !strings.EqualFold(req.RuleType, rule.RuleType) {
			continue
		}
		results = append(results, toProtoRule(rule))
	}
	return &pb.GetActiveRulesResponse{
		Rules:     results,
		Timestamp: time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *ruleEngineService) EvaluateRules(ctx context.Context, req *pb.RuleEvaluationRequest) (*pb.RuleEvaluationResponse, error) {
	if req == nil || req.Order == nil {
		return nil, errors.New("rule evaluation order is required")
	}
	decision, err := s.engine.Evaluate(ctx, &pb.OrderRequest{Order: req.Order, Account: req.Account}, risk.Options{
		TenantID:        firstNonEmpty(req.TenantId, tenantFromMetadata(req.Order.Metadata, metadataFromAccount(req.Account))),
		CorrelationID:   traceOrRequestID(ctx),
		TraceID:         observability.TraceFromContext(ctx).TraceID,
		IdempotencyKey:  firstNonEmpty(req.Order.Metadata["idempotency_key"], req.Order.OrderId, orderFingerprint(req.Order)),
		Source:          "rule-engine-service",
		RecordState:     false,
		CacheDecision:   false,
		PersistDecision: false,
		PublishEvents:   false,
	})
	if err != nil {
		return nil, err
	}
	return &pb.RuleEvaluationResponse{
		Approved:     decision.Approved,
		RejectReason: decision.RejectReason,
		Violations:   decision.Violations,
		Timestamp:    time.Now().UTC().UnixMilli(),
	}, nil
}

func (s *ruleEngineService) ReloadRules(ctx context.Context, req *pb.RuleReloadRequest) (*pb.RuleReloadResponse, error) {
	tenantID := ""
	source := "api"
	if req != nil {
		tenantID = strings.TrimSpace(req.TenantID)
		if req.Source != "" {
			source = req.Source
		}
	}
	snapshot, err := s.loadSnapshot(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	s.catalog.Replace(snapshot)
	_ = s.cache.StoreRuleVersion(ctx, tenantID, snapshot.Version, pb.DecisionTTL())
	if err := s.publishChange(ctx, tenantID, source, snapshot); err != nil {
		s.logger.Warn("failed to publish rule change", "error", err.Error())
	}
	return &pb.RuleReloadResponse{Reloaded: true, Timestamp: time.Now().UTC().UnixMilli()}, nil
}

func (s *ruleEngineService) StreamRuleUpdates(req *pb.RuleUpdateRequest, stream pb.RuleEngineService_StreamRuleUpdatesServer) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			rules, err := s.GetActiveRules(stream.Context(), &pb.GetActiveRulesRequest{RuleType: "", TenantId: "", AccountId: ""})
			if err != nil {
				return err
			}
			for _, rule := range rules.Rules {
				if req != nil && req.RuleId != "" && !strings.EqualFold(req.RuleId, rule.RuleId) {
					continue
				}
				if err := stream.Send(rule); err != nil {
					return err
				}
			}
		}
	}
}

func (s *ruleEngineService) refreshLoop(ctx context.Context, loader config.Loader) {
	refresh := func() {
		snapshot, err := s.loadSnapshot(ctx, "")
		if err != nil {
			s.logger.Warn("rule refresh failed", "error", err.Error())
			return
		}
		s.catalog.Replace(snapshot)
		_ = s.cache.StoreRuleVersion(context.Background(), "default", snapshot.Version, pb.DecisionTTL())
	}
	refresh()
	ticker := time.NewTicker(loader.Duration("RULE_REFRESH_INTERVAL", 10*time.Second))
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

func (s *ruleEngineService) loadSnapshot(ctx context.Context, tenantID string) (ruleengine.Snapshot, error) {
	key := "rms:rules:snapshot:default"
	if tenantID != "" {
		key = "rms:rules:snapshot:" + strings.ToLower(strings.TrimSpace(tenantID))
	}
	if s.redisClient != nil {
		if raw, ok, err := s.redisClient.Get(ctx, key); err == nil && ok && raw != "" {
			return ruleengine.LoadJSON("redis:"+key, []byte(raw))
		}
	}
	if raw := os.Getenv("RMS_RULES_JSON"); strings.TrimSpace(raw) != "" {
		return ruleengine.LoadJSON("env", []byte(raw))
	}
	return ruleengine.Snapshot{
		Version:  "built-in-v1",
		LoadedAt: time.Now().UTC(),
		Rules:    ruleengine.DefaultRules(),
	}, nil
}

func (s *ruleEngineService) publishChange(ctx context.Context, tenantID, source string, snapshot ruleengine.Snapshot) error {
	if s.producer == nil {
		return nil
	}
	envelope := messaging.EventEnvelope{
		EventID:       fmt.Sprintf("rules-%s", snapshot.Version),
		EventType:     "rules.changed",
		AggregateID:   tenantID,
		TenantID:      tenantID,
		Source:        "rule-engine-service",
		TraceID:       observability.TraceFromContext(ctx).TraceID,
		CorrelationID: observability.TraceFromContext(ctx).RequestID,
		SchemaVersion: "v1",
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"source": source,
		},
	}
	payload := map[string]any{
		"tenant_id":   tenantID,
		"source":      source,
		"version":     snapshot.Version,
		"rule_count":  len(snapshot.Rules),
		"loaded_at":   snapshot.LoadedAt.UTC().Format(time.RFC3339Nano),
	}
	return s.producer.PublishJSON(ctx, "rms.rules.changed.v1", messaging.PartitionKey(tenantID, snapshot.Version), envelope, payload)
}

func (s *ruleEngineService) close() {
	s.closeOnce.Do(func() {
		if s.producer != nil {
			_ = s.producer.Close()
		}
		if s.redisClient != nil {
			_ = s.redisClient.Close()
		}
	})
}

func serveHTTP(ctx context.Context, svc *ruleEngineService, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"service": "rule-engine-service",
			"status":  "ok",
		})
	})
	mux.HandleFunc("/metrics", platformhttp.MetricsHandler(svc.registry))
	mux.HandleFunc("/v1/rules", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		resp, err := svc.GetActiveRules(r.Context(), &pb.GetActiveRulesRequest{
			RuleType:  r.URL.Query().Get("rule_type"),
			TenantId:  r.URL.Query().Get("tenant_id"),
			AccountId: r.URL.Query().Get("account_id"),
			Symbol:    r.URL.Query().Get("symbol"),
		})
		if err != nil {
			platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		platformhttp.JSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/rules/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req pb.RuleReloadRequest
		if err := platformhttp.DecodeJSON(r, &req); err != nil {
			platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := svc.ReloadRules(r.Context(), &req)
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
		svc.logger.Info("starting rule engine http server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			svc.logger.Error("rule engine http server stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func toProtoRule(rule ruleengine.RuleConfig) *pb.Rule {
	return &pb.Rule{
		RuleId:              rule.RuleID,
		RuleName:            rule.RuleName,
		RuleDescription:     rule.RuleDescription,
		RuleType:            rule.RuleType,
		Severity:            rule.Severity,
		Enabled:             rule.Enabled,
		Priority:            int32(rule.Priority),
		TenantId:            rule.TenantID,
		AccountId:           rule.AccountID,
		Symbol:              rule.Symbol,
		ChainRuleIds:        append([]string(nil), rule.ChainRuleIDs...),
		ConditionExpression: rule.ConditionExpression,
		Parameters:          cloneStringMap(rule.Parameters),
		Thresholds:          cloneFloatMap(rule.Thresholds),
		StopOnFailure:       rule.StopOnFailure,
		CreatedAt:           rule.CreatedAt,
		UpdatedAt:           rule.UpdatedAt,
		CreatedBy:           rule.CreatedBy,
		UpdatedBy:           rule.UpdatedBy,
	}
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

func metadataFromAccount(account *pb.Account) map[string]string {
	if account == nil {
		return nil
	}
	return account.Metadata
}

func traceOrRequestID(ctx context.Context) string {
	trace := observability.TraceFromContext(ctx)
	if trace.RequestID != "" {
		return trace.RequestID
	}
	return trace.TraceID
}
