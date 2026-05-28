package main

import (
	"encoding/json"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"github.com/segmentio/kafka-go"

	"github.com/example/rms/shared/platform/grpcx"
	"github.com/example/rms/shared/platform/observability"
	pb "github.com/example/rms/shared/proto"
)

type riskEvaluationService struct {
	pb.UnimplementedRiskEvaluationServiceServer
	mu                   sync.Mutex
	maxOrderSize         int32
	maxOrderNotional     float64
	maxLeverage          float64
	maxConcentrationPct  float64
	maxOrdersPerMinute   int
	duplicateWindow      time.Duration
	washTradeWindow      time.Duration
	marketHoursRequired   bool
	killSwitchEnabled    bool
	alertAddr            string
	auditAddr            string
	kafkaWriter          *kafka.Writer
	connMu               sync.Mutex
	alertConn            *grpc.ClientConn
	auditConn            *grpc.ClientConn
	recentOrders         map[string][]*pb.Order
	recentFingerprints   map[string]time.Time
}

func newRiskEvaluationService() *riskEvaluationService {
	return &riskEvaluationService{
		maxOrderSize:        envInt32("RMS_MAX_ORDER_SIZE", 10000),
		maxOrderNotional:    envFloat("RMS_MAX_ORDER_NOTIONAL", 2_500_000),
		maxLeverage:         envFloat("RMS_MAX_LEVERAGE", 4.0),
		maxConcentrationPct: envFloat("RMS_MAX_CONCENTRATION_PCT", 0.25),
		maxOrdersPerMinute:  envInt("RMS_MAX_ORDERS_PER_MINUTE", 240),
		duplicateWindow:     envDuration("RMS_DUPLICATE_WINDOW", 30*time.Second),
		washTradeWindow:     envDuration("RMS_WASH_TRADE_WINDOW", 5*time.Minute),
		marketHoursRequired:  envBool("RMS_MARKET_HOURS_REQUIRED", true),
		killSwitchEnabled:   envBool("RMS_KILL_SWITCH", false),
		alertAddr:           envString("RMS_ALERTING_ADDR", "127.0.0.1:50052"),
		auditAddr:           envString("RMS_AUDIT_ADDR", "127.0.0.1:50053"),
		recentOrders:        make(map[string][]*pb.Order),
		recentFingerprints:  make(map[string]time.Time),
	}
	if brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS")); brokers != "" {
		parts := strings.Split(brokers, ",")
		addrs := make([]string, 0, len(parts))
		for _, part := range parts {
			if value := strings.TrimSpace(part); value != "" {
				addrs = append(addrs, value)
			}
		}
		if len(addrs) > 0 {
			svc.kafkaWriter = &kafka.Writer{
				Addr:         kafka.TCP(addrs...),
				Balancer:     &kafka.Hash{},
				RequiredAcks: kafka.RequireOne,
				Async:        true,
			}
		}
	}
	return svc
}

func (s *riskEvaluationService) EvaluateOrder(ctx context.Context, req *pb.OrderRequest) (*pb.OrderResponse, error) {
	if req == nil || req.Order == nil {
		return nil, fmt.Errorf("order request is required")
	}

	start := time.Now()
	order := req.Order
	account := req.Account
	if account == nil {
		account = syntheticAccount(order.AccountId)
	}
	if order.Timestamp == 0 {
		order.Timestamp = time.Now().UnixMilli()
	}

	violations := s.evaluate(order, account)
	approved := len(violations) == 0
	rejectReason := ""
	if !approved {
		rejectReason = summarizeViolations(violations)
	}

	s.recordOrder(order)

	log.Printf(
		"risk decision order_id=%s account_id=%s symbol=%s approved=%t latency_ms=%d violations=%d",
		order.OrderId,
		order.AccountId,
		order.Symbol,
		approved,
		time.Since(start).Milliseconds(),
		len(violations),
	)

	s.emitAuditEvent(ctx, order, account, violations, approved, rejectReason)
	if !approved {
		s.emitAlert(ctx, order, violations, rejectReason)
	}
	s.publishKafkaEvent(ctx, order, account, approved, violations, rejectReason)

	return &pb.OrderResponse{
		OrderId:      order.OrderId,
		Approved:     approved,
		RejectReason: rejectReason,
		Violations:   violations,
		Timestamp:    time.Now().UnixMilli(),
	}, nil
}

func (s *riskEvaluationService) BatchEvaluateOrders(ctx context.Context, req *pb.BatchOrderRequest) (*pb.BatchOrderResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("batch request is required")
	}

	log.Printf("received batch of %d orders", len(req.Orders))
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
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *riskEvaluationService) GetDecision(ctx context.Context, req *pb.OrderDecisionQueryRequest) (*pb.OrderDecisionQueryResponse, error) {
	_ = ctx
	_ = req
	return &pb.OrderDecisionQueryResponse{
		Found:     false,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *riskEvaluationService) StreamOrderEvaluations(stream pb.RiskEvaluationService_StreamOrderEvaluationsServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
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

func (s *riskEvaluationService) emitAuditEvent(ctx context.Context, order *pb.Order, account *pb.Account, violations []*pb.Violation, approved bool, rejectReason string) {
	payload, _ := json.Marshal(map[string]any{
		"order":         order,
		"account":       account,
		"approved":      approved,
		"reject_reason": rejectReason,
		"violations":    violations,
	})

	event := &pb.AuditEvent{
		EventType:       "ORDER_EVALUATED",
		AccountId:       order.AccountId,
		ServiceName:     "risk-evaluation-service",
		PayloadJSON:     string(payload),
		Timestamp:       time.Now().UnixMilli(),
		IngestTimestamp: time.Now().UnixMilli(),
		Metadata: map[string]string{
			"symbol":   order.Symbol,
			"order_id": order.OrderId,
		},
	}

	conn, err := s.auditClient(ctx)
	if err != nil {
		log.Printf("audit service unavailable: %v", err)
		return
	}
	resp := &pb.AuditResponse{}
	if err := conn.Invoke(ctx, "/rms.audit.AuditLoggingService/LogEvent", event, resp); err != nil {
		log.Printf("audit log failed: %v", err)
	}
}

func (s *riskEvaluationService) emitAlert(ctx context.Context, order *pb.Order, violations []*pb.Violation, rejectReason string) {
	alert := &pb.AlertRequest{
		Alert: &pb.AlertRecord{
			AccountId: order.AccountId,
			RuleId:    firstViolationRuleID(violations),
			Severity:   highestSeverity(violations),
			Message:   fmt.Sprintf("Order %s rejected: %s", order.OrderId, rejectReason),
			Timestamp: time.Now().UnixMilli(),
			Metadata: map[string]string{
				"order_id": order.OrderId,
				"symbol":   order.Symbol,
			},
		},
	}

	conn, err := s.alertClient(ctx)
	if err != nil {
		log.Printf("alert service unavailable: %v", err)
		return
	}
	resp := &pb.AlertResponse{}
	if err := conn.Invoke(ctx, "/rms.alerting.AlertingService/SendAlert", alert, resp); err != nil {
		log.Printf("alert send failed: %v", err)
	}
}

func (s *riskEvaluationService) alertClient(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.alertConn != nil {
		return s.alertConn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		s.alertAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	s.alertConn = conn
	return conn, nil
}

func (s *riskEvaluationService) auditClient(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.auditConn != nil {
		return s.auditConn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		s.auditAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	s.auditConn = conn
	return conn, nil
}

func (s *riskEvaluationService) close() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.alertConn != nil {
		_ = s.alertConn.Close()
		s.alertConn = nil
	}
	if s.auditConn != nil {
		_ = s.auditConn.Close()
		s.auditConn = nil
	}
	if s.kafkaWriter != nil {
		_ = s.kafkaWriter.Close()
		s.kafkaWriter = nil
	}
}

func (s *riskEvaluationService) evaluate(order *pb.Order, account *pb.Account) []*pb.Violation {
	s.mu.Lock()
	defer s.mu.Unlock()

	var violations []*pb.Violation

	if s.killSwitchEnabled {
		violations = append(violations, &pb.Violation{
			RuleId:          "kill-switch",
			RuleDescription: "Global kill switch is enabled",
			Severity:        "CRITICAL",
		})
		return violations
	}

	if s.marketHoursRequired && !marketIsOpen(time.Now()) {
		violations = append(violations, &pb.Violation{
			RuleId:          "market-hours",
			RuleDescription: "Order submitted outside regular market hours",
			Severity:        "HIGH",
		})
	}

	notional := absFloat64(float64(order.Quantity) * order.Price)
	if absInt32(order.Quantity) > s.maxOrderSize {
		violations = append(violations, &pb.Violation{
			RuleId:          "max-order-size",
			RuleDescription: fmt.Sprintf("Order quantity %d exceeds max size %d", absInt32(order.Quantity), s.maxOrderSize),
			Severity:        "HIGH",
		})
	}
	if notional > s.maxOrderNotional {
		violations = append(violations, &pb.Violation{
			RuleId:          "max-notional",
			RuleDescription: fmt.Sprintf("Order notional %.2f exceeds limit %.2f", notional, s.maxOrderNotional),
			Severity:        "HIGH",
		})
	}

	buyingPower := account.BuyingPower
	if buyingPower <= 0 {
		buyingPower = account.CashBalance
	}
	if buyingPower > 0 && notional > buyingPower {
		violations = append(violations, &pb.Violation{
			RuleId:          "buying-power",
			RuleDescription: fmt.Sprintf("Order notional %.2f exceeds buying power %.2f", notional, buyingPower),
			Severity:        "CRITICAL",
		})
	}

	equity := account.CashBalance + account.MarketValue
	if equity <= 0 {
		equity = buyingPower
	}
	if equity > 0 {
		leverage := (account.MarketValue + notional) / equity
		if leverage > s.maxLeverage {
			violations = append(violations, &pb.Violation{
				RuleId:          "leverage",
				RuleDescription: fmt.Sprintf("Projected leverage %.2fx exceeds limit %.2fx", leverage, s.maxLeverage),
				Severity:        "HIGH",
			})
		}
	}

	if account.MarketValue > 0 && (notional/account.MarketValue) > s.maxConcentrationPct {
		violations = append(violations, &pb.Violation{
			RuleId:          "concentration",
			RuleDescription: fmt.Sprintf("Order concentration %.2f%% exceeds limit %.2f%%", 100*(notional/account.MarketValue), 100*s.maxConcentrationPct),
			Severity:        "HIGH",
		})
	}

	if loss := parseFloat(account.Metadata, "daily_loss"); loss > 0 {
		dailyLossLimit := account.CashBalance * 0.05
		if loss > dailyLossLimit {
			violations = append(violations, &pb.Violation{
				RuleId:          "daily-loss",
				RuleDescription: fmt.Sprintf("Daily loss %.2f exceeds limit %.2f", loss, dailyLossLimit),
				Severity:        "CRITICAL",
			})
		}
	}

	fingerprint := orderFingerprint(order)
	if ts, ok := s.recentFingerprints[fingerprint]; ok && time.Since(ts) <= s.duplicateWindow {
		violations = append(violations, &pb.Violation{
			RuleId:          "duplicate-order",
			RuleDescription: "Duplicate order detected in the replay window",
			Severity:        "HIGH",
		})
	}

	if s.washTradeDetectedLocked(order) {
		violations = append(violations, &pb.Violation{
			RuleId:          "wash-trade",
			RuleDescription: "Potential wash trade detected for the same symbol and account",
			Severity:        "CRITICAL",
		})
	}

	if s.isHighFrequencyLocked(account.AccountId) {
		violations = append(violations, &pb.Violation{
			RuleId:          "excessive-frequency",
			RuleDescription: "Order rate exceeds configured threshold",
			Severity:        "HIGH",
		})
	}

	if len(violations) > 0 {
		// Suspicious activity gets an extra marker for downstream alerting.
		violations = append(violations, &pb.Violation{
			RuleId:          "suspicious-activity",
			RuleDescription: "Risk engine flagged this order as suspicious",
			Severity:        "HIGH",
		})
	}

	return violations
}

func (s *riskEvaluationService) recordOrder(order *pb.Order) {
	s.mu.Lock()
	defer s.mu.Unlock()

	accountID := order.AccountId
	s.recentOrders[accountID] = append(trimOrders(s.recentOrders[accountID], 20), cloneOrder(order))
	s.recentFingerprints[orderFingerprint(order)] = time.Now()
}

func (s *riskEvaluationService) washTradeDetectedLocked(order *pb.Order) bool {
	recent := s.recentOrders[order.AccountId]
	for _, prior := range recent {
		if strings.EqualFold(prior.Symbol, order.Symbol) && !strings.EqualFold(prior.Side, order.Side) {
			if absFloat64(prior.Price-order.Price) <= order.Price*0.01 {
				return true
			}
		}
	}
	return false
}

func (s *riskEvaluationService) isHighFrequencyLocked(accountID string) bool {
	now := time.Now()
	windowStart := now.Add(-1 * time.Minute)
	recent := s.recentOrders[accountID]
	count := 0
	for _, order := range recent {
		if time.UnixMilli(order.Timestamp).After(windowStart) {
			count++
		}
	}
	return count >= s.maxOrdersPerMinute
}

func trimOrders(orders []*pb.Order, max int) []*pb.Order {
	if len(orders) <= max {
		return orders
	}
	return append([]*pb.Order(nil), orders[len(orders)-max:]...)
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

func marketIsOpen(now time.Time) bool {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		loc = time.FixedZone("ET", -5*60*60)
	}
	local := now.In(loc)
	if local.Weekday() == time.Saturday || local.Weekday() == time.Sunday {
		return false
	}
	open := time.Date(local.Year(), local.Month(), local.Day(), 9, 30, 0, 0, loc)
	close := time.Date(local.Year(), local.Month(), local.Day(), 16, 0, 0, 0, loc)
	return !local.Before(open) && !local.After(close)
}

func summarizeViolations(violations []*pb.Violation) string {
	if len(violations) == 0 {
		return ""
	}
	parts := make([]string, 0, len(violations))
	for _, v := range violations {
		if v == nil || v.RuleId == "" {
			continue
		}
		parts = append(parts, v.RuleId)
	}
	return strings.Join(parts, ", ")
}

func firstViolationRuleID(violations []*pb.Violation) string {
	if len(violations) == 0 || violations[0] == nil {
		return ""
	}
	return violations[0].RuleId
}

func highestSeverity(violations []*pb.Violation) string {
	severityRank := map[string]int{
		"LOW": 1,
		"MEDIUM": 2,
		"HIGH": 3,
		"CRITICAL": 4,
	}
	best := "LOW"
	bestRank := 0
	for _, violation := range violations {
		if violation == nil {
			continue
		}
		rank := severityRank[strings.ToUpper(violation.Severity)]
		if rank > bestRank {
			bestRank = rank
			best = strings.ToUpper(violation.Severity)
		}
	}
	return best
}

func (s *riskEvaluationService) publishKafkaEvent(ctx context.Context, order *pb.Order, account *pb.Account, approved bool, violations []*pb.Violation, rejectReason string) {
	if s.kafkaWriter == nil {
		return
	}

	topic := "rms.orders.evaluated.v1"
	if !approved {
		topic = "rms.orders.rejected.v1"
	}

	trace := observability.TraceFromContext(ctx)
	eventType := "order.evaluated"
	if !approved {
		eventType = "order.rejected"
	}
	tenantID := tenantIDFromMetadata(order.Metadata, account.Metadata)
	eventID := fmt.Sprintf("%s-%s", order.OrderId, strings.ReplaceAll(eventType, ".", "-"))
	payload, _ := json.Marshal(map[string]any{
		"event_id":        eventID,
		"event_type":      eventType,
		"aggregate_id":    order.OrderId,
		"tenant_id":       tenantID,
		"correlation_id":  trace.RequestID,
		"trace_id":        trace.TraceID,
		"schema_version":  "v1",
		"occurred_at":     time.Now().UTC().Format(time.RFC3339Nano),
		"headers": map[string]string{
			"source":  "risk-evaluation-service",
			"service": "risk-evaluation-service",
		},
		"data": map[string]any{
			"order":         order,
			"account":       account,
			"approved":      approved,
			"reject_reason": rejectReason,
			"violations":    violations,
			"timestamp":     time.Now().UnixMilli(),
		},
	})

	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(strings.Join([]string{tenantID, order.AccountId, order.OrderId}, "|")),
		Value: payload,
		Time:  time.Now(),
	}); err != nil {
		log.Printf("kafka publish failed: %v", err)
	}
}

func orderFingerprint(order *pb.Order) string {
	return fmt.Sprintf("%s|%s|%d|%.4f|%s|%s|%s", order.AccountId, order.Symbol, order.Quantity, order.Price, strings.ToUpper(order.Side), strings.ToUpper(order.OrderType), strings.ToUpper(order.TimeInForce))
}

func tenantIDFromMetadata(maps ...map[string]string) string {
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
	return "default"
}

func syntheticAccount(accountID string) *pb.Account {
	if accountID == "" {
		accountID = "synthetic-account"
	}
	base := 250_000.0 + float64(len(accountID))*10_000.0
	return &pb.Account{
		AccountId:               accountID,
		UserId:                  "synthetic-user",
		Status:                  "ACTIVE",
		BuyingPower:             base * 2,
		CashBalance:             base,
		MarketValue:             base * 0.8,
		DayTradingBuyingPower:   base * 3,
		MaintenanceMarginExcess: base * 0.2,
		Metadata: map[string]string{
			"desk":         "institutional",
			"risk_tier":    "tier-1",
			"daily_loss":   "0",
			"source":       "synthetic",
		},
	}
}

func absFloat64(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func absInt32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}

func parseFloat(values map[string]string, key string) float64 {
	if values == nil {
		return 0
	}
	raw, ok := values[key]
	if !ok || raw == "" {
		return 0
	}
	var parsed float64
	fmt.Sscanf(raw, "%f", &parsed)
	return parsed
}

func envInt(name string, fallback int) int {
	if raw := os.Getenv(name); raw != "" {
		var parsed int
		if _, err := fmt.Sscanf(raw, "%d", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt32(name string, fallback int32) int32 {
	return int32(envInt(name, int(fallback)))
}

func envFloat(name string, fallback float64) float64 {
	if raw := os.Getenv(name); raw != "" {
		var parsed float64
		if _, err := fmt.Sscanf(raw, "%f", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if raw := os.Getenv(name); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil {
			return parsed
		}
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	if raw := os.Getenv(name); raw != "" {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return fallback
}

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	svc := newRiskEvaluationService()
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "risk-evaluation-service"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterRiskEvaluationServiceServer(s, svc)
	// Register health service
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	// Register reflection service on gRPC server.
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50051")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutting down gRPC server...")
	s.GracefulStop()
	svc.close()
}
