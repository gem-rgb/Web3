package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/example/rms/shared/platform/grpcx"
	pb "github.com/example/rms/shared/proto"
)

type ruleEngineService struct {
	pb.UnimplementedRuleEngineServiceServer
	mu    sync.RWMutex
	rules []*pb.Rule
}

func newRuleEngineService() *ruleEngineService {
	now := time.Now().UnixMilli()
	return &ruleEngineService{
		rules: []*pb.Rule{
			{
				RuleId:              "max-order-size",
				RuleName:            "Maximum Order Size",
				RuleDescription:     "Reject orders that exceed the configured quantity threshold.",
				RuleType:            "PRE_TRADE",
				Severity:            "HIGH",
				Enabled:             true,
				ConditionExpression: "abs(order.quantity) <= max_order_size",
				Parameters: map[string]string{"max_order_size": "10000"},
				CreatedAt:           now,
				UpdatedAt:           now,
				CreatedBy:           "system",
				UpdatedBy:           "system",
			},
			{
				RuleId:              "market-hours",
				RuleName:            "Market Hours Validation",
				RuleDescription:     "Orders must be submitted during regular market hours.",
				RuleType:            "PRE_TRADE",
				Severity:            "HIGH",
				Enabled:             true,
				ConditionExpression: "market_is_open(now)",
				Parameters:          map[string]string{"timezone": "America/New_York"},
				CreatedAt:           now,
				UpdatedAt:           now,
				CreatedBy:           "system",
				UpdatedBy:           "system",
			},
			{
				RuleId:              "excessive-frequency",
				RuleName:            "Excessive Frequency Detection",
				RuleDescription:     "Detect bursty order flow from the same account.",
				RuleType:            "MONITORING",
				Severity:            "MEDIUM",
				Enabled:             true,
				ConditionExpression: "orders_per_minute <= threshold",
				Parameters:          map[string]string{"threshold": "240"},
				CreatedAt:           now,
				UpdatedAt:           now,
				CreatedBy:           "system",
				UpdatedBy:           "system",
			},
		},
	}
}

func (s *ruleEngineService) GetActiveRules(ctx context.Context, req *pb.GetActiveRulesRequest) (*pb.GetActiveRulesResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("active rules request is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*pb.Rule, 0, len(s.rules))
	for _, rule := range s.rules {
		if !rule.Enabled {
			continue
		}
		if req.RuleType != "" && !strings.EqualFold(req.RuleType, rule.RuleType) {
			continue
		}
		results = append(results, cloneRule(rule))
	}
	sort.Slice(results, func(i, j int) bool { return results[i].RuleId < results[j].RuleId })
	return &pb.GetActiveRulesResponse{
		Rules:     results,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *ruleEngineService) EvaluateRules(ctx context.Context, req *pb.RuleEvaluationRequest) (*pb.RuleEvaluationResponse, error) {
	if req == nil || req.Order == nil {
		return nil, fmt.Errorf("rule evaluation order is required")
	}

	violations := s.evaluate(req)
	approved := len(violations) == 0
	rejectReason := ""
	if !approved {
		rejectReason = summarizeViolations(violations)
	}

	return &pb.RuleEvaluationResponse{
		Approved:     approved,
		RejectReason: rejectReason,
		Violations:   violations,
		Timestamp:    time.Now().UnixMilli(),
	}, nil
}

func (s *ruleEngineService) ReloadRules(ctx context.Context, req *pb.RuleReloadRequest) (*pb.RuleReloadResponse, error) {
	_ = ctx
	_ = req
	return &pb.RuleReloadResponse{
		Reloaded:  true,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *ruleEngineService) StreamRuleUpdates(req *pb.RuleUpdateRequest, stream pb.RuleEngineService_StreamRuleUpdatesServer) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			rules, err := s.rulesForSubscription(req)
			if err != nil {
				return err
			}
			for _, rule := range rules {
				if err := stream.Send(rule); err != nil {
					return err
				}
			}
		}
	}
}

func (s *ruleEngineService) rulesForSubscription(req *pb.RuleUpdateRequest) ([]*pb.Rule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := []*pb.Rule{}
	for _, rule := range s.rules {
		if req != nil && req.RuleId != "" && !strings.EqualFold(rule.RuleId, req.RuleId) {
			continue
		}
		results = append(results, cloneRule(rule))
	}
	return results, nil
}

func (s *ruleEngineService) evaluate(req *pb.RuleEvaluationRequest) []*pb.Violation {
	violations := []*pb.Violation{}
	order := req.Order
	account := req.Account
	if account == nil {
		account = syntheticAccount(order.AccountId)
	}

	if absInt32(order.Quantity) > 10000 {
		violations = append(violations, violation("max-order-size", "Order quantity exceeds limit", "HIGH"))
	}

	if !marketIsOpen(time.Now()) {
		violations = append(violations, violation("market-hours", "Order submitted outside regular market hours", "HIGH"))
	}

	notional := absFloat64(float64(order.Quantity) * order.Price)
	if account.BuyingPower > 0 && notional > account.BuyingPower {
		violations = append(violations, violation("buying-power", "Order notional exceeds buying power", "CRITICAL"))
	}

	equity := account.CashBalance + account.MarketValue
	if equity > 0 && (account.MarketValue+notional)/equity > 4.0 {
		violations = append(violations, violation("leverage", "Projected leverage exceeds limit", "HIGH"))
	}

	if account.MarketValue > 0 && (notional/account.MarketValue) > 0.25 {
		violations = append(violations, violation("concentration", "Order concentration exceeds limit", "HIGH"))
	}

	if duplicateDetected(order) {
		violations = append(violations, violation("duplicate-order", "Duplicate order detected", "HIGH"))
	}

	if washTradeDetected(order, req.Positions) {
		violations = append(violations, violation("wash-trade", "Potential wash trade detected", "CRITICAL"))
	}

	if excessiveFrequencyDetected(req.Account, req.Order) {
		violations = append(violations, violation("excessive-frequency", "Order frequency exceeds threshold", "MEDIUM"))
	}

	return violations
}

func duplicateDetected(order *pb.Order) bool {
	if order == nil {
		return false
	}
	if order.Metadata != nil {
		if strings.EqualFold(order.Metadata["duplicate_order"], "true") {
			return true
		}
	}
	return strings.Contains(strings.ToUpper(order.OrderId), "DUP")
}

func washTradeDetected(order *pb.Order, positions []*pb.Position) bool {
	if order == nil {
		return false
	}
	for _, position := range positions {
		if position == nil {
			continue
		}
		if strings.EqualFold(position.Symbol, order.Symbol) && position.AccountId == order.AccountId && position.Side != "" && !strings.EqualFold(position.Side, order.Side) {
			return true
		}
	}
	return false
}

func excessiveFrequencyDetected(account *pb.Account, order *pb.Order) bool {
	if account == nil || order == nil {
		return false
	}
	if account.Metadata != nil && strings.EqualFold(account.Metadata["frequency_profile"], "burst") {
		return true
	}
	if order.Metadata != nil && strings.EqualFold(order.Metadata["frequency_burst"], "true") {
		return true
	}
	return false
}

func violation(ruleID, description, severity string) *pb.Violation {
	return &pb.Violation{
		RuleId:          ruleID,
		RuleDescription: description,
		Severity:        severity,
	}
}

func cloneRule(rule *pb.Rule) *pb.Rule {
	if rule == nil {
		return nil
	}
	copy := *rule
	if rule.Parameters != nil {
		copy.Parameters = make(map[string]string, len(rule.Parameters))
		for k, v := range rule.Parameters {
			copy.Parameters[k] = v
		}
	}
	return &copy
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

func syntheticAccount(accountID string) *pb.Account {
	if accountID == "" {
		accountID = "synthetic-account"
	}
	base := 250_000.0 + float64(len(accountID))*12_000.0
	return &pb.Account{
		AccountId:               accountID,
		UserId:                  "rule-user",
		Status:                  "ACTIVE",
		BuyingPower:             base * 2,
		CashBalance:             base,
		MarketValue:             base * 0.8,
		DayTradingBuyingPower:   base * 3,
		MaintenanceMarginExcess: base * 0.2,
	}
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

func main() {
	lis, err := net.Listen("tcp", ":50058")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "rule-engine-service"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterRuleEngineServiceServer(s, newRuleEngineService())
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50058")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutting down gRPC server...")
	s.GracefulStop()
}
