package risk

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/rms/server/internal/cache"
	"github.com/example/rms/server/internal/ruleengine"
	"github.com/example/rms/server/internal/store"
	"github.com/example/rms/shared/platform/messaging"
	"github.com/example/rms/shared/platform/observability"
	pb "github.com/example/rms/shared/proto"
)

// Engine orchestrates low-latency risk decisions.
type Engine struct {
	Cache   *cache.RiskCache
	Rules   *ruleengine.Catalog
	Store   *store.DecisionStore
	Events  *messaging.Producer
	Source  string
	Version string

	MaxOrderSize        int32
	MaxLeverage         float64
	MaxOrdersPerMinute  int
	FatFingerPct        float64
	DuplicateWindow     time.Duration
	FrequencyWindow     time.Duration
	DecisionTTL         time.Duration
	MarketHoursRequired bool
}

// Options adjust how evaluation is executed in a specific service path.
type Options struct {
	TenantID       string
	CorrelationID  string
	TraceID        string
	IdempotencyKey string
	Source         string
	RecordState    bool
	CacheDecision  bool
	PersistDecision bool
	PublishEvents  bool
}

// Evaluate returns a deterministic decision for the supplied order.
func (e *Engine) Evaluate(ctx context.Context, req *pb.OrderRequest, opts Options) (*pb.RiskDecision, error) {
	if req == nil || req.Order == nil {
		return nil, fmt.Errorf("order request is required")
	}
	start := time.Now().UTC()
	order := cloneOrder(req.Order)
	account := cloneAccount(req.Account)
	if account == nil {
		account = e.syntheticAccount(order.AccountId)
	}
	tenantID := firstNonEmpty(opts.TenantID, tenantFromMetadata(order.Metadata, account.Metadata), "default")
	correlationID := firstNonEmpty(opts.CorrelationID, traceOrRequestID(ctx), order.Metadata["correlation_id"], account.Metadata["correlation_id"])
	traceID := firstNonEmpty(opts.TraceID, observability.TraceFromContext(ctx).TraceID)
	idempotencyKey := firstNonEmpty(opts.IdempotencyKey, order.Metadata["idempotency_key"], order.OrderId, orderFingerprint(order))
	source := firstNonEmpty(opts.Source, e.Source, "risk-evaluation-service")

	decision := &pb.RiskDecision{
		DecisionID:     stableDecisionID(order.OrderId, idempotencyKey),
		OrderID:        order.OrderId,
		AccountID:      account.AccountId,
		TenantID:       tenantID,
		CorrelationID:  correlationID,
		TraceID:        traceID,
		IdempotencyKey: idempotencyKey,
		Engine:         firstNonEmpty(e.Source, "rms-risk-engine"),
		DecisionSource: source,
		Metadata: map[string]string{
			"order_side": order.Side,
			"order_type": order.OrderType,
		},
		Version: "v1",
	}

	duplicateDetected := false
	if e.Cache != nil {
		if opts.RecordState {
			reserved, err := e.Cache.ReserveIdempotency(ctx, tenantID, idempotencyKey, pb.DecisionTTL())
			if err != nil {
				decision.Metadata["idempotency_error"] = err.Error()
			}
			if !reserved {
				duplicateDetected = true
			}
			if fingerprintStored, err := e.Cache.StoreFingerprint(ctx, tenantID, account.AccountId, orderFingerprint(order), e.DuplicateWindow); err == nil && !fingerprintStored {
				duplicateDetected = true
			}
			if count, err := e.Cache.IncrementFrequency(ctx, tenantID, account.AccountId, e.FrequencyWindow); err == nil {
				decision.Metadata["frequency_count"] = fmt.Sprintf("%d", count)
			}
			_ = e.Cache.StoreAccount(ctx, tenantID, account, 12*time.Hour)
		} else {
			if seen, err := e.Cache.HasFingerprint(ctx, tenantID, account.AccountId, orderFingerprint(order)); err == nil && seen {
				duplicateDetected = true
			}
			if count, ok, err := e.Cache.CurrentFrequency(ctx, tenantID, account.AccountId, e.FrequencyWindow); err == nil && ok {
				decision.Metadata["frequency_count"] = fmt.Sprintf("%d", count)
			}
		}
	}

	if opts.RecordState && !duplicateDetected && e.Cache != nil {
		// Capture the latest account snapshot for fast replay paths.
		_ = e.Cache.StoreAccount(ctx, tenantID, account, 12*time.Hour)
	}

	rules := e.activeRules(tenantID, account.AccountId, order.Symbol)
	violations := e.evaluateRules(ctx, order, account, tenantID, rules, duplicateDetected, decision.Metadata)
	if duplicateDetected {
		violations = append(violations, violation("duplicate-order", "Duplicate order detected in the replay window", "HIGH"))
	}

	approved := len(violations) == 0
	rejectReason := summarizeViolations(violations)
	decision.Approved = approved
	decision.RejectReason = rejectReason
	decision.Violations = violations
	decision.EvaluatedAt = time.Now().UTC().UnixMilli()
	decision.LatencyMicros = time.Since(start).Microseconds()
	decision.RuleVersion = e.ruleVersion(tenantID)
	if decision.RuleVersion == "" {
		decision.RuleVersion = "built-in-v1"
	}

	if e.Cache != nil && opts.CacheDecision {
		if err := e.Cache.StoreDecision(ctx, decision, e.decisionTTL()); err != nil {
			decision.Metadata["cache_error"] = err.Error()
		}
	}
	if e.Store != nil && opts.PersistDecision {
		if err := e.Store.Save(ctx, decision); err != nil {
			return nil, err
		}
	}
	if e.Events != nil && opts.PublishEvents {
		if err := e.publishDecision(ctx, decision, order, account); err != nil {
			decision.Metadata["publish_error"] = err.Error()
		}
	}
	return decision, nil
}

func (e *Engine) activeRules(tenantID, accountID, symbol string) []ruleengine.RuleConfig {
	if e == nil || e.Rules == nil {
		return ruleengine.DefaultRules()
	}
	rules := e.Rules.ActiveRules(tenantID, accountID, symbol)
	if len(rules) == 0 {
		return ruleengine.DefaultRules()
	}
	return rules
}

func (e *Engine) evaluateRules(ctx context.Context, order *pb.Order, account *pb.Account, tenantID string, rules []ruleengine.RuleConfig, duplicateDetected bool, metadata map[string]string) []*pb.Violation {
	byID := make(map[string]ruleengine.RuleConfig, len(rules))
	for _, rule := range rules {
		byID[rule.RuleID] = rule
	}
	visited := map[string]bool{}
	violations := make([]*pb.Violation, 0, 4)
	var walk func(string)
	walk = func(ruleID string) {
		if ruleID == "" || visited[ruleID] {
			return
		}
		rule, ok := byID[ruleID]
		if !ok {
			return
		}
		visited[ruleID] = true
		if violation := e.evaluateRule(ctx, rule, order, account, tenantID, duplicateDetected, metadata); violation != nil {
			violations = append(violations, violation)
			if rule.StopOnFailure {
				return
			}
		}
		for _, next := range rule.ChainRuleIDs {
			walk(next)
		}
	}

	for _, rule := range rules {
		walk(rule.RuleID)
	}
	return violations
}

func (e *Engine) evaluateRule(ctx context.Context, rule ruleengine.RuleConfig, order *pb.Order, account *pb.Account, tenantID string, duplicateDetected bool, metadata map[string]string) *pb.Violation {
	switch strings.ToUpper(strings.TrimSpace(rule.RuleType)) {
	case "MAX_ORDER_SIZE":
		maxQty := ruleFloat(rule.Thresholds, "max_quantity", float64(e.maxOrderSize()))
		if absInt32(order.Quantity) > int32(maxQty) {
			return violation(rule.RuleID, fmt.Sprintf("Order quantity %d exceeds max size %.0f", absInt32(order.Quantity), maxQty), ruleSeverity(rule.Severity, "HIGH"))
		}
	case "BUYING_POWER":
		notional := orderNotional(order)
		buyingPower := effectiveBuyingPower(account)
		if buyingPower > 0 && notional > buyingPower {
			return violation(rule.RuleID, fmt.Sprintf("Order notional %.2f exceeds buying power %.2f", notional, buyingPower), ruleSeverity(rule.Severity, "CRITICAL"))
		}
	case "LEVERAGE":
		notional := orderNotional(order)
		maxLeverage := ruleFloat(rule.Thresholds, "max_leverage", e.maxLeverageLimit())
		equity := account.CashBalance + account.MarketValue
		if equity <= 0 {
			equity = effectiveBuyingPower(account)
		}
		if equity > 0 {
			projected := (account.MarketValue + notional) / equity
			if projected > maxLeverage {
				return violation(rule.RuleID, fmt.Sprintf("Projected leverage %.2fx exceeds limit %.2fx", projected, maxLeverage), ruleSeverity(rule.Severity, "HIGH"))
			}
		}
	case "MARKET_HOURS":
		if e.marketHoursRequired() && !marketIsOpen(time.Now()) {
			return violation(rule.RuleID, "Order submitted outside regular market hours", ruleSeverity(rule.Severity, "HIGH"))
		}
	case "DUPLICATE_ORDER":
		if duplicateDetected {
			return violation(rule.RuleID, "Duplicate order detected in the replay window", ruleSeverity(rule.Severity, "HIGH"))
		}
	case "EXCESSIVE_FREQUENCY":
		maxPerMinute := ruleFloat(rule.Thresholds, "max_orders_per_minute", float64(e.maxOrdersPerMinute()))
		if freq := parseFloat(metadata, "frequency_count"); freq > 0 && freq > maxPerMinute {
			return violation(rule.RuleID, fmt.Sprintf("Order rate %.0f exceeds threshold %.0f", freq, maxPerMinute), ruleSeverity(rule.Severity, "HIGH"))
		}
	case "RESTRICTED_SYMBOL":
		if e.restrictedSymbol(rule, order.Symbol, account, tenantID) {
			return violation(rule.RuleID, fmt.Sprintf("Symbol %s is restricted", order.Symbol), ruleSeverity(rule.Severity, "CRITICAL"))
		}
	case "FAT_FINGER":
		if v := e.fatFingerViolation(rule, order, tenantID); v != nil {
			return v
		}
	case "ACCOUNT_STATUS":
		if !allowedAccountStatus(rule, account.Status) {
			return violation(rule.RuleID, fmt.Sprintf("Account status %s is not allowed", account.Status), ruleSeverity(rule.Severity, "CRITICAL"))
		}
	}
	return nil
}

func (e *Engine) fatFingerViolation(rule ruleengine.RuleConfig, order *pb.Order, tenantID string) *pb.Violation {
	if order == nil {
		return nil
	}
	if order.Price <= 0 {
		return violation(rule.RuleID, "Order price must be positive", ruleSeverity(rule.Severity, "HIGH"))
	}
	threshold := ruleFloat(rule.Thresholds, "max_deviation_pct", e.fatFingerLimit())
	if threshold <= 0 {
		threshold = e.fatFingerLimit()
	}
	lastPrice := order.Price
	if e.Cache != nil {
		if cached, ok, err := e.Cache.LoadMarketPrice(context.Background(), tenantID, order.Symbol); err == nil && ok && cached > 0 {
			lastPrice = cached
		}
	}
	if lastPrice <= 0 {
		return nil
	}
	deviation := math.Abs(order.Price-lastPrice) / lastPrice
	if deviation > threshold {
		return violation(rule.RuleID, fmt.Sprintf("Order price deviation %.2f%% exceeds limit %.2f%%", deviation*100, threshold*100), ruleSeverity(rule.Severity, "HIGH"))
	}
	return nil
}

func (e *Engine) restrictedSymbol(rule ruleengine.RuleConfig, symbol string, account *pb.Account, tenantID string) bool {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return false
	}
	if rule.Symbol != "" && strings.EqualFold(rule.Symbol, symbol) {
		return true
	}
	restricted := splitCSV(rule.Parameters["symbols"])
	for _, candidate := range restricted {
		if strings.EqualFold(candidate, symbol) {
			return true
		}
	}
	if account != nil && account.Metadata != nil {
		if rules := splitCSV(account.Metadata["restricted_symbols"]); len(rules) > 0 {
			for _, candidate := range rules {
				if strings.EqualFold(candidate, symbol) {
					return true
				}
			}
		}
	}
	if e.Cache != nil {
		if cached, ok, err := e.Cache.LoadRuleVersion(context.Background(), tenantID); err == nil && ok && cached != "" {
			_ = cached
		}
	}
	return false
}

func (e *Engine) publishDecision(ctx context.Context, decision *pb.RiskDecision, order *pb.Order, account *pb.Account) error {
	trace := observability.TraceFromContext(ctx)
	topic := "rms.orders.approved.v1"
	eventType := "order.approved"
	payload := OrderApprovedEvent{
		EventID:       decision.DecisionID,
		OrderID:       decision.OrderID,
		AccountID:     decision.AccountID,
		TenantID:      decision.TenantID,
		CorrelationID: firstNonEmpty(decision.CorrelationID, trace.RequestID),
		TraceID:       firstNonEmpty(decision.TraceID, trace.TraceID),
		ApprovedAt:    decision.EvaluatedAt,
		Decision:      decision,
		Metadata: map[string]string{
			"source": e.Source,
			"engine": decision.Engine,
		},
	}
	if !decision.Approved {
		topic = "rms.orders.rejected.v1"
		eventType = "order.rejected"
		payloadRejected := OrderRejectedEvent{
			EventID:       decision.DecisionID,
			OrderID:       decision.OrderID,
			AccountID:     decision.AccountID,
			TenantID:      decision.TenantID,
			CorrelationID: firstNonEmpty(decision.CorrelationID, trace.RequestID),
			TraceID:       firstNonEmpty(decision.TraceID, trace.TraceID),
			RejectedAt:    decision.EvaluatedAt,
			Decision:      decision,
			Metadata: map[string]string{
				"source": e.Source,
				"engine": decision.Engine,
			},
		}
		return e.publishEvent(ctx, topic, eventType, decision, order, account, payloadRejected)
	}
	return e.publishEvent(ctx, topic, eventType, decision, order, account, payload)
}

func (e *Engine) publishEvent(ctx context.Context, topic, eventType string, decision *pb.RiskDecision, order *pb.Order, account *pb.Account, payload any) error {
	if e == nil || e.Events == nil || decision == nil || order == nil {
		return nil
	}
	trace := observability.TraceFromContext(ctx)
	envelope := messaging.EventEnvelope{
		EventID:       decision.DecisionID,
		EventType:     eventType,
		AggregateID:   order.OrderId,
		TenantID:      decision.TenantID,
		Source:        e.Source,
		TraceID:       firstNonEmpty(decision.TraceID, trace.TraceID),
		CorrelationID: firstNonEmpty(decision.CorrelationID, trace.RequestID),
		SchemaVersion: decision.Version,
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"account_id": order.AccountId,
			"symbol":     order.Symbol,
		},
	}
	key := messaging.PartitionKey(decision.TenantID, order.AccountId, order.OrderId)
	if err := e.Events.PublishJSON(ctx, topic, key, envelope, payload); err != nil {
		return err
	}
	if decision.Approved {
		completed := RiskEvaluationCompletedEvent{
			EventID:       decision.DecisionID,
			OrderID:       order.OrderId,
			AccountID:     order.AccountId,
			TenantID:      decision.TenantID,
			CorrelationID: envelope.CorrelationID,
			TraceID:       envelope.TraceID,
			CompletedAt:   decision.EvaluatedAt,
			Decision:      decision,
			Metadata: map[string]string{
				"source": e.Source,
			},
		}
		_ = e.publishEnvelope(ctx, "rms.risk.evaluations.completed.v1", "risk.evaluation.completed", decision, order, completed)
	} else {
		rejected := RiskEvaluationCompletedEvent{
			EventID:       decision.DecisionID,
			OrderID:       order.OrderId,
			AccountID:     order.AccountId,
			TenantID:      decision.TenantID,
			CorrelationID: envelope.CorrelationID,
			TraceID:       envelope.TraceID,
			CompletedAt:   decision.EvaluatedAt,
			Decision:      decision,
			Metadata: map[string]string{
				"source": e.Source,
			},
		}
		_ = e.publishEnvelope(ctx, "rms.risk.evaluations.completed.v1", "risk.evaluation.completed", decision, order, rejected)
		if criticalViolation(decision.Violations) {
			alert := RiskAlertTriggeredEvent{
				EventID:       decision.DecisionID,
				AlertID:       fmt.Sprintf("alert-%s", order.OrderId),
				OrderID:       order.OrderId,
				AccountID:     order.AccountId,
				TenantID:      decision.TenantID,
				CorrelationID: envelope.CorrelationID,
				TraceID:       envelope.TraceID,
				RuleID:        firstViolationRuleID(decision.Violations),
				Severity:      highestSeverity(decision.Violations),
				Message:       fmt.Sprintf("Order %s rejected: %s", order.OrderId, decision.RejectReason),
				TriggeredAt:   decision.EvaluatedAt,
				Metadata: map[string]string{
					"symbol": order.Symbol,
				},
			}
			_ = e.publishEnvelope(ctx, "rms.risk.alerts.triggered.v1", "risk.alert.triggered", decision, order, alert)
		}
	}
	return nil
}

func (e *Engine) publishEnvelope(ctx context.Context, topic, eventType string, decision *pb.RiskDecision, order *pb.Order, payload any) error {
	if e == nil || e.Events == nil || decision == nil || order == nil {
		return nil
	}
	trace := observability.TraceFromContext(ctx)
	envelope := messaging.EventEnvelope{
		EventID:       decision.DecisionID,
		EventType:     eventType,
		AggregateID:   order.OrderId,
		TenantID:      decision.TenantID,
		Source:        e.Source,
		TraceID:       firstNonEmpty(decision.TraceID, trace.TraceID),
		CorrelationID: firstNonEmpty(decision.CorrelationID, trace.RequestID),
		SchemaVersion: decision.Version,
		OccurredAt:    time.Now().UTC(),
		Headers: map[string]string{
			"account_id": order.AccountId,
			"symbol":     order.Symbol,
		},
	}
	return e.Events.PublishJSON(ctx, topic, messaging.PartitionKey(decision.TenantID, order.AccountId, order.OrderId), envelope, payload)
}

func (e *Engine) decisionTTL() time.Duration {
	if e != nil && e.DecisionTTL > 0 {
		return e.DecisionTTL
	}
	return pb.DecisionTTL()
}

func (e *Engine) marketHoursRequired() bool {
	if e == nil {
		return true
	}
	return e.MarketHoursRequired
}

func (e *Engine) maxOrderSize() int32 {
	if e == nil || e.MaxOrderSize <= 0 {
		return 10000
	}
	return e.MaxOrderSize
}

func (e *Engine) maxLeverageLimit() float64 {
	if e == nil || e.MaxLeverage <= 0 {
		return 4.0
	}
	return e.MaxLeverage
}

func (e *Engine) maxOrdersPerMinute() int {
	if e == nil || e.MaxOrdersPerMinute <= 0 {
		return 240
	}
	return e.MaxOrdersPerMinute
}

func (e *Engine) fatFingerLimit() float64 {
	if e == nil || e.FatFingerPct <= 0 {
		return 0.08
	}
	return e.FatFingerPct
}

func (e *Engine) ruleVersion(tenantID string) string {
	if e == nil || e.Cache == nil {
		if e != nil && e.Version != "" {
			return e.Version
		}
		return ""
	}
	if version, ok, err := e.Cache.LoadRuleVersion(context.Background(), tenantID); err == nil && ok && version != "" {
		return version
	}
	if e.Version != "" {
		return e.Version
	}
	return ""
}

func (e *Engine) syntheticAccount(accountID string) *pb.Account {
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
			"source": "synthetic",
			"desk":   "institutional",
		},
	}
}

func orderNotional(order *pb.Order) float64 {
	if order == nil {
		return 0
	}
	return math.Abs(float64(order.Quantity) * order.Price)
}

func effectiveBuyingPower(account *pb.Account) float64 {
	if account == nil {
		return 0
	}
	if account.DayTradingBuyingPower > account.BuyingPower {
		return account.DayTradingBuyingPower
	}
	if account.BuyingPower > 0 {
		return account.BuyingPower
	}
	return account.CashBalance
}

func allowedAccountStatus(rule ruleengine.RuleConfig, status string) bool {
	status = strings.ToUpper(strings.TrimSpace(status))
	if status == "" {
		status = "ACTIVE"
	}
	allowed := splitCSV(rule.Parameters["allowed_statuses"])
	if len(allowed) == 0 {
		allowed = []string{"ACTIVE", "APPROVED"}
	}
	for _, candidate := range allowed {
		if strings.EqualFold(candidate, status) {
			return true
		}
	}
	return false
}

func ruleFloat(values map[string]float64, key string, fallback float64) float64 {
	if values == nil {
		return fallback
	}
	if value, ok := values[key]; ok && value > 0 {
		return value
	}
	return fallback
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

func traceOrRequestID(ctx context.Context) string {
	trace := observability.TraceFromContext(ctx)
	if trace.RequestID != "" {
		return trace.RequestID
	}
	return trace.TraceID
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

func orderFingerprint(order *pb.Order) string {
	if order == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%d|%.4f|%s|%s|%s", order.AccountId, order.Symbol, order.Quantity, order.Price, strings.ToUpper(order.Side), strings.ToUpper(order.OrderType), strings.ToUpper(order.TimeInForce))
}

func stableDecisionID(orderID, idempotencyKey string) string {
	if orderID == "" {
		orderID = "order"
	}
	if idempotencyKey == "" {
		idempotencyKey = "default"
	}
	sanitized := strings.NewReplacer("|", "-", " ", "-", "/", "-", ":", "-").Replace(idempotencyKey)
	return fmt.Sprintf("%s-%s", orderID, sanitized)
}

var (
	marketHoursOnce sync.Once
	marketHoursLoc  *time.Location
)

func marketIsOpen(now time.Time) bool {
	loc := loadMarketHoursLocation()
	local := now.In(loc)
	if local.Weekday() == time.Saturday || local.Weekday() == time.Sunday {
		return false
	}
	open := time.Date(local.Year(), local.Month(), local.Day(), 9, 30, 0, 0, loc)
	close := time.Date(local.Year(), local.Month(), local.Day(), 16, 0, 0, 0, loc)
	return !local.Before(open) && !local.After(close)
}

func loadMarketHoursLocation() *time.Location {
	marketHoursOnce.Do(func() {
		loc, err := time.LoadLocation("America/New_York")
		if err != nil {
			loc = time.FixedZone("ET", -5*60*60)
		}
		marketHoursLoc = loc
	})
	if marketHoursLoc == nil {
		marketHoursLoc = time.FixedZone("ET", -5*60*60)
	}
	return marketHoursLoc
}

func violation(ruleID, description, severity string) *pb.Violation {
	return &pb.Violation{
		RuleId:          ruleID,
		RuleDescription: description,
		Severity:        severity,
	}
}

func ruleSeverity(ruleSeverity, fallback string) string {
	severity := strings.ToUpper(strings.TrimSpace(ruleSeverity))
	if severity == "" {
		return fallback
	}
	return severity
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
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func highestSeverity(violations []*pb.Violation) string {
	severityRank := map[string]int{
		"LOW":      1,
		"MEDIUM":   2,
		"HIGH":     3,
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

func firstViolationRuleID(violations []*pb.Violation) string {
	if len(violations) == 0 || violations[0] == nil {
		return ""
	}
	return violations[0].RuleId
}

func criticalViolation(violations []*pb.Violation) bool {
	for _, v := range violations {
		if v == nil {
			continue
		}
		if strings.EqualFold(v.Severity, "CRITICAL") {
			return true
		}
	}
	return false
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
