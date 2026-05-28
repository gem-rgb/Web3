package proto

import "time"

// OrderState captures the lifecycle of an order as it moves through the phase 2 pipeline.
type OrderState string

const (
	OrderStateReceived  OrderState = "RECEIVED"
	OrderStateEvaluated OrderState = "EVALUATED"
	OrderStateApproved  OrderState = "APPROVED"
	OrderStateRejected  OrderState = "REJECTED"
	OrderStateDuplicate OrderState = "DUPLICATE"
	OrderStateFailed    OrderState = "FAILED"
)

// OrderIngestionRequest is the canonical command accepted by the order ingestion service.
type OrderIngestionRequest struct {
	Order          *Order            `json:"order,omitempty"`
	Account        *Account          `json:"account,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	Source         string            `json:"source,omitempty"`
	RequestedBy    string            `json:"requested_by,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// OrderIngestionResponse is returned to callers once the order has been evaluated.
type OrderIngestionResponse struct {
	OrderID       string        `json:"order_id,omitempty"`
	State         OrderState    `json:"state,omitempty"`
	Accepted      bool          `json:"accepted,omitempty"`
	Decision      *RiskDecision `json:"decision,omitempty"`
	CorrelationID string        `json:"correlation_id,omitempty"`
	TraceID       string        `json:"trace_id,omitempty"`
	Timestamp     int64         `json:"timestamp,omitempty"`
}

// OrderDecisionQueryRequest fetches the latest decision for an order.
type OrderDecisionQueryRequest struct {
	OrderID       string `json:"order_id,omitempty"`
	AccountID     string `json:"account_id,omitempty"`
	TenantID      string `json:"tenant_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// OrderDecisionQueryResponse returns a decision when one is present in the cache or store.
type OrderDecisionQueryResponse struct {
	Decision  *RiskDecision `json:"decision,omitempty"`
	Found     bool          `json:"found,omitempty"`
	Timestamp int64         `json:"timestamp,omitempty"`
}

// RiskDecision is the canonical persisted decision record.
type RiskDecision struct {
	DecisionID     string            `json:"decision_id,omitempty"`
	OrderID        string            `json:"order_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	TraceID        string            `json:"trace_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Engine         string            `json:"engine,omitempty"`
	RuleVersion    string            `json:"rule_version,omitempty"`
	Approved       bool              `json:"approved,omitempty"`
	RejectReason   string            `json:"reject_reason,omitempty"`
	Violations     []*Violation      `json:"violations,omitempty"`
	DecisionSource string            `json:"decision_source,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	EvaluatedAt    int64             `json:"evaluated_at,omitempty"`
	PersistedAt    int64             `json:"persisted_at,omitempty"`
	LatencyMicros  int64             `json:"latency_micros,omitempty"`
	Version        string            `json:"version,omitempty"`
}

// OrderReceivedEvent is emitted as soon as the order is accepted by the edge.
type OrderReceivedEvent struct {
	EventID        string            `json:"event_id,omitempty"`
	OrderID        string            `json:"order_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	TraceID        string            `json:"trace_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	ReceivedAt     int64             `json:"received_at,omitempty"`
	Source         string            `json:"source,omitempty"`
	Order          *Order            `json:"order,omitempty"`
	Account        *Account          `json:"account,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// RiskEvaluationRequestedEvent is emitted when an order is queued for evaluation.
type RiskEvaluationRequestedEvent struct {
	EventID        string            `json:"event_id,omitempty"`
	OrderID        string            `json:"order_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	TraceID        string            `json:"trace_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	RequestedAt    int64             `json:"requested_at,omitempty"`
	Reason         string            `json:"reason,omitempty"`
	Order          *Order            `json:"order,omitempty"`
	Account        *Account          `json:"account,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// RiskEvaluationCompletedEvent is emitted after the canonical decision is written.
type RiskEvaluationCompletedEvent struct {
	EventID       string            `json:"event_id,omitempty"`
	OrderID       string            `json:"order_id,omitempty"`
	AccountID     string            `json:"account_id,omitempty"`
	TenantID      string            `json:"tenant_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	CompletedAt   int64             `json:"completed_at,omitempty"`
	Decision      *RiskDecision     `json:"decision,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// OrderApprovedEvent is published for approved orders.
type OrderApprovedEvent struct {
	EventID       string            `json:"event_id,omitempty"`
	OrderID       string            `json:"order_id,omitempty"`
	AccountID     string            `json:"account_id,omitempty"`
	TenantID      string            `json:"tenant_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	ApprovedAt    int64             `json:"approved_at,omitempty"`
	Decision      *RiskDecision     `json:"decision,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// OrderRejectedEvent is published for rejected orders.
type OrderRejectedEvent struct {
	EventID       string            `json:"event_id,omitempty"`
	OrderID       string            `json:"order_id,omitempty"`
	AccountID     string            `json:"account_id,omitempty"`
	TenantID      string            `json:"tenant_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	RejectedAt    int64             `json:"rejected_at,omitempty"`
	Decision      *RiskDecision     `json:"decision,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// RiskAlertTriggeredEvent is emitted when a high-severity rule breach requires alerting.
type RiskAlertTriggeredEvent struct {
	EventID       string            `json:"event_id,omitempty"`
	AlertID       string            `json:"alert_id,omitempty"`
	OrderID       string            `json:"order_id,omitempty"`
	AccountID     string            `json:"account_id,omitempty"`
	TenantID      string            `json:"tenant_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	RuleID        string            `json:"rule_id,omitempty"`
	Severity      string            `json:"severity,omitempty"`
	Message       string            `json:"message,omitempty"`
	TriggeredAt   int64             `json:"triggered_at,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// RuleReloadRequest triggers a hot reload of the rule catalog.
type RuleReloadRequest struct {
	TenantID string `json:"tenant_id,omitempty"`
	Source   string `json:"source,omitempty"`
}

// RuleReloadResponse reports the outcome of a catalog reload.
type RuleReloadResponse struct {
	Reloaded  bool  `json:"reloaded,omitempty"`
	Timestamp int64 `json:"timestamp,omitempty"`
}

// DecisionTTL returns the recommended retention window for cached decisions.
func DecisionTTL() time.Duration {
	return 24 * time.Hour
}
