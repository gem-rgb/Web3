package proto

// Common domain objects shared by the service examples.

type Order struct {
	OrderId     string            `json:"order_id,omitempty"`
	AccountId   string            `json:"account_id,omitempty"`
	Symbol      string            `json:"symbol,omitempty"`
	Quantity    int32             `json:"quantity,omitempty"`
	Price       float64           `json:"price,omitempty"`
	Side        string            `json:"side,omitempty"`
	OrderType   string            `json:"order_type,omitempty"`
	TimeInForce string            `json:"time_in_force,omitempty"`
	Timestamp   int64             `json:"timestamp,omitempty"`
	StrategyId  string            `json:"strategy_id,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type Account struct {
	AccountId               string            `json:"account_id,omitempty"`
	TenantId                string            `json:"tenant_id,omitempty"`
	UserId                  string            `json:"user_id,omitempty"`
	Status                  string            `json:"status,omitempty"`
	BuyingPower             float64           `json:"buying_power,omitempty"`
	CashBalance             float64           `json:"cash_balance,omitempty"`
	MarketValue             float64           `json:"market_value,omitempty"`
	DayTradingBuyingPower   float64           `json:"day_trading_buying_power,omitempty"`
	MaintenanceMarginExcess float64           `json:"maintenance_margin_excess,omitempty"`
	Metadata                map[string]string `json:"metadata,omitempty"`
}

type Position struct {
	TenantId        string  `json:"tenant_id,omitempty"`
	AccountId       string  `json:"account_id,omitempty"`
	Symbol          string  `json:"symbol,omitempty"`
	Sector          string  `json:"sector,omitempty"`
	Quantity        int32   `json:"quantity,omitempty"`
	AveragePrice    float64 `json:"average_price,omitempty"`
	MarketPrice     float64 `json:"market_price,omitempty"`
	MarketValue     float64 `json:"market_value,omitempty"`
	CostBasis       float64 `json:"cost_basis,omitempty"`
	UnrealizedPL    float64 `json:"unrealized_pl,omitempty"`
	RealizedPL      float64 `json:"realized_pl,omitempty"`
	GrossExposure   float64 `json:"gross_exposure,omitempty"`
	NetExposure     float64 `json:"net_exposure,omitempty"`
	Leverage        float64 `json:"leverage,omitempty"`
	Side            string  `json:"side,omitempty"`
	Version         int64   `json:"version,omitempty"`
	Sequence        int64   `json:"sequence,omitempty"`
	SnapshotVersion string  `json:"snapshot_version,omitempty"`
	UpdatedAt       int64   `json:"updated_at,omitempty"`
	Timestamp       int64   `json:"timestamp,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

type RiskCheckResult struct {
	Approved     bool        `json:"approved,omitempty"`
	RejectReason string      `json:"reject_reason,omitempty"`
	Violations   []*Violation `json:"violations,omitempty"`
	Timestamp    int64       `json:"timestamp,omitempty"`
}

type Violation struct {
	RuleId          string `json:"rule_id,omitempty"`
	RuleDescription string `json:"rule_description,omitempty"`
	Severity        string `json:"severity,omitempty"`
}

type MarketDataUpdate struct {
	Symbol     string  `json:"symbol,omitempty"`
	BidPrice   float64 `json:"bid_price,omitempty"`
	AskPrice   float64 `json:"ask_price,omitempty"`
	LastPrice  float64 `json:"last_price,omitempty"`
	Volume     int64   `json:"volume,omitempty"`
	Timestamp  int64   `json:"timestamp,omitempty"`
}

type AggregatedExposure struct {
	TenantId        string            `json:"tenant_id,omitempty"`
	AccountId       string            `json:"account_id,omitempty"`
	Symbol          string            `json:"symbol,omitempty"`
	Sector          string            `json:"sector,omitempty"`
	NetQuantity     int32             `json:"net_quantity,omitempty"`
	GrossExposure   float64           `json:"gross_exposure,omitempty"`
	NetExposure     float64           `json:"net_exposure,omitempty"`
	LongExposure    float64           `json:"long_exposure,omitempty"`
	ShortExposure   float64           `json:"short_exposure,omitempty"`
	MarketValue     float64           `json:"market_value,omitempty"`
	UnrealizedPL    float64           `json:"unrealized_pl,omitempty"`
	RealizedPL      float64           `json:"realized_pl,omitempty"`
	Leverage        float64           `json:"leverage,omitempty"`
	ConcentrationPct float64          `json:"concentration_pct,omitempty"`
	PositionCount   int32             `json:"position_count,omitempty"`
	Version         int64             `json:"version,omitempty"`
	Sequence        int64             `json:"sequence,omitempty"`
	SnapshotVersion string            `json:"snapshot_version,omitempty"`
	UpdatedAt       int64             `json:"updated_at,omitempty"`
	Timestamp       int64             `json:"timestamp,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

type MarginRequirement struct {
	AccountId         string  `json:"account_id,omitempty"`
	InitialMargin     float64 `json:"initial_margin,omitempty"`
	MaintenanceMargin float64 `json:"maintenance_margin,omitempty"`
	MarginExcess      float64 `json:"margin_excess,omitempty"`
	MarginRatio       float64 `json:"margin_ratio,omitempty"`
	Timestamp         int64   `json:"timestamp,omitempty"`
}

// We use AlertRecord rather than Alert so the combined package does not collide
// with the alerting service's request wrapper.
type AlertRecord struct {
	AlertId   string            `json:"alert_id,omitempty"`
	AccountId string            `json:"account_id,omitempty"`
	RuleId    string            `json:"rule_id,omitempty"`
	Severity  string            `json:"severity,omitempty"`
	Message   string            `json:"message,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Alert = AlertRecord

type Rule struct {
	RuleId              string            `json:"rule_id,omitempty"`
	RuleName            string            `json:"rule_name,omitempty"`
	RuleDescription     string            `json:"rule_description,omitempty"`
	RuleType            string            `json:"rule_type,omitempty"`
	Severity            string            `json:"severity,omitempty"`
	Enabled             bool              `json:"enabled,omitempty"`
	Priority            int32             `json:"priority,omitempty"`
	TenantId            string            `json:"tenant_id,omitempty"`
	AccountId           string            `json:"account_id,omitempty"`
	Symbol              string            `json:"symbol,omitempty"`
	ChainRuleIds        []string          `json:"chain_rule_ids,omitempty"`
	ConditionExpression string            `json:"condition_expression,omitempty"`
	Parameters          map[string]string `json:"parameters,omitempty"`
	Thresholds          map[string]float64 `json:"thresholds,omitempty"`
	StopOnFailure       bool              `json:"stop_on_failure,omitempty"`
	CreatedAt           int64             `json:"created_at,omitempty"`
	UpdatedAt           int64             `json:"updated_at,omitempty"`
	CreatedBy           string            `json:"created_by,omitempty"`
	UpdatedBy           string            `json:"updated_by,omitempty"`
}

// Shared order evaluation messages.
type OrderRequest struct {
	Order   *Order   `json:"order,omitempty"`
	Account *Account `json:"account,omitempty"`
}

type OrderResponse struct {
	OrderId      string       `json:"order_id,omitempty"`
	Approved     bool         `json:"approved,omitempty"`
	RejectReason string       `json:"reject_reason,omitempty"`
	Violations   []*Violation `json:"violations,omitempty"`
	Timestamp    int64        `json:"timestamp,omitempty"`
}

type BatchOrderRequest struct {
	Orders   []*Order   `json:"orders,omitempty"`
	Accounts []*Account `json:"accounts,omitempty"`
}

type BatchOrderResponse struct {
	Responses []*OrderResponse `json:"responses,omitempty"`
	Timestamp int64            `json:"timestamp,omitempty"`
}

// API gateway specific messages.
type CancelOrderRequest struct {
	OrderId   string `json:"order_id,omitempty"`
	AccountId string `json:"account_id,omitempty"`
}

type CancelOrderResponse struct {
	Success      bool   `json:"success,omitempty"`
	RejectReason string `json:"reject_reason,omitempty"`
	Timestamp    int64  `json:"timestamp,omitempty"`
}

type AccountInfoRequest struct {
	AccountId string `json:"account_id,omitempty"`
}

type AccountInfoResponse struct {
	Account          *Account              `json:"account,omitempty"`
	Positions        []*Position           `json:"positions,omitempty"`
	MarginRequirement *MarginRequirement    `json:"margin_requirement,omitempty"`
	Exposures        []*AggregatedExposure `json:"exposures,omitempty"`
	Timestamp        int64                 `json:"timestamp,omitempty"`
}

type MarketDataRequest struct {
	Symbols []string `json:"symbols,omitempty"`
}

type MarketDataResponse struct {
	MarketData []*MarketDataUpdate `json:"market_data,omitempty"`
	Timestamp  int64               `json:"timestamp,omitempty"`
}

type MarketDataSubscriptionRequest struct {
	Symbols []string `json:"symbols,omitempty"`
}

type MarketDataHistoryRequest struct {
	Symbol    string `json:"symbol,omitempty"`
	StartTime int64  `json:"start_time,omitempty"`
	EndTime   int64  `json:"end_time,omitempty"`
	Limit     int32  `json:"limit,omitempty"`
}

type MarketDataHistoryResponse struct {
	MarketData []*MarketDataUpdate `json:"market_data,omitempty"`
	Timestamp  int64               `json:"timestamp,omitempty"`
}

// Exposure aggregation messages.
type PositionUpdate struct {
	Position     *Position          `json:"position,omitempty"`
	IsNew        bool               `json:"is_new,omitempty"`
	TenantId     string             `json:"tenant_id,omitempty"`
	CorrelationID string             `json:"correlation_id,omitempty"`
	TraceID      string             `json:"trace_id,omitempty"`
	Metadata     map[string]string  `json:"metadata,omitempty"`
}

type AccountExposureRequest struct {
	TenantId  string `json:"tenant_id,omitempty"`
	AccountId string `json:"account_id,omitempty"`
}

type AccountExposureResponse struct {
	Exposures       []*AggregatedExposure `json:"exposures,omitempty"`
	Timestamp       int64                 `json:"timestamp,omitempty"`
	SnapshotVersion string                `json:"snapshot_version,omitempty"`
}

type SymbolExposureRequest struct {
	TenantId string `json:"tenant_id,omitempty"`
	Symbol   string `json:"symbol,omitempty"`
}

type SymbolExposureResponse struct {
	Exposures       []*AggregatedExposure `json:"exposures,omitempty"`
	Timestamp       int64                 `json:"timestamp,omitempty"`
	SnapshotVersion string                `json:"snapshot_version,omitempty"`
}

// Margin calculation messages.
type MarginCalculationRequest struct {
	AccountId  string `json:"account_id,omitempty"`
	ForceRecalc bool   `json:"force_recalc,omitempty"`
}

type MarginCalculationResponse struct {
	MarginRequirement *MarginRequirement `json:"margin_requirement,omitempty"`
	Timestamp         int64              `json:"timestamp,omitempty"`
}

type AccountMarginRequest struct {
	AccountId string `json:"account_id,omitempty"`
}

type AccountMarginResponse struct {
	MarginRequirement *MarginRequirement `json:"margin_requirement,omitempty"`
	Timestamp         int64              `json:"timestamp,omitempty"`
}

// Position tracking messages.
type PositionUpdateRequest struct {
	Position      *Position          `json:"position,omitempty"`
	IsNew         bool               `json:"is_new,omitempty"`
	TenantId      string             `json:"tenant_id,omitempty"`
	CorrelationID string             `json:"correlation_id,omitempty"`
	TraceID       string             `json:"trace_id,omitempty"`
	Metadata      map[string]string  `json:"metadata,omitempty"`
}

type PositionUpdateResponse struct {
	Position        *Position `json:"position,omitempty"`
	Timestamp       int64     `json:"timestamp,omitempty"`
	SnapshotVersion string    `json:"snapshot_version,omitempty"`
}

type PositionRequest struct {
	TenantId  string `json:"tenant_id,omitempty"`
	AccountId string `json:"account_id,omitempty"`
	Symbol    string `json:"symbol,omitempty"`
}

type PositionResponse struct {
	Position        *Position `json:"position,omitempty"`
	Timestamp       int64     `json:"timestamp,omitempty"`
	SnapshotVersion string    `json:"snapshot_version,omitempty"`
}

type AccountPositionRequest struct {
	TenantId      string `json:"tenant_id,omitempty"`
	AccountId     string `json:"account_id,omitempty"`
	IncludeClosed bool   `json:"include_closed,omitempty"`
}

type AccountPositionResponse struct {
	Positions       []*Position `json:"positions,omitempty"`
	Timestamp       int64       `json:"timestamp,omitempty"`
	SnapshotVersion string      `json:"snapshot_version,omitempty"`
}

// Rule engine messages.
type GetActiveRulesRequest struct {
	RuleType  string `json:"rule_type,omitempty"`
	AccountId string `json:"account_id,omitempty"`
	TenantId  string `json:"tenant_id,omitempty"`
	Symbol    string `json:"symbol,omitempty"`
}

type GetActiveRulesResponse struct {
	Rules     []*Rule `json:"rules,omitempty"`
	Timestamp int64   `json:"timestamp,omitempty"`
}

type RuleEvaluationRequest struct {
	Order      *Order             `json:"order,omitempty"`
	Account    *Account           `json:"account,omitempty"`
	Positions  []*Position        `json:"positions,omitempty"`
	MarketData []*MarketDataUpdate `json:"market_data,omitempty"`
	TenantId   string             `json:"tenant_id,omitempty"`
	AccountOverride string         `json:"account_override,omitempty"`
}

type RuleEvaluationResponse struct {
	Approved     bool         `json:"approved,omitempty"`
	RejectReason string       `json:"reject_reason,omitempty"`
	Violations   []*Violation `json:"violations,omitempty"`
	Timestamp    int64        `json:"timestamp,omitempty"`
}

type RuleUpdateRequest struct {
	RuleId string `json:"rule_id,omitempty"`
}

// Alerting messages.
type AlertRequest struct {
	Alert *AlertRecord `json:"alert,omitempty"`
}

type AlertResponse struct {
	AlertId   string `json:"alert_id,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

type AccountAlertRequest struct {
	AccountId string `json:"account_id,omitempty"`
	Limit     int32  `json:"limit,omitempty"`
	StartTime int64  `json:"start_time,omitempty"`
	EndTime   int64  `json:"end_time,omitempty"`
}

type AccountAlertResponse struct {
	Alerts    []*AlertRecord `json:"alerts,omitempty"`
	Timestamp int64          `json:"timestamp,omitempty"`
}

type RuleAlertRequest struct {
	RuleId    string `json:"rule_id,omitempty"`
	Limit     int32  `json:"limit,omitempty"`
	StartTime int64  `json:"start_time,omitempty"`
	EndTime   int64  `json:"end_time,omitempty"`
}

type RuleAlertResponse struct {
	Alerts    []*AlertRecord `json:"alerts,omitempty"`
	Timestamp int64          `json:"timestamp,omitempty"`
}

type AlertSubscriptionRequest struct {
	AccountId string `json:"account_id,omitempty"`
	RuleId    string `json:"rule_id,omitempty"`
}

// Audit logging messages.
type AuditEvent struct {
	EventId        string            `json:"event_id,omitempty"`
	EventType      string            `json:"event_type,omitempty"`
	AccountId      string            `json:"account_id,omitempty"`
	UserId         string            `json:"user_id,omitempty"`
	ServiceName    string            `json:"service_name,omitempty"`
	PayloadJSON    string            `json:"payload_json,omitempty"`
	Timestamp      int64             `json:"timestamp,omitempty"`
	IngestTimestamp int64            `json:"ingest_timestamp,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type AuditResponse struct {
	EventId   string `json:"event_id,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

type AuditQueryRequest struct {
	AccountId   string `json:"account_id,omitempty"`
	EventType   string `json:"event_type,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	StartTime   int64  `json:"start_time,omitempty"`
	EndTime     int64  `json:"end_time,omitempty"`
	Limit       int32  `json:"limit,omitempty"`
	Offset      int32  `json:"offset,omitempty"`
}

type AuditQueryResponse struct {
	Events     []*AuditEvent `json:"events,omitempty"`
	Timestamp  int64         `json:"timestamp,omitempty"`
	TotalCount int32         `json:"total_count,omitempty"`
}
