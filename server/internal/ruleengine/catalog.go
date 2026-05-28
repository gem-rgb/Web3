package ruleengine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// RuleConfig is the internal representation of a dynamic rule.
type RuleConfig struct {
	RuleID         string            `json:"rule_id"`
	RuleName       string            `json:"rule_name"`
	RuleDescription string           `json:"rule_description"`
	RuleType       string            `json:"rule_type"`
	Severity       string            `json:"severity"`
	Enabled        bool              `json:"enabled"`
	Priority       int               `json:"priority"`
	TenantID       string            `json:"tenant_id"`
	AccountID      string            `json:"account_id"`
	Symbol         string            `json:"symbol"`
	ChainRuleIDs   []string          `json:"chain_rule_ids"`
	ConditionExpression string        `json:"condition_expression"`
	Parameters     map[string]string `json:"parameters"`
	Thresholds     map[string]float64 `json:"thresholds"`
	StopOnFailure  bool              `json:"stop_on_failure"`
	CreatedAt      int64             `json:"created_at"`
	UpdatedAt      int64             `json:"updated_at"`
	CreatedBy      string            `json:"created_by"`
	UpdatedBy      string            `json:"updated_by"`
}

// Snapshot is an immutable rule snapshot.
type Snapshot struct {
	Version  string
	LoadedAt time.Time
	Rules    []RuleConfig
}

// Catalog stores the current rule snapshot.
type Catalog struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

// NewCatalog returns a catalog seeded with default rules.
func NewCatalog(rules []RuleConfig, version string) *Catalog {
	if version == "" {
		version = "built-in-v1"
	}
	return &Catalog{
		snapshot: Snapshot{
			Version:  version,
			LoadedAt: time.Now().UTC(),
			Rules:    cloneRules(rules),
		},
	}
}

// Replace swaps in a new snapshot.
func (c *Catalog) Replace(snapshot Snapshot) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if snapshot.Version == "" {
		snapshot.Version = c.snapshot.Version
	}
	if snapshot.LoadedAt.IsZero() {
		snapshot.LoadedAt = time.Now().UTC()
	}
	snapshot.Rules = cloneRules(snapshot.Rules)
	c.snapshot = snapshot
}

// Snapshot returns a copy of the active snapshot.
func (c *Catalog) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	copy := c.snapshot
	copy.Rules = cloneRules(copy.Rules)
	return copy
}

// ActiveRules returns the rules that match the supplied scope.
func (c *Catalog) ActiveRules(tenantID, accountID, symbol string) []RuleConfig {
	snapshot := c.Snapshot()
	if len(snapshot.Rules) == 0 {
		return nil
	}
	ordered := cloneRules(snapshot.Rules)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftSpecificity := specificity(ordered[i])
		rightSpecificity := specificity(ordered[j])
		if leftSpecificity != rightSpecificity {
			return leftSpecificity < rightSpecificity
		}
		if ordered[i].Priority != ordered[j].Priority {
			return ordered[i].Priority < ordered[j].Priority
		}
		return ordered[i].RuleID < ordered[j].RuleID
	})

	selected := make(map[string]RuleConfig, len(ordered))
	for _, rule := range ordered {
		if !rule.Enabled {
			continue
		}
		if !scopeMatches(rule, tenantID, accountID, symbol) {
			continue
		}
		selected[rule.RuleID] = rule
	}
	rules := make([]RuleConfig, 0, len(selected))
	for _, rule := range selected {
		rules = append(rules, rule)
	}
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority < rules[j].Priority
		}
		if specificity(rules[i]) != specificity(rules[j]) {
			return specificity(rules[i]) > specificity(rules[j])
		}
		return rules[i].RuleID < rules[j].RuleID
	})
	return rules
}

// LoadJSON decodes a snapshot from JSON encoded rule configs.
func LoadJSON(version string, data []byte) (Snapshot, error) {
	data = []byte(strings.TrimSpace(string(data)))
	if len(data) == 0 {
		return Snapshot{Version: version, LoadedAt: time.Now().UTC()}, nil
	}
	var rules []RuleConfig
	if err := json.Unmarshal(data, &rules); err != nil {
		return Snapshot{}, fmt.Errorf("decode rule snapshot: %w", err)
	}
	return Snapshot{
		Version:  version,
		LoadedAt: time.Now().UTC(),
		Rules:    rules,
	}, nil
}

// DefaultRules returns the built-in institutional baseline.
func DefaultRules() []RuleConfig {
	now := time.Now().UTC().UnixMilli()
	return []RuleConfig{
		{
			RuleID:         "max-order-size",
			RuleName:       "Maximum Order Size",
			RuleDescription: "Reject orders that exceed the configured quantity threshold.",
			RuleType:       "MAX_ORDER_SIZE",
			Severity:       "HIGH",
			Enabled:        true,
			Priority:       10,
			Thresholds:     map[string]float64{"max_quantity": 10000},
			ChainRuleIDs:   []string{"buying-power", "leverage"},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "buying-power",
			RuleName:       "Buying Power Validation",
			RuleDescription: "Ensure the notional amount fits within available buying power.",
			RuleType:       "BUYING_POWER",
			Severity:       "CRITICAL",
			Enabled:        true,
			Priority:       20,
			Thresholds:     map[string]float64{"safety_factor": 1},
			ChainRuleIDs:   []string{"restricted-symbol"},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "leverage",
			RuleName:       "Leverage Check",
			RuleDescription: "Keep projected leverage within the configured ceiling.",
			RuleType:       "LEVERAGE",
			Severity:       "HIGH",
			Enabled:        true,
			Priority:       30,
			Thresholds:     map[string]float64{"max_leverage": 4.0},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "market-hours",
			RuleName:       "Market Hours Validation",
			RuleDescription: "Orders must be submitted during the regular cash session.",
			RuleType:       "MARKET_HOURS",
			Severity:       "HIGH",
			Enabled:        true,
			Priority:       40,
			Parameters:     map[string]string{"timezone": "America/New_York"},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "duplicate-order",
			RuleName:       "Duplicate Order Detection",
			RuleDescription: "Reject duplicate orders inside the replay window.",
			RuleType:       "DUPLICATE_ORDER",
			Severity:       "HIGH",
			Enabled:        true,
			Priority:       50,
			Thresholds:     map[string]float64{"window_seconds": 30},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "excessive-frequency",
			RuleName:       "Excessive Frequency Detection",
			RuleDescription: "Detect burst order flow from the same account.",
			RuleType:       "EXCESSIVE_FREQUENCY",
			Severity:       "HIGH",
			Enabled:        true,
			Priority:       60,
			Thresholds:     map[string]float64{"max_orders_per_minute": 240},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "restricted-symbol",
			RuleName:       "Restricted Symbol Validation",
			RuleDescription: "Block symbols that are forbidden for the tenant or account.",
			RuleType:       "RESTRICTED_SYMBOL",
			Severity:       "CRITICAL",
			Enabled:        true,
			Priority:       70,
			Parameters:     map[string]string{"symbols": ""},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "fat-finger",
			RuleName:       "Fat Finger Check",
			RuleDescription: "Reject outlier price inputs and stale quotes.",
			RuleType:       "FAT_FINGER",
			Severity:       "HIGH",
			Enabled:        true,
			Priority:       80,
			Thresholds:     map[string]float64{"max_deviation_pct": 0.08},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
		{
			RuleID:         "account-status",
			RuleName:       "Account Status Validation",
			RuleDescription: "Allow only active trading accounts.",
			RuleType:       "ACCOUNT_STATUS",
			Severity:       "CRITICAL",
			Enabled:        true,
			Priority:       90,
			Parameters:     map[string]string{"allowed_statuses": "ACTIVE,APPROVED"},
			CreatedAt:      now,
			UpdatedAt:      now,
			CreatedBy:      "system",
			UpdatedBy:      "system",
		},
	}
}

func scopeMatches(rule RuleConfig, tenantID, accountID, symbol string) bool {
	if rule.TenantID != "" && !strings.EqualFold(rule.TenantID, tenantID) {
		return false
	}
	if rule.AccountID != "" && !strings.EqualFold(rule.AccountID, accountID) {
		return false
	}
	if rule.Symbol != "" && !strings.EqualFold(rule.Symbol, symbol) {
		return false
	}
	return true
}

func specificity(rule RuleConfig) int {
	score := 0
	if strings.TrimSpace(rule.TenantID) != "" {
		score++
	}
	if strings.TrimSpace(rule.AccountID) != "" {
		score++
	}
	if strings.TrimSpace(rule.Symbol) != "" {
		score++
	}
	return score
}

func cloneRules(rules []RuleConfig) []RuleConfig {
	if len(rules) == 0 {
		return nil
	}
	result := make([]RuleConfig, 0, len(rules))
	for _, rule := range rules {
		copy := rule
		if rule.ChainRuleIDs != nil {
			copy.ChainRuleIDs = append([]string(nil), rule.ChainRuleIDs...)
		}
		if rule.Parameters != nil {
			copy.Parameters = make(map[string]string, len(rule.Parameters))
			for k, v := range rule.Parameters {
				copy.Parameters[k] = v
			}
		}
		if rule.Thresholds != nil {
			copy.Thresholds = make(map[string]float64, len(rule.Thresholds))
			for k, v := range rule.Thresholds {
				copy.Thresholds[k] = v
			}
		}
		result = append(result, copy)
	}
	return result
}
