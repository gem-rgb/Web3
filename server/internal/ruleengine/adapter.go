package ruleengine

import (
	"strings"
	"time"

	pb "github.com/example/rms/shared/proto"
)

// FromProto converts shared proto rules into the internal rule catalog shape.
func FromProto(rules []*pb.Rule, version string) Snapshot {
	configs := make([]RuleConfig, 0, len(rules))
	for _, rule := range rules {
		if rule == nil {
			continue
		}
		configs = append(configs, RuleConfig{
			RuleID:         rule.RuleId,
			RuleName:       rule.RuleName,
			RuleDescription: rule.RuleDescription,
			RuleType:       rule.RuleType,
			Severity:       rule.Severity,
			Enabled:        rule.Enabled,
			Priority:       int(rule.Priority),
			TenantID:       rule.TenantId,
			AccountID:      rule.AccountId,
			Symbol:         rule.Symbol,
			ChainRuleIDs:   append([]string(nil), rule.ChainRuleIds...),
			ConditionExpression: rule.ConditionExpression,
			Parameters:     cloneStringMap(rule.Parameters),
			Thresholds:     cloneFloatMap(rule.Thresholds),
			StopOnFailure:  rule.StopOnFailure,
			CreatedAt:      rule.CreatedAt,
			UpdatedAt:      rule.UpdatedAt,
			CreatedBy:      rule.CreatedBy,
			UpdatedBy:      rule.UpdatedBy,
		})
	}
	return Snapshot{
		Version:  version,
		LoadedAt: time.Now().UTC(),
		Rules:    configs,
	}
}

// ToProto converts an internal rule snapshot to the shared proto shape.
func ToProto(rules []RuleConfig) []*pb.Rule {
	result := make([]*pb.Rule, 0, len(rules))
	for _, rule := range rules {
		copy := rule
		result = append(result, &pb.Rule{
			RuleId:              copy.RuleID,
			RuleName:            copy.RuleName,
			RuleDescription:     copy.RuleDescription,
			RuleType:            copy.RuleType,
			Severity:            copy.Severity,
			Enabled:             copy.Enabled,
			Priority:            int32(copy.Priority),
			TenantId:            copy.TenantID,
			AccountId:           copy.AccountID,
			Symbol:              copy.Symbol,
			ChainRuleIds:        append([]string(nil), copy.ChainRuleIDs...),
			ConditionExpression: copy.ConditionExpression,
			Parameters:          cloneStringMap(copy.Parameters),
			Thresholds:          cloneFloatMap(copy.Thresholds),
			StopOnFailure:       copy.StopOnFailure,
			CreatedAt:           copy.CreatedAt,
			UpdatedAt:           copy.UpdatedAt,
			CreatedBy:           copy.CreatedBy,
			UpdatedBy:           copy.UpdatedBy,
		})
	}
	return result
}

// NormalizeSymbol extracts the canonical symbol from a rule or order input.
func NormalizeSymbol(symbol string) string {
	return strings.ToUpper(strings.TrimSpace(symbol))
}
