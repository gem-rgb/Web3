package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "github.com/example/rms/shared/proto"
)

// DecisionStore persists canonical decisions to disk so the risk pipeline has
// a durable query path even without a database driver in the local workspace.
type DecisionStore struct {
	path string

	mu           sync.RWMutex
	byOrder      map[string]*pb.RiskDecision
	byCorrelation map[string]*pb.RiskDecision
	recent       []*pb.RiskDecision
}

// NewDecisionStore creates or loads a JSONL-backed store.
func NewDecisionStore(path string) (*DecisionStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = filepath.Join("data", "risk-decisions.jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	store := &DecisionStore{
		path:         path,
		byOrder:      map[string]*pb.RiskDecision{},
		byCorrelation: map[string]*pb.RiskDecision{},
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

// Save appends the decision to the JSONL file and updates the in-memory index.
func (s *DecisionStore) Save(_ context.Context, decision *pb.RiskDecision) error {
	if s == nil || decision == nil {
		return nil
	}
	copy := cloneDecision(decision)
	if copy.PersistedAt == 0 {
		copy.PersistedAt = time.Now().UTC().UnixMilli()
	}
	payload, err := json.Marshal(copy)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(append(payload, '\n')); err != nil {
		return err
	}

	s.indexLocked(copy)
	return nil
}

// GetByOrderID returns the latest decision for an order.
func (s *DecisionStore) GetByOrderID(_ context.Context, orderID string) (*pb.RiskDecision, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	decision, ok := s.byOrder[orderID]
	if !ok {
		return nil, false
	}
	return cloneDecision(decision), true
}

// GetByCorrelationID returns the latest decision for a correlation identifier.
func (s *DecisionStore) GetByCorrelationID(_ context.Context, correlationID string) (*pb.RiskDecision, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	decision, ok := s.byCorrelation[correlationID]
	if !ok {
		return nil, false
	}
	return cloneDecision(decision), true
}

// ListRecent returns the most recent decisions up to the requested limit.
func (s *DecisionStore) ListRecent(_ context.Context, limit int) []*pb.RiskDecision {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > len(s.recent) {
		limit = len(s.recent)
	}
	results := make([]*pb.RiskDecision, 0, limit)
	for i := len(s.recent) - 1; i >= 0 && len(results) < limit; i-- {
		results = append(results, cloneDecision(s.recent[i]))
	}
	return results
}

// ListByAccount filters recent decisions by account identifier.
func (s *DecisionStore) ListByAccount(_ context.Context, accountID string, limit int) []*pb.RiskDecision {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	results := make([]*pb.RiskDecision, 0, limit)
	for i := len(s.recent) - 1; i >= 0 && (limit <= 0 || len(results) < limit); i-- {
		if s.recent[i] != nil && strings.EqualFold(s.recent[i].AccountID, accountID) {
			results = append(results, cloneDecision(s.recent[i]))
		}
	}
	return results
}

func (s *DecisionStore) load() error {
	file, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var decision pb.RiskDecision
		if err := json.Unmarshal([]byte(line), &decision); err != nil {
			return fmt.Errorf("load decision store: %w", err)
		}
		s.indexLocked(&decision)
	}
	return scanner.Err()
}

func (s *DecisionStore) indexLocked(decision *pb.RiskDecision) {
	if decision == nil {
		return
	}
	copy := cloneDecision(decision)
	if copy.PersistedAt == 0 {
		copy.PersistedAt = time.Now().UTC().UnixMilli()
	}
	s.byOrder[copy.OrderID] = copy
	if copy.CorrelationID != "" {
		s.byCorrelation[copy.CorrelationID] = copy
	}
	s.recent = append(s.recent, copy)
	if len(s.recent) > 2048 {
		s.recent = append([]*pb.RiskDecision(nil), s.recent[len(s.recent)-2048:]...)
	}
}

func cloneDecision(decision *pb.RiskDecision) *pb.RiskDecision {
	if decision == nil {
		return nil
	}
	copy := *decision
	if decision.Violations != nil {
		copy.Violations = make([]*pb.Violation, 0, len(decision.Violations))
		for _, violation := range decision.Violations {
			if violation == nil {
				continue
			}
			v := *violation
			copy.Violations = append(copy.Violations, &v)
		}
	}
	if decision.Metadata != nil {
		copy.Metadata = make(map[string]string, len(decision.Metadata))
		for k, v := range decision.Metadata {
			copy.Metadata[k] = v
		}
	}
	return &copy
}
