package main

import (
	"context"
	"encoding/json"
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
	"github.com/segmentio/kafka-go"

	"github.com/example/rms/shared/platform/grpcx"
	"github.com/example/rms/shared/platform/observability"
	pb "github.com/example/rms/shared/proto"
)

type alertingService struct {
	pb.UnimplementedAlertingServiceServer
	mu          sync.RWMutex
	alerts      []*pb.AlertRecord
	subscribers map[int]chan *pb.AlertRecord
	nextSubID   int
	kafkaWriter *kafka.Writer
	topicPrefix string
}

func newAlertingService() *alertingService {
	svc := &alertingService{
		alerts:      []*pb.AlertRecord{},
		subscribers: make(map[int]chan *pb.AlertRecord),
		topicPrefix: topicPrefixFromEnv(),
	}
	if brokers := kafkaBrokersFromEnv(); len(brokers) > 0 {
		svc.kafkaWriter = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: false,
			Async:                  false,
			BatchTimeout:           25 * time.Millisecond,
			BatchSize:              64,
		}
	}
	return svc
}

func (s *alertingService) SendAlert(ctx context.Context, req *pb.AlertRequest) (*pb.AlertResponse, error) {
	if req == nil || req.Alert == nil {
		return nil, fmt.Errorf("alert payload is required")
	}

	alert := cloneAlert(req.Alert)
	if alert.AlertId == "" {
		alert.AlertId = fmt.Sprintf("alert-%d", time.Now().UnixNano())
	}
	if alert.Timestamp == 0 {
		alert.Timestamp = time.Now().UnixMilli()
	}

	s.mu.Lock()
	s.alerts = append(s.alerts, alert)
	s.broadcastLocked(alert)
	s.mu.Unlock()
	s.publishKafka(ctx, alert)

	log.Printf("alert stored alert_id=%s account_id=%s rule_id=%s severity=%s", alert.AlertId, alert.AccountId, alert.RuleId, alert.Severity)
	return &pb.AlertResponse{
		AlertId:   alert.AlertId,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *alertingService) GetAlertsForAccount(ctx context.Context, req *pb.AccountAlertRequest) (*pb.AccountAlertResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account alert request is required")
	}
	matches := s.filterAlerts(func(alert *pb.AlertRecord) bool {
		return req.AccountId == "" || strings.EqualFold(alert.AccountId, req.AccountId)
	}, req.StartTime, req.EndTime, req.Limit)
	return &pb.AccountAlertResponse{
		Alerts:    matches,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *alertingService) GetAlertsForRule(ctx context.Context, req *pb.RuleAlertRequest) (*pb.RuleAlertResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("rule alert request is required")
	}
	matches := s.filterAlerts(func(alert *pb.AlertRecord) bool {
		return req.RuleId == "" || strings.EqualFold(alert.RuleId, req.RuleId)
	}, req.StartTime, req.EndTime, req.Limit)
	return &pb.RuleAlertResponse{
		Alerts:    matches,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *alertingService) StreamAlerts(req *pb.AlertSubscriptionRequest, stream pb.AlertingService_StreamAlertsServer) error {
	if req == nil {
		return fmt.Errorf("alert subscription request is required")
	}

	subID, ch := s.subscribe()
	defer s.unsubscribe(subID)

	backlog := s.filterAlerts(func(alert *pb.AlertRecord) bool {
		if req.AccountId != "" && !strings.EqualFold(req.AccountId, alert.AccountId) {
			return false
		}
		if req.RuleId != "" && !strings.EqualFold(req.RuleId, alert.RuleId) {
			return false
		}
		return true
	}, 0, 0, 0)
	for _, alert := range backlog {
		if err := stream.Send(alert); err != nil {
			return err
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case alert := <-ch:
			if alert == nil {
				continue
			}
			if req.AccountId != "" && !strings.EqualFold(req.AccountId, alert.AccountId) {
				continue
			}
			if req.RuleId != "" && !strings.EqualFold(req.RuleId, alert.RuleId) {
				continue
			}
			if err := stream.Send(alert); err != nil {
				return err
			}
		}
	}
}

func (s *alertingService) filterAlerts(match func(*pb.AlertRecord) bool, startTime, endTime int64, limit int32) []*pb.AlertRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	results := make([]*pb.AlertRecord, 0, len(s.alerts))
	for i := len(s.alerts) - 1; i >= 0; i-- {
		alert := s.alerts[i]
		if !match(alert) {
			continue
		}
		if startTime > 0 && alert.Timestamp < startTime {
			continue
		}
		if endTime > 0 && alert.Timestamp > endTime {
			continue
		}
		results = append(results, cloneAlert(alert))
		if int32(len(results)) >= limit {
			break
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Timestamp < results[j].Timestamp })
	return results
}

func (s *alertingService) subscribe() (int, chan *pb.AlertRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSubID++
	id := s.nextSubID
	ch := make(chan *pb.AlertRecord, 64)
	s.subscribers[id] = ch
	return id, ch
}

func (s *alertingService) unsubscribe(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

func (s *alertingService) broadcastLocked(alert *pb.AlertRecord) {
	for _, ch := range s.subscribers {
		select {
		case ch <- cloneAlert(alert):
		default:
			// Slow subscribers are dropped rather than blocking alert ingestion.
		}
	}
}

func cloneAlert(alert *pb.AlertRecord) *pb.AlertRecord {
	if alert == nil {
		return nil
	}
	copy := *alert
	if alert.Metadata != nil {
		copy.Metadata = make(map[string]string, len(alert.Metadata))
		for k, v := range alert.Metadata {
			copy.Metadata[k] = v
		}
	}
	return &copy
}

func (s *alertingService) publishKafka(ctx context.Context, alert *pb.AlertRecord) {
	if s == nil || s.kafkaWriter == nil || alert == nil {
		return
	}

	trace := observability.TraceFromContext(ctx)
	tenantID := "default"
	if alert.Metadata != nil {
		if value := strings.TrimSpace(alert.Metadata["tenant_id"]); value != "" {
			tenantID = value
		}
	}
	payload, err := json.Marshal(map[string]any{
		"event_id":       fmt.Sprintf("%s-alert-triggered", alert.AlertId),
		"event_type":     "alert.triggered",
		"aggregate_id":   alert.AlertId,
		"tenant_id":      tenantID,
		"correlation_id": trace.RequestID,
		"trace_id":       trace.TraceID,
		"schema_version": "v1",
		"occurred_at":    time.UnixMilli(alert.Timestamp).UTC().Format(time.RFC3339Nano),
		"headers": map[string]string{
			"source":  "alerting-service",
			"service": "alerting-service",
		},
		"data": alert,
	})
	if err != nil {
		log.Printf("alert kafka payload marshal failed: %v", err)
		return
	}

	kafkaCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	topic := s.topicPrefix + ".alerts.triggered.v1"
	if err := s.kafkaWriter.WriteMessages(kafkaCtx, kafka.Message{
		Topic: topic,
		Key:   []byte(strings.Join([]string{tenantID, alert.AccountId, alert.AlertId}, "|")),
		Value: payload,
		Time:  time.UnixMilli(alert.Timestamp),
	}); err != nil {
		log.Printf("alert kafka publish failed: %v", err)
	}
}

func (s *alertingService) close() {
	if s != nil && s.kafkaWriter != nil {
		_ = s.kafkaWriter.Close()
		s.kafkaWriter = nil
	}
}

func kafkaBrokersFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			brokers = append(brokers, value)
		}
	}
	return brokers
}

func topicPrefixFromEnv() string {
	if value := strings.TrimSpace(os.Getenv("KAFKA_TOPIC_PREFIX")); value != "" {
		return strings.TrimSuffix(value, ".")
	}
	return "rms"
}

func main() {
	svc := newAlertingService()
	defer svc.close()

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "alerting-service"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterAlertingServiceServer(s, svc)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50052")
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
