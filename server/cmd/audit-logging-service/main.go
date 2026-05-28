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

type auditLoggingService struct {
	pb.UnimplementedAuditLoggingServiceServer
	mu          sync.RWMutex
	events      []*pb.AuditEvent
	subscribers map[int]chan *pb.AuditEvent
	nextSubID   int
	kafkaWriter *kafka.Writer
	topicPrefix string
}

func newAuditLoggingService() *auditLoggingService {
	svc := &auditLoggingService{
		events:      []*pb.AuditEvent{},
		subscribers: make(map[int]chan *pb.AuditEvent),
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

func (s *auditLoggingService) LogEvent(ctx context.Context, req *pb.AuditEvent) (*pb.AuditResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("audit event is required")
	}

	event := cloneAuditEvent(req)
	if event.EventId == "" {
		event.EventId = fmt.Sprintf("audit-%d", time.Now().UnixNano())
	}
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixMilli()
	}
	if event.IngestTimestamp == 0 {
		event.IngestTimestamp = time.Now().UnixMilli()
	}

	s.mu.Lock()
	s.events = append(s.events, event)
	s.broadcastLocked(event)
	s.mu.Unlock()
	s.publishKafka(ctx, event)

	log.Printf("audit event stored event_id=%s event_type=%s account_id=%s service=%s", event.EventId, event.EventType, event.AccountId, event.ServiceName)
	return &pb.AuditResponse{
		EventId:   event.EventId,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *auditLoggingService) GetEvents(ctx context.Context, req *pb.AuditQueryRequest) (*pb.AuditQueryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("audit query request is required")
	}

	matches := s.filter(req)
	return &pb.AuditQueryResponse{
		Events:     matches,
		Timestamp:  time.Now().UnixMilli(),
		TotalCount: s.countMatches(req),
	}, nil
}

func (s *auditLoggingService) StreamEvents(req *pb.AuditQueryRequest, stream pb.AuditLoggingService_StreamEventsServer) error {
	if req == nil {
		return fmt.Errorf("audit stream query is required")
	}

	subID, ch := s.subscribe()
	defer s.unsubscribe(subID)

	backlog := s.filter(req)
	for _, event := range backlog {
		if err := stream.Send(event); err != nil {
			return err
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event := <-ch:
			if event == nil {
				continue
			}
			if !matchesQuery(event, req) {
				continue
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

func (s *auditLoggingService) filter(req *pb.AuditQueryRequest) []*pb.AuditEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}

	results := make([]*pb.AuditEvent, 0, len(s.events))
	for i := len(s.events) - 1; i >= 0; i-- {
		event := s.events[i]
		if !matchesQuery(event, req) {
			continue
		}
		results = append(results, cloneAuditEvent(event))
		if int32(len(results)) >= limit {
			break
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Timestamp < results[j].Timestamp })
	return results
}

func (s *auditLoggingService) countMatches(req *pb.AuditQueryRequest) int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int32
	for _, event := range s.events {
		if matchesQuery(event, req) {
			count++
		}
	}
	return count
}

func matchesQuery(event *pb.AuditEvent, req *pb.AuditQueryRequest) bool {
	if event == nil || req == nil {
		return false
	}
	if req.AccountId != "" && req.AccountId != event.AccountId {
		return false
	}
	if req.EventType != "" && req.EventType != event.EventType {
		return false
	}
	if req.ServiceName != "" && req.ServiceName != event.ServiceName {
		return false
	}
	if req.StartTime > 0 && event.Timestamp < req.StartTime {
		return false
	}
	if req.EndTime > 0 && event.Timestamp > req.EndTime {
		return false
	}
	return true
}

func (s *auditLoggingService) subscribe() (int, chan *pb.AuditEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSubID++
	id := s.nextSubID
	ch := make(chan *pb.AuditEvent, 64)
	s.subscribers[id] = ch
	return id, ch
}

func (s *auditLoggingService) unsubscribe(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

func (s *auditLoggingService) broadcastLocked(event *pb.AuditEvent) {
	for _, ch := range s.subscribers {
		select {
		case ch <- cloneAuditEvent(event):
		default:
		}
	}
}

func cloneAuditEvent(event *pb.AuditEvent) *pb.AuditEvent {
	if event == nil {
		return nil
	}
	copy := *event
	if event.Metadata != nil {
		copy.Metadata = make(map[string]string, len(event.Metadata))
		for k, v := range event.Metadata {
			copy.Metadata[k] = v
		}
	}
	return &copy
}

func (s *auditLoggingService) publishKafka(ctx context.Context, event *pb.AuditEvent) {
	if s == nil || s.kafkaWriter == nil || event == nil {
		return
	}

	trace := observability.TraceFromContext(ctx)
	tenantID := "default"
	if event.Metadata != nil {
		if value := strings.TrimSpace(event.Metadata["tenant_id"]); value != "" {
			tenantID = value
		}
	}
	payload, err := json.Marshal(map[string]any{
		"event_id":       event.EventId,
		"event_type":     "audit.logged",
		"aggregate_id":   event.EventId,
		"tenant_id":      tenantID,
		"correlation_id": trace.RequestID,
		"trace_id":       trace.TraceID,
		"schema_version": "v1",
		"occurred_at":    time.UnixMilli(event.Timestamp).UTC().Format(time.RFC3339Nano),
		"headers": map[string]string{
			"source":  "audit-logging-service",
			"service": "audit-logging-service",
		},
		"data": event,
	})
	if err != nil {
		log.Printf("audit kafka payload marshal failed: %v", err)
		return
	}

	kafkaCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	topic := s.topicPrefix + ".audit.logged.v1"
	if err := s.kafkaWriter.WriteMessages(kafkaCtx, kafka.Message{
		Topic: topic,
		Key:   []byte(strings.Join([]string{tenantID, event.EventId}, "|")),
		Value: payload,
		Time:  time.UnixMilli(event.Timestamp),
	}); err != nil {
		log.Printf("audit kafka publish failed: %v", err)
	}
}

func (s *auditLoggingService) close() {
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
	svc := newAuditLoggingService()
	defer svc.close()

	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "audit-logging-service"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterAuditLoggingServiceServer(s, svc)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50053")
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
