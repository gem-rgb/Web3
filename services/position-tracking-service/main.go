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

type positionTrackingService struct {
	pb.UnimplementedPositionTrackingServiceServer
	mu          sync.RWMutex
	positions   map[string]map[string]*pb.Position
	subscribers map[int]chan *pb.PositionUpdateResponse
	nextSubID   int
	kafkaWriter *kafka.Writer
	topicPrefix string
}

func newPositionTrackingService() *positionTrackingService {
	svc := &positionTrackingService{
		positions:   make(map[string]map[string]*pb.Position),
		subscribers: make(map[int]chan *pb.PositionUpdateResponse),
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

func (s *positionTrackingService) UpdatePosition(ctx context.Context, req *pb.PositionUpdateRequest) (*pb.PositionUpdateResponse, error) {
	if req == nil || req.Position == nil {
		return nil, fmt.Errorf("position update is required")
	}

	updated := s.applyUpdate(req.Position, req.IsNew)

	s.mu.Lock()
	if _, ok := s.positions[updated.AccountId]; !ok {
		s.positions[updated.AccountId] = make(map[string]*pb.Position)
	}
	s.positions[updated.AccountId][strings.ToUpper(updated.Symbol)] = clonePosition(updated)
	s.broadcastLocked(&pb.PositionUpdateResponse{Position: clonePosition(updated), Timestamp: time.Now().UnixMilli()})
	s.mu.Unlock()
	s.publishKafka(ctx, updated, req.IsNew)

	log.Printf("position updated account_id=%s symbol=%s quantity=%d avg_price=%.2f", updated.AccountId, updated.Symbol, updated.Quantity, updated.AveragePrice)
	return &pb.PositionUpdateResponse{Position: clonePosition(updated), Timestamp: time.Now().UnixMilli()}, nil
}

func (s *positionTrackingService) GetPosition(ctx context.Context, req *pb.PositionRequest) (*pb.PositionResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("position request is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if account, ok := s.positions[req.AccountId]; ok {
		if pos, ok := account[strings.ToUpper(req.Symbol)]; ok {
			return &pb.PositionResponse{Position: clonePosition(pos), Timestamp: time.Now().UnixMilli()}, nil
		}
	}
	return &pb.PositionResponse{Position: &pb.Position{AccountId: req.AccountId, Symbol: req.Symbol, Timestamp: time.Now().UnixMilli()}, Timestamp: time.Now().UnixMilli()}, nil
}

func (s *positionTrackingService) GetPositionsForAccount(ctx context.Context, req *pb.AccountPositionRequest) (*pb.AccountPositionResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account position request is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	positions := []*pb.Position{}
	if account, ok := s.positions[req.AccountId]; ok {
		for _, pos := range account {
			positions = append(positions, clonePosition(pos))
		}
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i].Symbol < positions[j].Symbol })
	return &pb.AccountPositionResponse{Positions: positions, Timestamp: time.Now().UnixMilli()}, nil
}

func (s *positionTrackingService) StreamPositionUpdates(req *pb.PositionRequest, stream pb.PositionTrackingService_StreamPositionUpdatesServer) error {
	if req == nil {
		return fmt.Errorf("position stream request is required")
	}

	subID, ch := s.subscribe()
	defer s.unsubscribe(subID)

	// Prime the stream with the current state for the requested position.
	if resp, err := s.GetPosition(stream.Context(), req); err == nil && resp.Position != nil {
		if err := stream.Send(&pb.PositionUpdateResponse{Position: resp.Position, Timestamp: resp.Timestamp}); err != nil {
			return err
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case update := <-ch:
			if update == nil || update.Position == nil {
				continue
			}
			if !strings.EqualFold(req.AccountId, update.Position.AccountId) {
				continue
			}
			if req.Symbol != "" && !strings.EqualFold(req.Symbol, update.Position.Symbol) {
				continue
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}
}

func (s *positionTrackingService) applyUpdate(incoming *pb.Position, isNew bool) *pb.Position {
	s.mu.RLock()
	current := s.lookupLocked(incoming.AccountId, incoming.Symbol)
	s.mu.RUnlock()

	if current == nil || isNew {
		copy := clonePosition(incoming)
		if copy.MarketValue == 0 {
			copy.MarketValue = float64(copy.Quantity) * copy.AveragePrice
		}
		if copy.CostBasis == 0 {
			copy.CostBasis = copy.MarketValue
		}
		copy.Side = positionSide(copy.Quantity)
		copy.Timestamp = time.Now().UnixMilli()
		return copy
	}

	updated := clonePosition(current)
	previousQty := updated.Quantity
	newQty := previousQty + incoming.Quantity
	if newQty == 0 {
		updated.RealizedPL += float64(incoming.Quantity) * (incoming.AveragePrice - updated.AveragePrice)
		updated.Quantity = 0
		updated.AveragePrice = incoming.AveragePrice
	} else {
		updated.AveragePrice = weightedAverage(previousQty, updated.AveragePrice, incoming.Quantity, incoming.AveragePrice)
		updated.Quantity = newQty
	}
	updated.MarketValue = float64(updated.Quantity) * incoming.AveragePrice
	updated.CostBasis = float64(updated.Quantity) * updated.AveragePrice
	updated.UnrealizedPL = updated.MarketValue - updated.CostBasis
	updated.Side = positionSide(updated.Quantity)
	updated.Timestamp = time.Now().UnixMilli()
	return updated
}

func (s *positionTrackingService) lookupLocked(accountID, symbol string) *pb.Position {
	account, ok := s.positions[accountID]
	if !ok {
		return nil
	}
	pos, ok := account[strings.ToUpper(symbol)]
	if !ok {
		return nil
	}
	return clonePosition(pos)
}

func (s *positionTrackingService) subscribe() (int, chan *pb.PositionUpdateResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSubID++
	id := s.nextSubID
	ch := make(chan *pb.PositionUpdateResponse, 64)
	s.subscribers[id] = ch
	return id, ch
}

func (s *positionTrackingService) unsubscribe(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

func (s *positionTrackingService) broadcastLocked(update *pb.PositionUpdateResponse) {
	for _, ch := range s.subscribers {
		select {
		case ch <- cloneUpdateResponse(update):
		default:
		}
	}
}

func clonePosition(pos *pb.Position) *pb.Position {
	if pos == nil {
		return nil
	}
	copy := *pos
	return &copy
}

func cloneUpdateResponse(resp *pb.PositionUpdateResponse) *pb.PositionUpdateResponse {
	if resp == nil {
		return nil
	}
	copy := *resp
	copy.Position = clonePosition(resp.Position)
	return &copy
}

func (s *positionTrackingService) publishKafka(ctx context.Context, position *pb.Position, isNew bool) {
	if s == nil || s.kafkaWriter == nil || position == nil {
		return
	}

	trace := observability.TraceFromContext(ctx)
	payload, err := json.Marshal(map[string]any{
		"event_id":       fmt.Sprintf("%s-%s-position-updated", position.AccountId, strings.ToUpper(position.Symbol)),
		"event_type":     "position.updated",
		"aggregate_id":   fmt.Sprintf("%s:%s", position.AccountId, strings.ToUpper(position.Symbol)),
		"tenant_id":      "default",
		"correlation_id": trace.RequestID,
		"trace_id":       trace.TraceID,
		"schema_version": "v1",
		"occurred_at":    time.UnixMilli(position.Timestamp).UTC().Format(time.RFC3339Nano),
		"headers": map[string]string{
			"source":  "position-tracking-service",
			"service": "position-tracking-service",
		},
		"data": map[string]any{
			"position": position,
			"is_new":   isNew,
		},
	})
	if err != nil {
		log.Printf("position kafka payload marshal failed: %v", err)
		return
	}

	kafkaCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	topic := s.topicPrefix + ".positions.updated.v1"
	if err := s.kafkaWriter.WriteMessages(kafkaCtx, kafka.Message{
		Topic: topic,
		Key:   []byte(strings.Join([]string{"default", position.AccountId, strings.ToUpper(position.Symbol)}, "|")),
		Value: payload,
		Time:  time.UnixMilli(position.Timestamp),
	}); err != nil {
		log.Printf("position kafka publish failed: %v", err)
	}
}

func (s *positionTrackingService) close() {
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

func weightedAverage(currentQty int32, currentAvg float64, deltaQty int32, deltaAvg float64) float64 {
	totalQty := float64(currentQty + deltaQty)
	if totalQty == 0 {
		return deltaAvg
	}
	return ((float64(currentQty) * currentAvg) + (float64(deltaQty) * deltaAvg)) / totalQty
}

func positionSide(quantity int32) string {
	switch {
	case quantity > 0:
		return "LONG"
	case quantity < 0:
		return "SHORT"
	default:
		return "FLAT"
	}
}

func main() {
	svc := newPositionTrackingService()
	defer svc.close()

	lis, err := net.Listen("tcp", ":50056")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "position-tracking-service"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterPositionTrackingServiceServer(s, svc)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50056")
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
