package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
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

type marketDataConsumerService struct {
	pb.UnimplementedMarketDataConsumerServiceServer
	mu          sync.RWMutex
	latest      map[string]*pb.MarketDataUpdate
	history     map[string][]*pb.MarketDataUpdate
	subscribers map[int]chan *pb.MarketDataUpdate
	nextSubID   int
	kafkaWriter *kafka.Writer
	topicPrefix string
}

func newMarketDataConsumerService() *marketDataConsumerService {
	svc := &marketDataConsumerService{
		latest:      make(map[string]*pb.MarketDataUpdate),
		history:     make(map[string][]*pb.MarketDataUpdate),
		subscribers: make(map[int]chan *pb.MarketDataUpdate),
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

func (s *marketDataConsumerService) SubscribeToMarketData(req *pb.MarketDataSubscriptionRequest, stream pb.MarketDataConsumerService_SubscribeToMarketDataServer) error {
	if req == nil {
		return fmt.Errorf("market data subscription request is required")
	}
	if len(req.Symbols) == 0 {
		req.Symbols = []string{"AAPL"}
	}

	subID, ch := s.subscribe()
	defer s.unsubscribe(subID)

	// Prime the stream with current snapshots.
	if resp, err := s.GetLatestMarketData(stream.Context(), &pb.MarketDataRequest{Symbols: req.Symbols}); err == nil {
		for _, update := range resp.MarketData {
			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			for _, symbol := range req.Symbols {
				update := s.generateUpdate(symbol)
				s.publish(stream.Context(), update)
			}
		case update := <-ch:
			if update == nil {
				continue
			}
			if !matchesSymbol(update.Symbol, req.Symbols) {
				continue
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}
}

func (s *marketDataConsumerService) GetLatestMarketData(ctx context.Context, req *pb.MarketDataRequest) (*pb.MarketDataResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("market data request is required")
	}

	updates := make([]*pb.MarketDataUpdate, 0, len(req.Symbols))
	for _, symbol := range req.Symbols {
		updates = append(updates, s.latestOrGenerate(ctx, symbol))
	}
	if len(updates) == 0 {
		updates = append(updates, s.latestOrGenerate(ctx, "AAPL"))
	}

	return &pb.MarketDataResponse{
		MarketData: updates,
		Timestamp:  time.Now().UnixMilli(),
	}, nil
}

func (s *marketDataConsumerService) GetMarketDataHistory(ctx context.Context, req *pb.MarketDataHistoryRequest) (*pb.MarketDataHistoryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("market data history request is required")
	}
	if strings.TrimSpace(req.Symbol) == "" {
		req.Symbol = "AAPL"
	}

	s.mu.RLock()
	history := append([]*pb.MarketDataUpdate(nil), s.history[strings.ToUpper(req.Symbol)]...)
	s.mu.RUnlock()

	results := make([]*pb.MarketDataUpdate, 0, len(history))
	for _, update := range history {
		if req.StartTime > 0 && update.Timestamp < req.StartTime {
			continue
		}
		if req.EndTime > 0 && update.Timestamp > req.EndTime {
			continue
		}
		results = append(results, cloneUpdate(update))
		if req.Limit > 0 && int32(len(results)) >= req.Limit {
			break
		}
	}

	if len(results) == 0 {
		for i := 0; i < 5; i++ {
			update := s.generateUpdate(req.Symbol)
			s.publish(ctx, update)
			results = append(results, cloneUpdate(update))
		}
	}

	return &pb.MarketDataHistoryResponse{
		MarketData: results,
		Timestamp:  time.Now().UnixMilli(),
	}, nil
}

func (s *marketDataConsumerService) latestOrGenerate(ctx context.Context, symbol string) *pb.MarketDataUpdate {
	s.mu.RLock()
	update, ok := s.latest[strings.ToUpper(symbol)]
	s.mu.RUnlock()
	if ok {
		return cloneUpdate(update)
	}
	update = s.generateUpdate(symbol)
	s.publish(ctx, update)
	return cloneUpdate(update)
}

func (s *marketDataConsumerService) generateUpdate(symbol string) *pb.MarketDataUpdate {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		symbol = "AAPL"
	}

	base := 100 + float64(len(symbol))*8 + rand.Float64()*6
	spread := 0.05 + rand.Float64()*0.2
	return &pb.MarketDataUpdate{
		Symbol:    symbol,
		BidPrice:  base - spread/2,
		AskPrice:  base + spread/2,
		LastPrice: base,
		Volume:    int64(500_000 + rand.Int63n(2_000_000)),
		Timestamp: time.Now().UnixMilli(),
	}
}

func (s *marketDataConsumerService) publish(ctx context.Context, update *pb.MarketDataUpdate) {
	if update == nil {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	symbol := strings.ToUpper(update.Symbol)
	copy := cloneUpdate(update)
	s.latest[symbol] = copy
	s.history[symbol] = append(trimHistory(s.history[symbol], 1000), copy)
	for _, ch := range s.subscribers {
		select {
		case ch <- cloneUpdate(copy):
		default:
		}
	}
	s.mu.Unlock()

	s.publishKafka(ctx, copy)
}

func (s *marketDataConsumerService) subscribe() (int, chan *pb.MarketDataUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSubID++
	id := s.nextSubID
	ch := make(chan *pb.MarketDataUpdate, 128)
	s.subscribers[id] = ch
	return id, ch
}

func (s *marketDataConsumerService) unsubscribe(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

func (s *marketDataConsumerService) publishKafka(ctx context.Context, update *pb.MarketDataUpdate) {
	if s == nil || s.kafkaWriter == nil || update == nil {
		return
	}

	trace := observability.TraceFromContext(ctx)
	payload, err := json.Marshal(map[string]any{
		"event_id":       fmt.Sprintf("%s-%d", update.Symbol, update.Timestamp),
		"event_type":     "marketdata.tick",
		"aggregate_id":   update.Symbol,
		"tenant_id":      "global",
		"correlation_id": trace.RequestID,
		"trace_id":       trace.TraceID,
		"schema_version": "v1",
		"occurred_at":    time.UnixMilli(update.Timestamp).UTC().Format(time.RFC3339Nano),
		"headers": map[string]string{
			"source":  "market-data-consumer",
			"service": "market-data-consumer",
		},
		"data": update,
	})
	if err != nil {
		log.Printf("market data kafka payload marshal failed: %v", err)
		return
	}

	kafkaCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	topic := s.topicPrefix + ".marketdata.tick.v1"
	if err := s.kafkaWriter.WriteMessages(kafkaCtx, kafka.Message{
		Topic: topic,
		Key:   []byte(strings.ToUpper(update.Symbol)),
		Value: payload,
		Time:  time.UnixMilli(update.Timestamp),
	}); err != nil {
		log.Printf("market data kafka publish failed: %v", err)
	}
}

func (s *marketDataConsumerService) close() {
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

func matchesSymbol(symbol string, requested []string) bool {
	if len(requested) == 0 {
		return true
	}
	for _, candidate := range requested {
		if strings.EqualFold(candidate, symbol) {
			return true
		}
	}
	return false
}

func trimHistory(history []*pb.MarketDataUpdate, max int) []*pb.MarketDataUpdate {
	if len(history) <= max {
		return history
	}
	return append([]*pb.MarketDataUpdate(nil), history[len(history)-max:]...)
}

func cloneUpdate(update *pb.MarketDataUpdate) *pb.MarketDataUpdate {
	if update == nil {
		return nil
	}
	copy := *update
	return &copy
}

func main() {
	svc := newMarketDataConsumerService()
	defer svc.close()

	lis, err := net.Listen("tcp", ":50054")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "market-data-consumer"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterMarketDataConsumerServiceServer(s, svc)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50054")
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
