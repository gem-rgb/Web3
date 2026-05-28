package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/example/rms/shared/platform/grpcx"
	pb "github.com/example/rms/shared/proto"
)

type marginCalculationEngine struct {
	pb.UnimplementedMarginCalculationEngineServer
	mu      sync.RWMutex
	cache   map[string]*pb.MarginRequirement
}

func newMarginCalculationEngine() *marginCalculationEngine {
	return &marginCalculationEngine{
		cache: make(map[string]*pb.MarginRequirement),
	}
}

func (s *marginCalculationEngine) CalculateMargin(ctx context.Context, req *pb.MarginCalculationRequest) (*pb.MarginCalculationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("margin calculation request is required")
	}

	margin := s.calculate(req.AccountId, req.ForceRecalc)
	return &pb.MarginCalculationResponse{
		MarginRequirement: cloneMargin(margin),
		Timestamp:         time.Now().UnixMilli(),
	}, nil
}

func (s *marginCalculationEngine) GetAccountMargin(ctx context.Context, req *pb.AccountMarginRequest) (*pb.AccountMarginResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account margin request is required")
	}

	margin := s.calculate(req.AccountId, false)
	return &pb.AccountMarginResponse{
		MarginRequirement: cloneMargin(margin),
		Timestamp:         time.Now().UnixMilli(),
	}, nil
}

func (s *marginCalculationEngine) StreamMarginUpdates(req *pb.AccountMarginRequest, stream pb.MarginCalculationEngine_StreamMarginUpdatesServer) error {
	if req == nil {
		return fmt.Errorf("account margin request is required")
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			resp, err := s.CalculateMargin(stream.Context(), &pb.MarginCalculationRequest{AccountId: req.AccountId, ForceRecalc: true})
			if err != nil {
				return err
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (s *marginCalculationEngine) calculate(accountID string, force bool) *pb.MarginRequirement {
	s.mu.RLock()
	cached, ok := s.cache[accountID]
	s.mu.RUnlock()
	if ok && !force {
		return cloneMargin(cached)
	}

	account := syntheticAccount(accountID)
	positions := syntheticPositions(accountID)
	notional := 0.0
	for _, position := range positions {
		notional += math.Abs(float64(position.Quantity) * position.AveragePrice)
	}

	initial := notional * 0.5
	maintenance := notional * 0.3
	equity := account.CashBalance + account.MarketValue
	margin := &pb.MarginRequirement{
		AccountId:         accountID,
		InitialMargin:     initial,
		MaintenanceMargin: maintenance,
		MarginExcess:      equity - maintenance,
		MarginRatio:       safeDiv(equity, maintenance),
		Timestamp:         time.Now().UnixMilli(),
	}

	s.mu.Lock()
	s.cache[accountID] = cloneMargin(margin)
	s.mu.Unlock()
	return margin
}

func syntheticAccount(accountID string) *pb.Account {
	if accountID == "" {
		accountID = "synthetic-account"
	}
	base := 400_000.0 + float64(len(accountID))*18_000.0
	return &pb.Account{
		AccountId:               accountID,
		UserId:                  "margin-user",
		Status:                  "ACTIVE",
		BuyingPower:             base * 2,
		CashBalance:             base,
		MarketValue:             base * 0.9,
		DayTradingBuyingPower:   base * 3,
		MaintenanceMarginExcess: base * 0.25,
	}
}

func syntheticPositions(accountID string) []*pb.Position {
	base := float64(len(accountID)%5 + 1)
	now := time.Now().UnixMilli()
	return []*pb.Position{
		{AccountId: accountID, Symbol: "AAPL", Quantity: int32(100 * base), AveragePrice: 190.25, MarketValue: 19025 * base, CostBasis: 19025 * base, Side: "LONG", Timestamp: now},
		{AccountId: accountID, Symbol: "MSFT", Quantity: int32(60 * base), AveragePrice: 408.50, MarketValue: 24510 * base, CostBasis: 24510 * base, Side: "LONG", Timestamp: now},
		{AccountId: accountID, Symbol: "NVDA", Quantity: int32(-20 * base), AveragePrice: 950.00, MarketValue: -19000 * base, CostBasis: 19000 * base, Side: "SHORT", Timestamp: now},
	}
}

func cloneMargin(margin *pb.MarginRequirement) *pb.MarginRequirement {
	if margin == nil {
		return nil
	}
	copy := *margin
	return &copy
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func main() {
	lis, err := net.Listen("tcp", ":50057")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "margin-calculation-engine"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterMarginCalculationEngineServer(s, newMarginCalculationEngine())
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50057")
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
