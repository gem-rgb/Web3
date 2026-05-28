package main

import (
	"context"
	"fmt"
	"log"
	"math"
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

	"github.com/example/rms/shared/platform/grpcx"
	pb "github.com/example/rms/shared/proto"
)

type exposureAggregationService struct {
	pb.UnimplementedExposureAggregationServiceServer
	mu        sync.RWMutex
	exposures map[string]map[string]*pb.AggregatedExposure
}

func newExposureAggregationService() *exposureAggregationService {
	return &exposureAggregationService{
		exposures: make(map[string]map[string]*pb.AggregatedExposure),
	}
}

func (s *exposureAggregationService) UpdatePosition(ctx context.Context, req *pb.PositionUpdate) (*pb.AggregatedExposure, error) {
	if req == nil || req.Position == nil {
		return nil, fmt.Errorf("position update is required")
	}

	pos := req.Position
	exposure := exposureFromPosition(pos)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.exposures[pos.AccountId]; !ok {
		s.exposures[pos.AccountId] = make(map[string]*pb.AggregatedExposure)
	}
	s.exposures[pos.AccountId][strings.ToUpper(pos.Symbol)] = exposure

	log.Printf("exposure updated account_id=%s symbol=%s net_quantity=%d market_value=%.2f", pos.AccountId, pos.Symbol, exposure.NetQuantity, exposure.MarketValue)
	return cloneExposure(exposure), nil
}

func (s *exposureAggregationService) GetAccountExposure(ctx context.Context, req *pb.AccountExposureRequest) (*pb.AccountExposureResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account exposure request is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	accountMap := s.exposures[req.AccountId]
	exposures := make([]*pb.AggregatedExposure, 0, len(accountMap))
	for _, exposure := range accountMap {
		exposures = append(exposures, cloneExposure(exposure))
	}
	return &pb.AccountExposureResponse{
		Exposures: exposures,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *exposureAggregationService) GetSymbolExposure(ctx context.Context, req *pb.SymbolExposureRequest) (*pb.SymbolExposureResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("symbol exposure request is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	exposures := []*pb.AggregatedExposure{}
	for _, accountMap := range s.exposures {
		if exposure, ok := accountMap[strings.ToUpper(req.Symbol)]; ok {
			exposures = append(exposures, cloneExposure(exposure))
		}
	}
	return &pb.SymbolExposureResponse{
		Exposures: exposures,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *exposureAggregationService) StreamAccountExposure(req *pb.AccountExposureRequest, stream pb.ExposureAggregationService_StreamAccountExposureServer) error {
	if req == nil {
		return fmt.Errorf("account exposure request is required")
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			resp, err := s.GetAccountExposure(stream.Context(), req)
			if err != nil {
				return err
			}
			for _, exposure := range resp.Exposures {
				if err := stream.Send(exposure); err != nil {
					return err
				}
			}
		}
	}
}

func exposureFromPosition(pos *pb.Position) *pb.AggregatedExposure {
	notional := math.Abs(float64(pos.Quantity) * pos.AveragePrice)
	return &pb.AggregatedExposure{
		AccountId:    pos.AccountId,
		Symbol:       pos.Symbol,
		NetQuantity:  pos.Quantity,
		GrossExposure: notional,
		NetExposure:  float64(pos.Quantity) * pos.AveragePrice,
		MarketValue:  pos.MarketValue,
		Timestamp:    time.Now().UnixMilli(),
	}
}

func cloneExposure(exposure *pb.AggregatedExposure) *pb.AggregatedExposure {
	if exposure == nil {
		return nil
	}
	copy := *exposure
	return &copy
}

func main() {
	lis, err := net.Listen("tcp", ":50055")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "exposure-aggregation-service"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterExposureAggregationServiceServer(s, newExposureAggregationService())
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	reflection.Register(s)

	go func() {
		log.Println("Starting gRPC server on :50055")
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
