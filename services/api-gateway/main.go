package main

import (
	"encoding/json"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/example/rms/shared/platform/grpcx"
	"github.com/example/rms/shared/platform/security"
	pb "github.com/example/rms/shared/proto"
)

type apiGatewayService struct {
	pb.UnimplementedAPIGatewayServiceServer
	orderIngestionAddr string
	marketDataAddr string
	positionAddr   string
	exposureAddr   string
	marginAddr     string
	rateLimit      int
	window         time.Duration
	mu             sync.Mutex
	requests       map[string][]time.Time
	connMu         sync.Mutex
	orderConn      *grpc.ClientConn
	marketConn     *grpc.ClientConn
	positionConn   *grpc.ClientConn
	exposureConn   *grpc.ClientConn
	marginConn     *grpc.ClientConn
	requestCount   atomic.Int64
	throttledCount atomic.Int64
	rejectedCount  atomic.Int64
	approvedCount  atomic.Int64
	authVerifier   *security.JWTVerifier
}

type authClaimsContextKey struct{}

func newAPIGatewayService() *apiGatewayService {
	rand.Seed(time.Now().UnixNano())
	return &apiGatewayService{
		orderIngestionAddr: envString("RMS_ORDER_INGESTION_ADDR", "127.0.0.1:50062"),
		marketDataAddr: envString("RMS_MARKET_DATA_ADDR", "127.0.0.1:50054"),
		positionAddr:   envString("RMS_POSITION_TRACKING_ADDR", "127.0.0.1:50056"),
		exposureAddr:   envString("RMS_EXPOSURE_ADDR", "127.0.0.1:50055"),
		marginAddr:     envString("RMS_MARGIN_ADDR", "127.0.0.1:50057"),
		rateLimit:      envInt("RMS_GATEWAY_RATE_LIMIT", 200),
		window:         envDuration("RMS_GATEWAY_RATE_WINDOW", time.Minute),
		requests:       make(map[string][]time.Time),
		authVerifier:   security.NewJWTVerifier([]byte(envString("RMS_AUTH_SECRET", "dev-auth-secret")), envString("RMS_AUTH_ISSUER", "rms-auth"), envString("RMS_AUTH_AUDIENCE", "rms-platform")),
	}
}

func (s *apiGatewayService) SubmitOrder(ctx context.Context, req *pb.OrderRequest) (*pb.OrderResponse, error) {
	if req == nil || req.Order == nil {
		return nil, fmt.Errorf("order request is required")
	}
	if err := s.authorizeContext(ctx); err != nil {
		return nil, err
	}

	s.requestCount.Add(1)
	if !s.allow(req.Order.AccountId) {
		s.throttledCount.Add(1)
		return &pb.OrderResponse{
			OrderId:      req.Order.OrderId,
			Approved:     false,
			RejectReason: "gateway throttling limit exceeded",
			Violations: []*pb.Violation{{
				RuleId:          "gateway-throttle",
				RuleDescription: "Gateway-level rate limit exceeded",
				Severity:        "HIGH",
			}},
			Timestamp: time.Now().UnixMilli(),
		}, nil
	}

	resp, err := s.forwardOrder(ctx, req)
	if err != nil {
		log.Printf("order ingestion service unavailable, failing closed: %v", err)
		resp = gatewayFallbackRiskDecision(req)
	}
	if resp.Approved {
		s.approvedCount.Add(1)
	} else {
		s.rejectedCount.Add(1)
	}

	return resp, nil
}

func (s *apiGatewayService) CancelOrder(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("cancel request is required")
	}
	if err := s.authorizeContext(ctx); err != nil {
		return nil, err
	}
	log.Printf("cancel request received account_id=%s order_id=%s", req.AccountId, req.OrderId)
	return &pb.CancelOrderResponse{
		Success:   true,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func (s *apiGatewayService) GetAccountInfo(ctx context.Context, req *pb.AccountInfoRequest) (*pb.AccountInfoResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account info request is required")
	}
	if err := s.authorizeContext(ctx); err != nil {
		return nil, err
	}

	account := syntheticAccount(req.AccountId)
	positions := s.fetchPositions(ctx, req.AccountId)
	if len(positions) == 0 {
		positions = syntheticPositions(req.AccountId)
	}
	exposures := s.fetchExposures(ctx, req.AccountId)
	if len(exposures) == 0 {
		exposures = aggregateExposures(req.AccountId, positions)
	}
	margin := computeMargin(account, positions)
	if fetched := s.fetchMargin(ctx, req.AccountId); fetched != nil {
		margin = fetched
	}

	return &pb.AccountInfoResponse{
		Account:           account,
		Positions:         positions,
		MarginRequirement: margin,
		Exposures:         exposures,
		Timestamp:         time.Now().UnixMilli(),
	}, nil
}

func (s *apiGatewayService) GetMarketData(ctx context.Context, req *pb.MarketDataRequest) (*pb.MarketDataResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("market data request is required")
	}
	if err := s.authorizeContext(ctx); err != nil {
		return nil, err
	}

	resp, err := s.forwardMarketData(ctx, req)
	if err != nil {
		log.Printf("market data service unavailable, using gateway fallback: %v", err)
		return syntheticMarketDataResponse(req.Symbols), nil
	}
	return resp, nil
}

func (s *apiGatewayService) StreamMarketData(req *pb.MarketDataSubscriptionRequest, stream pb.APIGatewayService_StreamMarketDataServer) error {
	if req == nil {
		return fmt.Errorf("market data subscription request is required")
	}
	if err := s.authorizeContext(stream.Context()); err != nil {
		return err
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			resp, err := s.GetMarketData(stream.Context(), &pb.MarketDataRequest{Symbols: req.Symbols})
			if err != nil {
				return err
			}
			for _, update := range resp.MarketData {
				if err := stream.Send(update); err != nil {
					return err
				}
			}
		}
	}
}

func (s *apiGatewayService) allow(accountID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-s.window)
	requests := s.requests[accountID]
	kept := requests[:0]
	for _, ts := range requests {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	if len(kept) >= s.rateLimit {
		s.requests[accountID] = kept
		return false
	}
	kept = append(kept, now)
	s.requests[accountID] = kept
	return true
}

func (s *apiGatewayService) forwardOrder(ctx context.Context, req *pb.OrderRequest) (*pb.OrderResponse, error) {
	conn, err := s.orderClient(ctx)
	if err != nil {
		return nil, err
	}

	resp := &pb.OrderIngestionResponse{}
	if err := conn.Invoke(ctx, "/rms.orders.v1.OrderIngestionService/SubmitOrder", &pb.OrderIngestionRequest{
		Order:   req.Order,
		Account: req.Account,
	}, resp); err != nil {
		return nil, err
	}
	if resp.Decision == nil {
		return &pb.OrderResponse{
			OrderId:   req.Order.OrderId,
			Approved:  resp.Accepted,
			Timestamp: resp.Timestamp,
		}, nil
	}
	return &pb.OrderResponse{
		OrderId:      resp.Decision.OrderID,
		Approved:     resp.Decision.Approved,
		RejectReason: resp.Decision.RejectReason,
		Violations:   resp.Decision.Violations,
		Timestamp:    resp.Timestamp,
	}, nil
}

func (s *apiGatewayService) forwardMarketData(ctx context.Context, req *pb.MarketDataRequest) (*pb.MarketDataResponse, error) {
	conn, err := s.marketClient(ctx)
	if err != nil {
		return nil, err
	}

	resp := &pb.MarketDataResponse{}
	if err := conn.Invoke(ctx, "/rms.marketdata.MarketDataConsumerService/GetLatestMarketData", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *apiGatewayService) orderClient(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.orderConn != nil {
		return s.orderConn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		s.orderIngestionAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	s.orderConn = conn
	return conn, nil
}

func (s *apiGatewayService) marketClient(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.marketConn != nil {
		return s.marketConn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		s.marketDataAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	s.marketConn = conn
	return conn, nil
}

func (s *apiGatewayService) positionClient(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.positionConn != nil {
		return s.positionConn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		s.positionAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	s.positionConn = conn
	return conn, nil
}

func (s *apiGatewayService) exposureClient(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.exposureConn != nil {
		return s.exposureConn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		s.exposureAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	s.exposureConn = conn
	return conn, nil
}

func (s *apiGatewayService) marginClient(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.marginConn != nil {
		return s.marginConn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		s.marginAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	s.marginConn = conn
	return conn, nil
}

func (s *apiGatewayService) close() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.orderConn != nil {
		_ = s.orderConn.Close()
		s.orderConn = nil
	}
	if s.marketConn != nil {
		_ = s.marketConn.Close()
		s.marketConn = nil
	}
	if s.positionConn != nil {
		_ = s.positionConn.Close()
		s.positionConn = nil
	}
	if s.exposureConn != nil {
		_ = s.exposureConn.Close()
		s.exposureConn = nil
	}
	if s.marginConn != nil {
		_ = s.marginConn.Close()
		s.marginConn = nil
	}
}

func (s *apiGatewayService) fetchPositions(ctx context.Context, accountID string) []*pb.Position {
	conn, err := s.positionClient(ctx)
	if err != nil {
		log.Printf("position service unavailable, using gateway fallback: %v", err)
		return nil
	}

	resp := &pb.AccountPositionResponse{}
	if err := conn.Invoke(ctx, "/rms.position.PositionTrackingService/GetPositionsForAccount", &pb.AccountPositionRequest{AccountId: accountID}, resp); err != nil {
		log.Printf("position lookup failed, using gateway fallback: %v", err)
		return nil
	}
	return resp.Positions
}

func (s *apiGatewayService) fetchExposures(ctx context.Context, accountID string) []*pb.AggregatedExposure {
	conn, err := s.exposureClient(ctx)
	if err != nil {
		log.Printf("exposure service unavailable, using gateway fallback: %v", err)
		return nil
	}

	resp := &pb.AccountExposureResponse{}
	if err := conn.Invoke(ctx, "/rms.exposure.ExposureAggregationService/GetAccountExposure", &pb.AccountExposureRequest{AccountId: accountID}, resp); err != nil {
		log.Printf("exposure lookup failed, using gateway fallback: %v", err)
		return nil
	}
	return resp.Exposures
}

func (s *apiGatewayService) fetchMargin(ctx context.Context, accountID string) *pb.MarginRequirement {
	conn, err := s.marginClient(ctx)
	if err != nil {
		log.Printf("margin service unavailable, using gateway fallback: %v", err)
		return nil
	}

	resp := &pb.AccountMarginResponse{}
	if err := conn.Invoke(ctx, "/rms.margin.MarginCalculationEngine/GetAccountMargin", &pb.AccountMarginRequest{AccountId: accountID}, resp); err != nil {
		log.Printf("margin lookup failed, using gateway fallback: %v", err)
		return nil
	}
	return resp.MarginRequirement
}

func (s *apiGatewayService) authorizeContext(ctx context.Context) error {
	if s.authVerifier == nil {
		return nil
	}
	if claims, ok := ctx.Value(authClaimsContextKey{}).(*security.JWTClaims); ok && claims != nil {
		if !security.HasRole(claims, "trader", "allocator", "admin") {
			return status.Error(codes.PermissionDenied, "insufficient role")
		}
		return nil
	}
	token := authTokenFromContext(ctx)
	if token == "" {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}
	claims, err := s.authVerifier.Verify(token)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if !security.HasRole(claims, "trader", "allocator", "admin") {
		return status.Error(codes.PermissionDenied, "insufficient role")
	}
	return nil
}

func authTokenFromContext(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("authorization"); len(values) > 0 {
			return security.ParseBearer(values[0])
		}
		if values := md.Get("x-rms-token"); len(values) > 0 {
			return security.ParseBearer(values[0])
		}
	}
	return ""
}

func gatewayFallbackRiskDecision(req *pb.OrderRequest) *pb.OrderResponse {
	order := req.Order
	return &pb.OrderResponse{
		OrderId:      order.OrderId,
		Approved:     false,
		RejectReason: "order ingestion service unavailable",
		Violations: []*pb.Violation{{
			RuleId:          "gateway-unavailable",
			RuleDescription: "Order ingestion service unavailable; request failed closed",
			Severity:        "CRITICAL",
		}},
		Timestamp:    time.Now().UnixMilli(),
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50050")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	svc := newAPIGatewayService()
	s := grpcx.NewServer(grpcx.ServerConfig{ServiceName: "api-gateway"}, grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterAPIGatewayServiceServer(s, svc)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	// Register reflection service on gRPC server.
	reflection.Register(s)

	go serveREST(svc)

	go func() {
		log.Println("Starting gRPC server on :50050")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutting down gRPC server...")
	s.GracefulStop()
	svc.close()
}

func syntheticAccount(accountID string) *pb.Account {
	if accountID == "" {
		accountID = "synthetic-account"
	}
	base := 300_000.0 + float64(len(accountID))*15_000.0
	return &pb.Account{
		AccountId:               accountID,
		UserId:                  "synthetic-user",
		Status:                  "ACTIVE",
		BuyingPower:             base * 2,
		CashBalance:             base,
		MarketValue:             base * 0.75,
		DayTradingBuyingPower:   base * 3,
		MaintenanceMarginExcess: base * 0.2,
		Metadata: map[string]string{
			"desk":      "gateway",
			"tier":      "institutional",
			"source":    "synthetic",
		},
	}
}

func computeMargin(account *pb.Account, positions []*pb.Position) *pb.MarginRequirement {
	notional := 0.0
	for _, position := range positions {
		notional += math.Abs(float64(position.Quantity) * position.AveragePrice)
	}
	initial := notional * 0.5
	maintenance := notional * 0.3
	equity := account.CashBalance + account.MarketValue
	return &pb.MarginRequirement{
		AccountId:         account.AccountId,
		InitialMargin:     initial,
		MaintenanceMargin: maintenance,
		MarginExcess:      equity - maintenance,
		MarginRatio:       safeDiv(equity, maintenance),
		Timestamp:         time.Now().UnixMilli(),
	}
}

func aggregateExposures(accountID string, positions []*pb.Position) []*pb.AggregatedExposure {
	exposures := make([]*pb.AggregatedExposure, 0, len(positions))
	for _, position := range positions {
		if position == nil {
			continue
		}
		notional := math.Abs(float64(position.Quantity) * position.AveragePrice)
		exposures = append(exposures, &pb.AggregatedExposure{
			AccountId:     accountID,
			Symbol:        position.Symbol,
			NetQuantity:   position.Quantity,
			GrossExposure: notional,
			NetExposure:   float64(position.Quantity) * position.AveragePrice,
			MarketValue:   position.MarketValue,
			Timestamp:     position.Timestamp,
		})
	}
	if len(exposures) == 0 {
		exposures = append(exposures, &pb.AggregatedExposure{
			AccountId:   accountID,
			Symbol:      "AAPL",
			NetQuantity: 0,
			Timestamp:   time.Now().UnixMilli(),
		})
	}
	return exposures
}

func syntheticMarketDataResponse(symbols []string) *pb.MarketDataResponse {
	if len(symbols) == 0 {
		symbols = []string{"AAPL", "MSFT", "NVDA"}
	}
	updates := make([]*pb.MarketDataUpdate, 0, len(symbols))
	for _, symbol := range symbols {
		base := 100 + float64(len(symbol))*7.5
		updates = append(updates, &pb.MarketDataUpdate{
			Symbol:    symbol,
			BidPrice:  base - 0.05,
			AskPrice:  base + 0.05,
			LastPrice: base,
			Volume:    int64(1_000_000 + rand.Intn(50_000)),
			Timestamp: time.Now().UnixMilli(),
		})
	}
	return &pb.MarketDataResponse{
		MarketData: updates,
		Timestamp:  time.Now().UnixMilli(),
	}
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		var value int
		if _, err := fmt.Sscanf(raw, "%d", &value); err == nil {
			return value
		}
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			return d
		}
	}
	return fallback
}

func serveREST(svc *apiGatewayService) {
	port := envString("RMS_HTTP_PORT", "8080")
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics := fmt.Sprintf(
			"# HELP rms_gateway_requests_total Total gateway requests\n# TYPE rms_gateway_requests_total counter\nrms_gateway_requests_total %d\n# HELP rms_gateway_approved_total Approved orders\n# TYPE rms_gateway_approved_total counter\nrms_gateway_approved_total %d\n# HELP rms_gateway_rejected_total Rejected orders\n# TYPE rms_gateway_rejected_total counter\nrms_gateway_rejected_total %d\n# HELP rms_gateway_throttled_total Throttled orders\n# TYPE rms_gateway_throttled_total counter\nrms_gateway_throttled_total %d\n",
			svc.requestCount.Load(),
			svc.approvedCount.Load(),
			svc.rejectedCount.Load(),
			svc.throttledCount.Load(),
		)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(metrics))
	})
	mux.HandleFunc("/v1/orders/evaluate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, err := svc.authorizeHTTPContext(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		var req pb.OrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := svc.SubmitOrder(ctx, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/orders/batch-evaluate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, err := svc.authorizeHTTPContext(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		var req pb.BatchOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		results := make([]*pb.OrderResponse, 0, len(req.Orders))
		for i, order := range req.Orders {
			var account *pb.Account
			if i < len(req.Accounts) {
				account = req.Accounts[i]
			}
			resp, err := svc.SubmitOrder(ctx, &pb.OrderRequest{Order: order, Account: account})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			results = append(results, resp)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"responses": results,
			"timestamp": time.Now().UnixMilli(),
		})
	})
	mux.HandleFunc("/v1/orders/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, err := svc.authorizeHTTPContext(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		var req pb.CancelOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := svc.CancelOrder(ctx, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/accounts/", func(w http.ResponseWriter, r *http.Request) {
		ctx, err := svc.authorizeHTTPContext(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		accountID := strings.TrimPrefix(r.URL.Path, "/v1/accounts/")
		if accountID == "" {
			http.NotFound(w, r)
			return
		}
		resp, err := svc.GetAccountInfo(ctx, &pb.AccountInfoRequest{AccountId: accountID})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/v1/marketdata/latest", func(w http.ResponseWriter, r *http.Request) {
		ctx, err := svc.authorizeHTTPContext(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		symbols := parseSymbols(r.URL.Query().Get("symbols"))
		resp, err := svc.GetMarketData(ctx, &pb.MarketDataRequest{Symbols: symbols})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})

	addr := net.JoinHostPort("", port)
	log.Printf("Starting REST server on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Printf("REST server stopped: %v", err)
	}
}

func (s *apiGatewayService) authorizeHTTPContext(r *http.Request) (context.Context, error) {
	if s.authVerifier == nil {
		return r.Context(), nil
	}
	token := security.ParseBearer(r.Header.Get("Authorization"))
	if token == "" {
		return nil, fmt.Errorf("missing bearer token")
	}
	claims, err := s.authVerifier.Verify(token)
	if err != nil {
		return nil, err
	}
	return context.WithValue(r.Context(), authClaimsContextKey{}, claims), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parseSymbols(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	symbols := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			symbols = append(symbols, strings.ToUpper(value))
		}
	}
	return symbols
}
