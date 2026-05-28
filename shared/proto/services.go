package proto

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// APIGatewayService
type APIGatewayServiceServer interface {
	SubmitOrder(context.Context, *OrderRequest) (*OrderResponse, error)
	CancelOrder(context.Context, *CancelOrderRequest) (*CancelOrderResponse, error)
	GetAccountInfo(context.Context, *AccountInfoRequest) (*AccountInfoResponse, error)
	GetMarketData(context.Context, *MarketDataRequest) (*MarketDataResponse, error)
	StreamMarketData(*MarketDataSubscriptionRequest, APIGatewayService_StreamMarketDataServer) error
	mustEmbedUnimplementedAPIGatewayServiceServer()
}

type UnimplementedAPIGatewayServiceServer struct{}

func (UnimplementedAPIGatewayServiceServer) SubmitOrder(context.Context, *OrderRequest) (*OrderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SubmitOrder not implemented")
}
func (UnimplementedAPIGatewayServiceServer) CancelOrder(context.Context, *CancelOrderRequest) (*CancelOrderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CancelOrder not implemented")
}
func (UnimplementedAPIGatewayServiceServer) GetAccountInfo(context.Context, *AccountInfoRequest) (*AccountInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAccountInfo not implemented")
}
func (UnimplementedAPIGatewayServiceServer) GetMarketData(context.Context, *MarketDataRequest) (*MarketDataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMarketData not implemented")
}
func (UnimplementedAPIGatewayServiceServer) StreamMarketData(*MarketDataSubscriptionRequest, APIGatewayService_StreamMarketDataServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamMarketData not implemented")
}
func (UnimplementedAPIGatewayServiceServer) mustEmbedUnimplementedAPIGatewayServiceServer() {}

type APIGatewayService_StreamMarketDataServer interface {
	Send(*MarketDataUpdate) error
	grpc.ServerStream
}

func RegisterAPIGatewayServiceServer(s grpc.ServiceRegistrar, srv APIGatewayServiceServer) {
	s.RegisterService(&APIGatewayService_ServiceDesc, srv)
}

var APIGatewayService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.api.APIGatewayService",
	HandlerType: (*APIGatewayServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "SubmitOrder", Handler: _APIGatewayService_SubmitOrder_Handler},
		{MethodName: "CancelOrder", Handler: _APIGatewayService_CancelOrder_Handler},
		{MethodName: "GetAccountInfo", Handler: _APIGatewayService_GetAccountInfo_Handler},
		{MethodName: "GetMarketData", Handler: _APIGatewayService_GetMarketData_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamMarketData", Handler: _APIGatewayService_StreamMarketData_Handler, ServerStreams: true},
	},
	Metadata: "api_gateway.proto",
}

func _APIGatewayService_SubmitOrder_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(APIGatewayServiceServer).SubmitOrder(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.api.APIGatewayService/SubmitOrder"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(APIGatewayServiceServer).SubmitOrder(ctx, req.(*OrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _APIGatewayService_CancelOrder_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelOrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(APIGatewayServiceServer).CancelOrder(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.api.APIGatewayService/CancelOrder"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(APIGatewayServiceServer).CancelOrder(ctx, req.(*CancelOrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _APIGatewayService_GetAccountInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AccountInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(APIGatewayServiceServer).GetAccountInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.api.APIGatewayService/GetAccountInfo"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(APIGatewayServiceServer).GetAccountInfo(ctx, req.(*AccountInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _APIGatewayService_GetMarketData_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MarketDataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(APIGatewayServiceServer).GetMarketData(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.api.APIGatewayService/GetMarketData"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(APIGatewayServiceServer).GetMarketData(ctx, req.(*MarketDataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _APIGatewayService_StreamMarketData_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(MarketDataSubscriptionRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(APIGatewayServiceServer).StreamMarketData(m, &aPIGatewayServiceStreamMarketDataServer{stream})
}

type aPIGatewayServiceStreamMarketDataServer struct {
	grpc.ServerStream
}

func (x *aPIGatewayServiceStreamMarketDataServer) Send(m *MarketDataUpdate) error {
	return x.ServerStream.SendMsg(m)
}

// RiskEvaluationService
type RiskEvaluationServiceServer interface {
	EvaluateOrder(context.Context, *OrderRequest) (*OrderResponse, error)
	BatchEvaluateOrders(context.Context, *BatchOrderRequest) (*BatchOrderResponse, error)
	GetDecision(context.Context, *OrderDecisionQueryRequest) (*OrderDecisionQueryResponse, error)
	StreamOrderEvaluations(RiskEvaluationService_StreamOrderEvaluationsServer) error
	mustEmbedUnimplementedRiskEvaluationServiceServer()
}

type UnimplementedRiskEvaluationServiceServer struct{}

func (UnimplementedRiskEvaluationServiceServer) EvaluateOrder(context.Context, *OrderRequest) (*OrderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EvaluateOrder not implemented")
}
func (UnimplementedRiskEvaluationServiceServer) BatchEvaluateOrders(context.Context, *BatchOrderRequest) (*BatchOrderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BatchEvaluateOrders not implemented")
}
func (UnimplementedRiskEvaluationServiceServer) GetDecision(context.Context, *OrderDecisionQueryRequest) (*OrderDecisionQueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDecision not implemented")
}
func (UnimplementedRiskEvaluationServiceServer) StreamOrderEvaluations(RiskEvaluationService_StreamOrderEvaluationsServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamOrderEvaluations not implemented")
}
func (UnimplementedRiskEvaluationServiceServer) mustEmbedUnimplementedRiskEvaluationServiceServer() {}

type RiskEvaluationService_StreamOrderEvaluationsServer interface {
	Send(*OrderResponse) error
	Recv() (*OrderRequest, error)
	grpc.ServerStream
}

func RegisterRiskEvaluationServiceServer(s grpc.ServiceRegistrar, srv RiskEvaluationServiceServer) {
	s.RegisterService(&RiskEvaluationService_ServiceDesc, srv)
}

var RiskEvaluationService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.risk.RiskEvaluationService",
	HandlerType: (*RiskEvaluationServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "EvaluateOrder", Handler: _RiskEvaluationService_EvaluateOrder_Handler},
		{MethodName: "BatchEvaluateOrders", Handler: _RiskEvaluationService_BatchEvaluateOrders_Handler},
		{MethodName: "GetDecision", Handler: _RiskEvaluationService_GetDecision_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamOrderEvaluations", Handler: _RiskEvaluationService_StreamOrderEvaluations_Handler, ServerStreams: true, ClientStreams: true},
	},
	Metadata: "risk_evaluation.proto",
}

func _RiskEvaluationService_EvaluateOrder_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RiskEvaluationServiceServer).EvaluateOrder(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.risk.RiskEvaluationService/EvaluateOrder"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RiskEvaluationServiceServer).EvaluateOrder(ctx, req.(*OrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RiskEvaluationService_BatchEvaluateOrders_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchOrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RiskEvaluationServiceServer).BatchEvaluateOrders(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.risk.RiskEvaluationService/BatchEvaluateOrders"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RiskEvaluationServiceServer).BatchEvaluateOrders(ctx, req.(*BatchOrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RiskEvaluationService_GetDecision_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OrderDecisionQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RiskEvaluationServiceServer).GetDecision(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.risk.RiskEvaluationService/GetDecision"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RiskEvaluationServiceServer).GetDecision(ctx, req.(*OrderDecisionQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RiskEvaluationService_StreamOrderEvaluations_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RiskEvaluationServiceServer).StreamOrderEvaluations(&riskEvaluationServiceStreamOrderEvaluationsServer{stream})
}

type riskEvaluationServiceStreamOrderEvaluationsServer struct {
	grpc.ServerStream
}

func (x *riskEvaluationServiceStreamOrderEvaluationsServer) Send(m *OrderResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *riskEvaluationServiceStreamOrderEvaluationsServer) Recv() (*OrderRequest, error) {
	m := new(OrderRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ExposureAggregationService
type ExposureAggregationServiceServer interface {
	UpdatePosition(context.Context, *PositionUpdate) (*AggregatedExposure, error)
	GetAccountExposure(context.Context, *AccountExposureRequest) (*AccountExposureResponse, error)
	GetSymbolExposure(context.Context, *SymbolExposureRequest) (*SymbolExposureResponse, error)
	GetExposureSnapshot(context.Context, *ExposureSnapshotRequest) (*ExposureSnapshotResponse, error)
	ReplayExposureState(context.Context, *ExposureReplayRequest) (*ExposureReplayResponse, error)
	ReconcileExposureState(context.Context, *ExposureReconciliationRequest) (*ExposureReconciliationResponse, error)
	StreamAccountExposure(*AccountExposureRequest, ExposureAggregationService_StreamAccountExposureServer) error
	mustEmbedUnimplementedExposureAggregationServiceServer()
}

type UnimplementedExposureAggregationServiceServer struct{}

func (UnimplementedExposureAggregationServiceServer) UpdatePosition(context.Context, *PositionUpdate) (*AggregatedExposure, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdatePosition not implemented")
}
func (UnimplementedExposureAggregationServiceServer) GetAccountExposure(context.Context, *AccountExposureRequest) (*AccountExposureResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAccountExposure not implemented")
}
func (UnimplementedExposureAggregationServiceServer) GetSymbolExposure(context.Context, *SymbolExposureRequest) (*SymbolExposureResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSymbolExposure not implemented")
}
func (UnimplementedExposureAggregationServiceServer) GetExposureSnapshot(context.Context, *ExposureSnapshotRequest) (*ExposureSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetExposureSnapshot not implemented")
}
func (UnimplementedExposureAggregationServiceServer) ReplayExposureState(context.Context, *ExposureReplayRequest) (*ExposureReplayResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplayExposureState not implemented")
}
func (UnimplementedExposureAggregationServiceServer) ReconcileExposureState(context.Context, *ExposureReconciliationRequest) (*ExposureReconciliationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReconcileExposureState not implemented")
}
func (UnimplementedExposureAggregationServiceServer) StreamAccountExposure(*AccountExposureRequest, ExposureAggregationService_StreamAccountExposureServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamAccountExposure not implemented")
}
func (UnimplementedExposureAggregationServiceServer) mustEmbedUnimplementedExposureAggregationServiceServer() {}

type ExposureAggregationService_StreamAccountExposureServer interface {
	Send(*AggregatedExposure) error
	grpc.ServerStream
}

func RegisterExposureAggregationServiceServer(s grpc.ServiceRegistrar, srv ExposureAggregationServiceServer) {
	s.RegisterService(&ExposureAggregationService_ServiceDesc, srv)
}

var ExposureAggregationService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.exposure.ExposureAggregationService",
	HandlerType: (*ExposureAggregationServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "UpdatePosition", Handler: _ExposureAggregationService_UpdatePosition_Handler},
		{MethodName: "GetAccountExposure", Handler: _ExposureAggregationService_GetAccountExposure_Handler},
		{MethodName: "GetSymbolExposure", Handler: _ExposureAggregationService_GetSymbolExposure_Handler},
		{MethodName: "GetExposureSnapshot", Handler: _ExposureAggregationService_GetExposureSnapshot_Handler},
		{MethodName: "ReplayExposureState", Handler: _ExposureAggregationService_ReplayExposureState_Handler},
		{MethodName: "ReconcileExposureState", Handler: _ExposureAggregationService_ReconcileExposureState_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamAccountExposure", Handler: _ExposureAggregationService_StreamAccountExposure_Handler, ServerStreams: true},
	},
	Metadata: "exposure_aggregation.proto",
}

func _ExposureAggregationService_UpdatePosition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PositionUpdate)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExposureAggregationServiceServer).UpdatePosition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.exposure.ExposureAggregationService/UpdatePosition"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExposureAggregationServiceServer).UpdatePosition(ctx, req.(*PositionUpdate))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExposureAggregationService_GetAccountExposure_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AccountExposureRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExposureAggregationServiceServer).GetAccountExposure(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.exposure.ExposureAggregationService/GetAccountExposure"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExposureAggregationServiceServer).GetAccountExposure(ctx, req.(*AccountExposureRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExposureAggregationService_GetSymbolExposure_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SymbolExposureRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExposureAggregationServiceServer).GetSymbolExposure(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.exposure.ExposureAggregationService/GetSymbolExposure"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExposureAggregationServiceServer).GetSymbolExposure(ctx, req.(*SymbolExposureRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExposureAggregationService_GetExposureSnapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExposureSnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExposureAggregationServiceServer).GetExposureSnapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.exposure.ExposureAggregationService/GetExposureSnapshot"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExposureAggregationServiceServer).GetExposureSnapshot(ctx, req.(*ExposureSnapshotRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExposureAggregationService_ReplayExposureState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExposureReplayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExposureAggregationServiceServer).ReplayExposureState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.exposure.ExposureAggregationService/ReplayExposureState"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExposureAggregationServiceServer).ReplayExposureState(ctx, req.(*ExposureReplayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExposureAggregationService_ReconcileExposureState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExposureReconciliationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExposureAggregationServiceServer).ReconcileExposureState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.exposure.ExposureAggregationService/ReconcileExposureState"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExposureAggregationServiceServer).ReconcileExposureState(ctx, req.(*ExposureReconciliationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExposureAggregationService_StreamAccountExposure_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(AccountExposureRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ExposureAggregationServiceServer).StreamAccountExposure(m, &exposureAggregationServiceStreamAccountExposureServer{stream})
}

type exposureAggregationServiceStreamAccountExposureServer struct {
	grpc.ServerStream
}

func (x *exposureAggregationServiceStreamAccountExposureServer) Send(m *AggregatedExposure) error {
	return x.ServerStream.SendMsg(m)
}

// MarginCalculationEngine
type MarginCalculationEngineServer interface {
	CalculateMargin(context.Context, *MarginCalculationRequest) (*MarginCalculationResponse, error)
	GetAccountMargin(context.Context, *AccountMarginRequest) (*AccountMarginResponse, error)
	StreamMarginUpdates(*AccountMarginRequest, MarginCalculationEngine_StreamMarginUpdatesServer) error
	mustEmbedUnimplementedMarginCalculationEngineServer()
}

type UnimplementedMarginCalculationEngineServer struct{}

func (UnimplementedMarginCalculationEngineServer) CalculateMargin(context.Context, *MarginCalculationRequest) (*MarginCalculationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CalculateMargin not implemented")
}
func (UnimplementedMarginCalculationEngineServer) GetAccountMargin(context.Context, *AccountMarginRequest) (*AccountMarginResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAccountMargin not implemented")
}
func (UnimplementedMarginCalculationEngineServer) StreamMarginUpdates(*AccountMarginRequest, MarginCalculationEngine_StreamMarginUpdatesServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamMarginUpdates not implemented")
}
func (UnimplementedMarginCalculationEngineServer) mustEmbedUnimplementedMarginCalculationEngineServer() {}

type MarginCalculationEngine_StreamMarginUpdatesServer interface {
	Send(*MarginCalculationResponse) error
	grpc.ServerStream
}

func RegisterMarginCalculationEngineServer(s grpc.ServiceRegistrar, srv MarginCalculationEngineServer) {
	s.RegisterService(&MarginCalculationEngine_ServiceDesc, srv)
}

var MarginCalculationEngine_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.margin.MarginCalculationEngine",
	HandlerType: (*MarginCalculationEngineServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "CalculateMargin", Handler: _MarginCalculationEngine_CalculateMargin_Handler},
		{MethodName: "GetAccountMargin", Handler: _MarginCalculationEngine_GetAccountMargin_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamMarginUpdates", Handler: _MarginCalculationEngine_StreamMarginUpdates_Handler, ServerStreams: true},
	},
	Metadata: "margin_calculation.proto",
}

func _MarginCalculationEngine_CalculateMargin_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MarginCalculationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MarginCalculationEngineServer).CalculateMargin(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.margin.MarginCalculationEngine/CalculateMargin"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MarginCalculationEngineServer).CalculateMargin(ctx, req.(*MarginCalculationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MarginCalculationEngine_GetAccountMargin_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AccountMarginRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MarginCalculationEngineServer).GetAccountMargin(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.margin.MarginCalculationEngine/GetAccountMargin"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MarginCalculationEngineServer).GetAccountMargin(ctx, req.(*AccountMarginRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MarginCalculationEngine_StreamMarginUpdates_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(AccountMarginRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(MarginCalculationEngineServer).StreamMarginUpdates(m, &marginCalculationEngineStreamMarginUpdatesServer{stream})
}

type marginCalculationEngineStreamMarginUpdatesServer struct {
	grpc.ServerStream
}

func (x *marginCalculationEngineStreamMarginUpdatesServer) Send(m *MarginCalculationResponse) error {
	return x.ServerStream.SendMsg(m)
}

// PositionTrackingService
type PositionTrackingServiceServer interface {
	UpdatePosition(context.Context, *PositionUpdateRequest) (*PositionUpdateResponse, error)
	GetPosition(context.Context, *PositionRequest) (*PositionResponse, error)
	GetPositionsForAccount(context.Context, *AccountPositionRequest) (*AccountPositionResponse, error)
	GetPositionSnapshot(context.Context, *PositionSnapshotRequest) (*PositionSnapshotResponse, error)
	ReplayPositionState(context.Context, *PositionReplayRequest) (*PositionReplayResponse, error)
	ReconcilePositionState(context.Context, *PositionReconciliationRequest) (*PositionReconciliationResponse, error)
	StreamPositionUpdates(*PositionRequest, PositionTrackingService_StreamPositionUpdatesServer) error
	mustEmbedUnimplementedPositionTrackingServiceServer()
}

type UnimplementedPositionTrackingServiceServer struct{}

func (UnimplementedPositionTrackingServiceServer) UpdatePosition(context.Context, *PositionUpdateRequest) (*PositionUpdateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdatePosition not implemented")
}
func (UnimplementedPositionTrackingServiceServer) GetPosition(context.Context, *PositionRequest) (*PositionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPosition not implemented")
}
func (UnimplementedPositionTrackingServiceServer) GetPositionsForAccount(context.Context, *AccountPositionRequest) (*AccountPositionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPositionsForAccount not implemented")
}
func (UnimplementedPositionTrackingServiceServer) GetPositionSnapshot(context.Context, *PositionSnapshotRequest) (*PositionSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPositionSnapshot not implemented")
}
func (UnimplementedPositionTrackingServiceServer) ReplayPositionState(context.Context, *PositionReplayRequest) (*PositionReplayResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplayPositionState not implemented")
}
func (UnimplementedPositionTrackingServiceServer) ReconcilePositionState(context.Context, *PositionReconciliationRequest) (*PositionReconciliationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReconcilePositionState not implemented")
}
func (UnimplementedPositionTrackingServiceServer) StreamPositionUpdates(*PositionRequest, PositionTrackingService_StreamPositionUpdatesServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamPositionUpdates not implemented")
}
func (UnimplementedPositionTrackingServiceServer) mustEmbedUnimplementedPositionTrackingServiceServer() {}

type PositionTrackingService_StreamPositionUpdatesServer interface {
	Send(*PositionUpdateResponse) error
	grpc.ServerStream
}

func RegisterPositionTrackingServiceServer(s grpc.ServiceRegistrar, srv PositionTrackingServiceServer) {
	s.RegisterService(&PositionTrackingService_ServiceDesc, srv)
}

var PositionTrackingService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.position.PositionTrackingService",
	HandlerType: (*PositionTrackingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "UpdatePosition", Handler: _PositionTrackingService_UpdatePosition_Handler},
		{MethodName: "GetPosition", Handler: _PositionTrackingService_GetPosition_Handler},
		{MethodName: "GetPositionsForAccount", Handler: _PositionTrackingService_GetPositionsForAccount_Handler},
		{MethodName: "GetPositionSnapshot", Handler: _PositionTrackingService_GetPositionSnapshot_Handler},
		{MethodName: "ReplayPositionState", Handler: _PositionTrackingService_ReplayPositionState_Handler},
		{MethodName: "ReconcilePositionState", Handler: _PositionTrackingService_ReconcilePositionState_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamPositionUpdates", Handler: _PositionTrackingService_StreamPositionUpdates_Handler, ServerStreams: true},
	},
	Metadata: "position_tracking.proto",
}

func _PositionTrackingService_UpdatePosition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PositionUpdateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PositionTrackingServiceServer).UpdatePosition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.position.PositionTrackingService/UpdatePosition"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PositionTrackingServiceServer).UpdatePosition(ctx, req.(*PositionUpdateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PositionTrackingService_GetPosition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PositionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PositionTrackingServiceServer).GetPosition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.position.PositionTrackingService/GetPosition"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PositionTrackingServiceServer).GetPosition(ctx, req.(*PositionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PositionTrackingService_GetPositionsForAccount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AccountPositionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PositionTrackingServiceServer).GetPositionsForAccount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.position.PositionTrackingService/GetPositionsForAccount"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PositionTrackingServiceServer).GetPositionsForAccount(ctx, req.(*AccountPositionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PositionTrackingService_GetPositionSnapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PositionSnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PositionTrackingServiceServer).GetPositionSnapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.position.PositionTrackingService/GetPositionSnapshot"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PositionTrackingServiceServer).GetPositionSnapshot(ctx, req.(*PositionSnapshotRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PositionTrackingService_ReplayPositionState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PositionReplayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PositionTrackingServiceServer).ReplayPositionState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.position.PositionTrackingService/ReplayPositionState"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PositionTrackingServiceServer).ReplayPositionState(ctx, req.(*PositionReplayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PositionTrackingService_ReconcilePositionState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PositionReconciliationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PositionTrackingServiceServer).ReconcilePositionState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.position.PositionTrackingService/ReconcilePositionState"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PositionTrackingServiceServer).ReconcilePositionState(ctx, req.(*PositionReconciliationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PositionTrackingService_StreamPositionUpdates_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(PositionRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(PositionTrackingServiceServer).StreamPositionUpdates(m, &positionTrackingServiceStreamPositionUpdatesServer{stream})
}

type positionTrackingServiceStreamPositionUpdatesServer struct {
	grpc.ServerStream
}

func (x *positionTrackingServiceStreamPositionUpdatesServer) Send(m *PositionUpdateResponse) error {
	return x.ServerStream.SendMsg(m)
}

// RuleEngineService
type RuleEngineServiceServer interface {
	GetActiveRules(context.Context, *GetActiveRulesRequest) (*GetActiveRulesResponse, error)
	EvaluateRules(context.Context, *RuleEvaluationRequest) (*RuleEvaluationResponse, error)
	ReloadRules(context.Context, *RuleReloadRequest) (*RuleReloadResponse, error)
	StreamRuleUpdates(*RuleUpdateRequest, RuleEngineService_StreamRuleUpdatesServer) error
	mustEmbedUnimplementedRuleEngineServiceServer()
}

type UnimplementedRuleEngineServiceServer struct{}

func (UnimplementedRuleEngineServiceServer) GetActiveRules(context.Context, *GetActiveRulesRequest) (*GetActiveRulesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetActiveRules not implemented")
}
func (UnimplementedRuleEngineServiceServer) EvaluateRules(context.Context, *RuleEvaluationRequest) (*RuleEvaluationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EvaluateRules not implemented")
}
func (UnimplementedRuleEngineServiceServer) ReloadRules(context.Context, *RuleReloadRequest) (*RuleReloadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReloadRules not implemented")
}
func (UnimplementedRuleEngineServiceServer) StreamRuleUpdates(*RuleUpdateRequest, RuleEngineService_StreamRuleUpdatesServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamRuleUpdates not implemented")
}
func (UnimplementedRuleEngineServiceServer) mustEmbedUnimplementedRuleEngineServiceServer() {}

type RuleEngineService_StreamRuleUpdatesServer interface {
	Send(*Rule) error
	grpc.ServerStream
}

func RegisterRuleEngineServiceServer(s grpc.ServiceRegistrar, srv RuleEngineServiceServer) {
	s.RegisterService(&RuleEngineService_ServiceDesc, srv)
}

var RuleEngineService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.rule.RuleEngineService",
	HandlerType: (*RuleEngineServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "GetActiveRules", Handler: _RuleEngineService_GetActiveRules_Handler},
		{MethodName: "EvaluateRules", Handler: _RuleEngineService_EvaluateRules_Handler},
		{MethodName: "ReloadRules", Handler: _RuleEngineService_ReloadRules_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamRuleUpdates", Handler: _RuleEngineService_StreamRuleUpdates_Handler, ServerStreams: true},
	},
	Metadata: "rule_engine.proto",
}

func _RuleEngineService_GetActiveRules_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetActiveRulesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RuleEngineServiceServer).GetActiveRules(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.rule.RuleEngineService/GetActiveRules"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RuleEngineServiceServer).GetActiveRules(ctx, req.(*GetActiveRulesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RuleEngineService_EvaluateRules_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RuleEvaluationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RuleEngineServiceServer).EvaluateRules(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.rule.RuleEngineService/EvaluateRules"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RuleEngineServiceServer).EvaluateRules(ctx, req.(*RuleEvaluationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RuleEngineService_ReloadRules_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RuleReloadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RuleEngineServiceServer).ReloadRules(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.rule.RuleEngineService/ReloadRules"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RuleEngineServiceServer).ReloadRules(ctx, req.(*RuleReloadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RuleEngineService_StreamRuleUpdates_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RuleUpdateRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RuleEngineServiceServer).StreamRuleUpdates(m, &ruleEngineServiceStreamRuleUpdatesServer{stream})
}

type ruleEngineServiceStreamRuleUpdatesServer struct {
	grpc.ServerStream
}

func (x *ruleEngineServiceStreamRuleUpdatesServer) Send(m *Rule) error {
	return x.ServerStream.SendMsg(m)
}

// AlertingService
type AlertingServiceServer interface {
	SendAlert(context.Context, *AlertRequest) (*AlertResponse, error)
	GetAlertsForAccount(context.Context, *AccountAlertRequest) (*AccountAlertResponse, error)
	GetAlertsForRule(context.Context, *RuleAlertRequest) (*RuleAlertResponse, error)
	StreamAlerts(*AlertSubscriptionRequest, AlertingService_StreamAlertsServer) error
	mustEmbedUnimplementedAlertingServiceServer()
}

type UnimplementedAlertingServiceServer struct{}

func (UnimplementedAlertingServiceServer) SendAlert(context.Context, *AlertRequest) (*AlertResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendAlert not implemented")
}
func (UnimplementedAlertingServiceServer) GetAlertsForAccount(context.Context, *AccountAlertRequest) (*AccountAlertResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAlertsForAccount not implemented")
}
func (UnimplementedAlertingServiceServer) GetAlertsForRule(context.Context, *RuleAlertRequest) (*RuleAlertResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAlertsForRule not implemented")
}
func (UnimplementedAlertingServiceServer) StreamAlerts(*AlertSubscriptionRequest, AlertingService_StreamAlertsServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamAlerts not implemented")
}
func (UnimplementedAlertingServiceServer) mustEmbedUnimplementedAlertingServiceServer() {}

type AlertingService_StreamAlertsServer interface {
	Send(*AlertRecord) error
	grpc.ServerStream
}

func RegisterAlertingServiceServer(s grpc.ServiceRegistrar, srv AlertingServiceServer) {
	s.RegisterService(&AlertingService_ServiceDesc, srv)
}

var AlertingService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.alerting.AlertingService",
	HandlerType: (*AlertingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "SendAlert", Handler: _AlertingService_SendAlert_Handler},
		{MethodName: "GetAlertsForAccount", Handler: _AlertingService_GetAlertsForAccount_Handler},
		{MethodName: "GetAlertsForRule", Handler: _AlertingService_GetAlertsForRule_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamAlerts", Handler: _AlertingService_StreamAlerts_Handler, ServerStreams: true},
	},
	Metadata: "alerting_service.proto",
}

func _AlertingService_SendAlert_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AlertRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AlertingServiceServer).SendAlert(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.alerting.AlertingService/SendAlert"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AlertingServiceServer).SendAlert(ctx, req.(*AlertRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AlertingService_GetAlertsForAccount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AccountAlertRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AlertingServiceServer).GetAlertsForAccount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.alerting.AlertingService/GetAlertsForAccount"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AlertingServiceServer).GetAlertsForAccount(ctx, req.(*AccountAlertRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AlertingService_GetAlertsForRule_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RuleAlertRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AlertingServiceServer).GetAlertsForRule(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.alerting.AlertingService/GetAlertsForRule"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AlertingServiceServer).GetAlertsForRule(ctx, req.(*RuleAlertRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AlertingService_StreamAlerts_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(AlertSubscriptionRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(AlertingServiceServer).StreamAlerts(m, &alertingServiceStreamAlertsServer{stream})
}

type alertingServiceStreamAlertsServer struct {
	grpc.ServerStream
}

func (x *alertingServiceStreamAlertsServer) Send(m *AlertRecord) error {
	return x.ServerStream.SendMsg(m)
}

// AuditLoggingService
type AuditLoggingServiceServer interface {
	LogEvent(context.Context, *AuditEvent) (*AuditResponse, error)
	GetEvents(context.Context, *AuditQueryRequest) (*AuditQueryResponse, error)
	StreamEvents(*AuditQueryRequest, AuditLoggingService_StreamEventsServer) error
	mustEmbedUnimplementedAuditLoggingServiceServer()
}

type UnimplementedAuditLoggingServiceServer struct{}

func (UnimplementedAuditLoggingServiceServer) LogEvent(context.Context, *AuditEvent) (*AuditResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LogEvent not implemented")
}
func (UnimplementedAuditLoggingServiceServer) GetEvents(context.Context, *AuditQueryRequest) (*AuditQueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetEvents not implemented")
}
func (UnimplementedAuditLoggingServiceServer) StreamEvents(*AuditQueryRequest, AuditLoggingService_StreamEventsServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamEvents not implemented")
}
func (UnimplementedAuditLoggingServiceServer) mustEmbedUnimplementedAuditLoggingServiceServer() {}

type AuditLoggingService_StreamEventsServer interface {
	Send(*AuditEvent) error
	grpc.ServerStream
}

func RegisterAuditLoggingServiceServer(s grpc.ServiceRegistrar, srv AuditLoggingServiceServer) {
	s.RegisterService(&AuditLoggingService_ServiceDesc, srv)
}

var AuditLoggingService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.audit.AuditLoggingService",
	HandlerType: (*AuditLoggingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "LogEvent", Handler: _AuditLoggingService_LogEvent_Handler},
		{MethodName: "GetEvents", Handler: _AuditLoggingService_GetEvents_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamEvents", Handler: _AuditLoggingService_StreamEvents_Handler, ServerStreams: true},
	},
	Metadata: "audit_logging.proto",
}

func _AuditLoggingService_LogEvent_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuditEvent)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuditLoggingServiceServer).LogEvent(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.audit.AuditLoggingService/LogEvent"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuditLoggingServiceServer).LogEvent(ctx, req.(*AuditEvent))
	}
	return interceptor(ctx, in, info, handler)
}

func _AuditLoggingService_GetEvents_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuditQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuditLoggingServiceServer).GetEvents(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.audit.AuditLoggingService/GetEvents"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuditLoggingServiceServer).GetEvents(ctx, req.(*AuditQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AuditLoggingService_StreamEvents_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(AuditQueryRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(AuditLoggingServiceServer).StreamEvents(m, &auditLoggingServiceStreamEventsServer{stream})
}

type auditLoggingServiceStreamEventsServer struct {
	grpc.ServerStream
}

func (x *auditLoggingServiceStreamEventsServer) Send(m *AuditEvent) error {
	return x.ServerStream.SendMsg(m)
}

// MarketDataConsumerService
type MarketDataConsumerServiceServer interface {
	SubscribeToMarketData(*MarketDataSubscriptionRequest, MarketDataConsumerService_SubscribeToMarketDataServer) error
	GetLatestMarketData(context.Context, *MarketDataRequest) (*MarketDataResponse, error)
	GetMarketDataHistory(context.Context, *MarketDataHistoryRequest) (*MarketDataHistoryResponse, error)
	mustEmbedUnimplementedMarketDataConsumerServiceServer()
}

type UnimplementedMarketDataConsumerServiceServer struct{}

func (UnimplementedMarketDataConsumerServiceServer) SubscribeToMarketData(*MarketDataSubscriptionRequest, MarketDataConsumerService_SubscribeToMarketDataServer) error {
	return status.Errorf(codes.Unimplemented, "method SubscribeToMarketData not implemented")
}
func (UnimplementedMarketDataConsumerServiceServer) GetLatestMarketData(context.Context, *MarketDataRequest) (*MarketDataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetLatestMarketData not implemented")
}
func (UnimplementedMarketDataConsumerServiceServer) GetMarketDataHistory(context.Context, *MarketDataHistoryRequest) (*MarketDataHistoryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMarketDataHistory not implemented")
}
func (UnimplementedMarketDataConsumerServiceServer) mustEmbedUnimplementedMarketDataConsumerServiceServer() {}

type MarketDataConsumerService_SubscribeToMarketDataServer interface {
	Send(*MarketDataUpdate) error
	grpc.ServerStream
}

func RegisterMarketDataConsumerServiceServer(s grpc.ServiceRegistrar, srv MarketDataConsumerServiceServer) {
	s.RegisterService(&MarketDataConsumerService_ServiceDesc, srv)
}

var MarketDataConsumerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.marketdata.MarketDataConsumerService",
	HandlerType: (*MarketDataConsumerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "GetLatestMarketData", Handler: _MarketDataConsumerService_GetLatestMarketData_Handler},
		{MethodName: "GetMarketDataHistory", Handler: _MarketDataConsumerService_GetMarketDataHistory_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "SubscribeToMarketData", Handler: _MarketDataConsumerService_SubscribeToMarketData_Handler, ServerStreams: true},
	},
	Metadata: "market_data_consumer.proto",
}

func _MarketDataConsumerService_SubscribeToMarketData_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(MarketDataSubscriptionRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(MarketDataConsumerServiceServer).SubscribeToMarketData(m, &marketDataConsumerServiceSubscribeToMarketDataServer{stream})
}

type marketDataConsumerServiceSubscribeToMarketDataServer struct {
	grpc.ServerStream
}

func (x *marketDataConsumerServiceSubscribeToMarketDataServer) Send(m *MarketDataUpdate) error {
	return x.ServerStream.SendMsg(m)
}

func _MarketDataConsumerService_GetLatestMarketData_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MarketDataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MarketDataConsumerServiceServer).GetLatestMarketData(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.marketdata.MarketDataConsumerService/GetLatestMarketData"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MarketDataConsumerServiceServer).GetLatestMarketData(ctx, req.(*MarketDataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MarketDataConsumerService_GetMarketDataHistory_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MarketDataHistoryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MarketDataConsumerServiceServer).GetMarketDataHistory(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.marketdata.MarketDataConsumerService/GetMarketDataHistory"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MarketDataConsumerServiceServer).GetMarketDataHistory(ctx, req.(*MarketDataHistoryRequest))
	}
	return interceptor(ctx, in, info, handler)
}
