package proto

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PortfolioRiskService exposes the portfolio state, replay, and reconciliation controls.
type PortfolioRiskServiceServer interface {
	CalculatePortfolioRisk(context.Context, *PortfolioRiskRequest) (*PortfolioRiskResponse, error)
	GetPortfolioSnapshot(context.Context, *PortfolioSnapshotRequest) (*PortfolioSnapshotResponse, error)
	ReplayPortfolioState(context.Context, *PortfolioReplayRequest) (*PortfolioReplayResponse, error)
	ReconcilePortfolioState(context.Context, *PortfolioReconciliationRequest) (*PortfolioReconciliationResponse, error)
	StreamPortfolioRisk(*PortfolioRiskSubscriptionRequest, PortfolioRiskService_StreamPortfolioRiskServer) error
	mustEmbedUnimplementedPortfolioRiskServiceServer()
}

type UnimplementedPortfolioRiskServiceServer struct{}

func (UnimplementedPortfolioRiskServiceServer) CalculatePortfolioRisk(context.Context, *PortfolioRiskRequest) (*PortfolioRiskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CalculatePortfolioRisk not implemented")
}

func (UnimplementedPortfolioRiskServiceServer) GetPortfolioSnapshot(context.Context, *PortfolioSnapshotRequest) (*PortfolioSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPortfolioSnapshot not implemented")
}

func (UnimplementedPortfolioRiskServiceServer) ReplayPortfolioState(context.Context, *PortfolioReplayRequest) (*PortfolioReplayResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplayPortfolioState not implemented")
}

func (UnimplementedPortfolioRiskServiceServer) ReconcilePortfolioState(context.Context, *PortfolioReconciliationRequest) (*PortfolioReconciliationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReconcilePortfolioState not implemented")
}

func (UnimplementedPortfolioRiskServiceServer) StreamPortfolioRisk(*PortfolioRiskSubscriptionRequest, PortfolioRiskService_StreamPortfolioRiskServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamPortfolioRisk not implemented")
}

func (UnimplementedPortfolioRiskServiceServer) mustEmbedUnimplementedPortfolioRiskServiceServer() {}

type PortfolioRiskService_StreamPortfolioRiskServer interface {
	Send(*PortfolioRiskResponse) error
	grpc.ServerStream
}

func RegisterPortfolioRiskServiceServer(s grpc.ServiceRegistrar, srv PortfolioRiskServiceServer) {
	s.RegisterService(&PortfolioRiskService_ServiceDesc, srv)
}

var PortfolioRiskService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.portfolio.PortfolioRiskService",
	HandlerType: (*PortfolioRiskServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "CalculatePortfolioRisk", Handler: _PortfolioRiskService_CalculatePortfolioRisk_Handler},
		{MethodName: "GetPortfolioSnapshot", Handler: _PortfolioRiskService_GetPortfolioSnapshot_Handler},
		{MethodName: "ReplayPortfolioState", Handler: _PortfolioRiskService_ReplayPortfolioState_Handler},
		{MethodName: "ReconcilePortfolioState", Handler: _PortfolioRiskService_ReconcilePortfolioState_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamPortfolioRisk", Handler: _PortfolioRiskService_StreamPortfolioRisk_Handler, ServerStreams: true},
	},
	Metadata: "portfolio_risk.proto",
}

func _PortfolioRiskService_CalculatePortfolioRisk_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PortfolioRiskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PortfolioRiskServiceServer).CalculatePortfolioRisk(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.portfolio.PortfolioRiskService/CalculatePortfolioRisk"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PortfolioRiskServiceServer).CalculatePortfolioRisk(ctx, req.(*PortfolioRiskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PortfolioRiskService_GetPortfolioSnapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PortfolioSnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PortfolioRiskServiceServer).GetPortfolioSnapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.portfolio.PortfolioRiskService/GetPortfolioSnapshot"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PortfolioRiskServiceServer).GetPortfolioSnapshot(ctx, req.(*PortfolioSnapshotRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PortfolioRiskService_ReplayPortfolioState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PortfolioReplayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PortfolioRiskServiceServer).ReplayPortfolioState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.portfolio.PortfolioRiskService/ReplayPortfolioState"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PortfolioRiskServiceServer).ReplayPortfolioState(ctx, req.(*PortfolioReplayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PortfolioRiskService_ReconcilePortfolioState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PortfolioReconciliationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PortfolioRiskServiceServer).ReconcilePortfolioState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.portfolio.PortfolioRiskService/ReconcilePortfolioState"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PortfolioRiskServiceServer).ReconcilePortfolioState(ctx, req.(*PortfolioReconciliationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PortfolioRiskService_StreamPortfolioRisk_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(PortfolioRiskSubscriptionRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(PortfolioRiskServiceServer).StreamPortfolioRisk(m, &portfolioRiskServiceStreamPortfolioRiskServer{stream})
}

type portfolioRiskServiceStreamPortfolioRiskServer struct {
	grpc.ServerStream
}

func (x *portfolioRiskServiceStreamPortfolioRiskServer) Send(m *PortfolioRiskResponse) error {
	return x.ServerStream.SendMsg(m)
}
