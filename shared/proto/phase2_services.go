package proto

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OrderIngestionService
type OrderIngestionServiceServer interface {
	SubmitOrder(context.Context, *OrderIngestionRequest) (*OrderIngestionResponse, error)
	GetOrderDecision(context.Context, *OrderDecisionQueryRequest) (*OrderDecisionQueryResponse, error)
	StreamOrderDecisions(*OrderDecisionQueryRequest, OrderIngestionService_StreamOrderDecisionsServer) error
	mustEmbedUnimplementedOrderIngestionServiceServer()
}

type UnimplementedOrderIngestionServiceServer struct{}

func (UnimplementedOrderIngestionServiceServer) SubmitOrder(context.Context, *OrderIngestionRequest) (*OrderIngestionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SubmitOrder not implemented")
}

func (UnimplementedOrderIngestionServiceServer) GetOrderDecision(context.Context, *OrderDecisionQueryRequest) (*OrderDecisionQueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOrderDecision not implemented")
}

func (UnimplementedOrderIngestionServiceServer) StreamOrderDecisions(*OrderDecisionQueryRequest, OrderIngestionService_StreamOrderDecisionsServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamOrderDecisions not implemented")
}

func (UnimplementedOrderIngestionServiceServer) mustEmbedUnimplementedOrderIngestionServiceServer() {}

type OrderIngestionService_StreamOrderDecisionsServer interface {
	Send(*OrderIngestionResponse) error
	grpc.ServerStream
}

func RegisterOrderIngestionServiceServer(s grpc.ServiceRegistrar, srv OrderIngestionServiceServer) {
	s.RegisterService(&OrderIngestionService_ServiceDesc, srv)
}

var OrderIngestionService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rms.orders.v1.OrderIngestionService",
	HandlerType: (*OrderIngestionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "SubmitOrder", Handler: _OrderIngestionService_SubmitOrder_Handler},
		{MethodName: "GetOrderDecision", Handler: _OrderIngestionService_GetOrderDecision_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "StreamOrderDecisions", Handler: _OrderIngestionService_StreamOrderDecisions_Handler, ServerStreams: true},
	},
	Metadata: "orders/v1/orders.proto",
}

func _OrderIngestionService_SubmitOrder_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OrderIngestionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OrderIngestionServiceServer).SubmitOrder(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.orders.v1.OrderIngestionService/SubmitOrder"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OrderIngestionServiceServer).SubmitOrder(ctx, req.(*OrderIngestionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OrderIngestionService_GetOrderDecision_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OrderDecisionQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OrderIngestionServiceServer).GetOrderDecision(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/rms.orders.v1.OrderIngestionService/GetOrderDecision"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OrderIngestionServiceServer).GetOrderDecision(ctx, req.(*OrderDecisionQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OrderIngestionService_StreamOrderDecisions_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(OrderDecisionQueryRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(OrderIngestionServiceServer).StreamOrderDecisions(m, &orderIngestionServiceStreamOrderDecisionsServer{stream})
}

type orderIngestionServiceStreamOrderDecisionsServer struct {
	grpc.ServerStream
}

func (x *orderIngestionServiceStreamOrderDecisionsServer) Send(m *OrderIngestionResponse) error {
	return x.ServerStream.SendMsg(m)
}
