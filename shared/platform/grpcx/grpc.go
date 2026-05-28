package grpcx

import (
	"context"
	"log/slog"
	"time"

	"github.com/example/rms/shared/platform/observability"
	"github.com/example/rms/shared/platform/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ServerConfig captures the shared gRPC runtime knobs.
type ServerConfig struct {
	ServiceName string
	Logger      *slog.Logger
	Metrics     *observability.Registry
	Limiter     *security.Limiter
}

// NewServer creates a gRPC server with shared middleware enabled.
func NewServer(cfg ServerConfig, opts ...grpc.ServerOption) *grpc.Server {
	unary := grpc.ChainUnaryInterceptor(
		unaryLogging(cfg),
		unaryTracing(),
		unaryRecovery(cfg.Logger),
		unaryRateLimit(cfg.Limiter),
	)
	stream := grpc.ChainStreamInterceptor(
		streamTracing(),
		streamRecovery(cfg.Logger),
	)
	options := append([]grpc.ServerOption{unary, stream}, opts...)
	return grpc.NewServer(options...)
}

// ClientUnaryInterceptor injects trace metadata into outbound unary RPCs.
func ClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = observability.InjectGRPC(ctx, md)
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ClientStreamInterceptor injects trace metadata into outbound streaming RPCs.
func ClientStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = observability.InjectGRPC(ctx, md)
		ctx = metadata.NewOutgoingContext(ctx, md)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func unaryLogging(cfg ServerConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		trace := observability.TraceFromContext(ctx)
		if cfg.Logger != nil {
			cfg.Logger.Info("grpc request",
				"service", cfg.ServiceName,
				"method", info.FullMethod,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", trace.RequestID,
				"trace_id", trace.TraceID,
				"peer", peerString(ctx),
				"error", errorString(err),
			)
		}
		return resp, err
	}
}

func unaryTracing() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		ctx = observability.ExtractGRPCContext(ctx, md)
		return handler(ctx, req)
	}
}

func unaryRecovery(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if logger != nil {
					logger.Error("grpc panic recovered", "method", info.FullMethod, "panic", recovered)
				}
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

func unaryRateLimit(limiter *security.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if limiter == nil {
			return handler(ctx, req)
		}
		key := peerString(ctx)
		if key == "" {
			key = info.FullMethod
		}
		if !limiter.Allow(key) {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

func streamTracing() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		ctx := observability.ExtractGRPCContext(ss.Context(), md)
		wrapped := &serverStreamWithContext{ServerStream: ss, ctx: ctx}
		return handler(srv, wrapped)
	}
}

func streamRecovery(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if logger != nil {
					logger.Error("grpc stream panic recovered", "method", info.FullMethod, "panic", recovered)
				}
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, ss)
	}
}

type serverStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStreamWithContext) Context() context.Context {
	return s.ctx
}

func peerString(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p != nil {
		return p.Addr.String()
	}
	return ""
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
