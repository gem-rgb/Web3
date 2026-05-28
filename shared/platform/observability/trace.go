package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
)

type traceContextKey struct{}

// TraceContext models the minimum state required for W3C trace propagation.
type TraceContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	RequestID    string
	Sampled      bool
}

// Span is a lightweight span record used by the platform middleware.
type Span struct {
	Name        string
	Trace       TraceContext
	StartedAt   time.Time
	Attributes  map[string]string
	Error       error
}

// NewTraceContext returns a fresh context suitable for an inbound request.
func NewTraceContext() TraceContext {
	return TraceContext{
		TraceID:   randomHex(16),
		SpanID:    randomHex(8),
		RequestID: randomHex(8),
		Sampled:   true,
	}
}

// ContextWithTrace attaches trace metadata to a context.
func ContextWithTrace(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey{}, tc)
}

// TraceFromContext reads trace metadata from a context.
func TraceFromContext(ctx context.Context) TraceContext {
	if ctx == nil {
		return NewTraceContext()
	}
	if value, ok := ctx.Value(traceContextKey{}).(TraceContext); ok {
		return value
	}
	return NewTraceContext()
}

// StartSpan creates a child span and returns an updated context.
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	parent := TraceFromContext(ctx)
	current := TraceContext{
		TraceID:      parent.TraceID,
		SpanID:       randomHex(8),
		ParentSpanID: parent.SpanID,
		RequestID:    parent.RequestID,
		Sampled:      parent.Sampled,
	}
	if current.TraceID == "" {
		current.TraceID = randomHex(16)
	}
	if current.RequestID == "" {
		current.RequestID = randomHex(8)
	}
	span := &Span{
		Name:      name,
		Trace:     current,
		StartedAt: time.Now().UTC(),
		Attributes: map[string]string{},
	}
	return ContextWithTrace(ctx, current), span
}

// End finalizes the span and returns the elapsed duration.
func (s *Span) End(err error) time.Duration {
	s.Error = err
	return time.Since(s.StartedAt)
}

// AddAttribute records a span attribute.
func (s *Span) AddAttribute(key, value string) {
	if s == nil {
		return
	}
	if s.Attributes == nil {
		s.Attributes = map[string]string{}
	}
	s.Attributes[key] = value
}

// InjectHTTP writes trace headers into an outbound request.
func InjectHTTP(ctx context.Context, header http.Header) {
	tc := TraceFromContext(ctx)
	if tc.TraceID == "" {
		tc = NewTraceContext()
	}
	header.Set("traceparent", traceParent(tc))
	header.Set("x-request-id", tc.RequestID)
}

// ExtractHTTPContext pulls trace headers from an inbound request.
func ExtractHTTPContext(ctx context.Context, header http.Header) context.Context {
	tc, err := parseTrace(header.Get("traceparent"))
	if err != nil {
		tc = NewTraceContext()
	}
	if requestID := strings.TrimSpace(header.Get("x-request-id")); requestID != "" {
		tc.RequestID = requestID
	}
	return ContextWithTrace(ctx, tc)
}

// ExtractHTTP pulls trace headers from an inbound request using a background context.
func ExtractHTTP(header http.Header) context.Context {
	return ExtractHTTPContext(context.Background(), header)
}

// InjectGRPC writes trace headers into gRPC metadata.
func InjectGRPC(ctx context.Context, md metadata.MD) metadata.MD {
	if md == nil {
		md = metadata.MD{}
	}
	tc := TraceFromContext(ctx)
	if tc.TraceID == "" {
		tc = NewTraceContext()
	}
	md.Set("traceparent", traceParent(tc))
	md.Set("x-request-id", tc.RequestID)
	return md
}

// ExtractGRPCContext pulls trace headers from gRPC metadata.
func ExtractGRPCContext(ctx context.Context, md metadata.MD) context.Context {
	if md == nil {
		return ContextWithTrace(ctx, NewTraceContext())
	}
	tc, err := parseTrace(firstValue(md.Get("traceparent")))
	if err != nil {
		tc = NewTraceContext()
	}
	if requestID := strings.TrimSpace(firstValue(md.Get("x-request-id"))); requestID != "" {
		tc.RequestID = requestID
	}
	return ContextWithTrace(ctx, tc)
}

// ExtractGRPC pulls trace headers from gRPC metadata using a background context.
func ExtractGRPC(md metadata.MD) context.Context {
	return ExtractGRPCContext(context.Background(), md)
}

func traceParent(tc TraceContext) string {
	sampled := "00"
	if tc.Sampled {
		sampled = "01"
	}
	return "00-" + normalizeHex(tc.TraceID, 32) + "-" + normalizeHex(tc.SpanID, 16) + "-" + sampled
}

func parseTrace(raw string) (TraceContext, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return TraceContext{}, errors.New("missing traceparent")
	}
	parts := strings.Split(raw, "-")
	if len(parts) != 4 {
		return TraceContext{}, errors.New("invalid traceparent")
	}
	tc := TraceContext{
		TraceID: normalizeHex(parts[1], 32),
		SpanID:  normalizeHex(parts[2], 16),
		Sampled: strings.ToLower(parts[3]) == "01",
	}
	if tc.TraceID == "" || tc.SpanID == "" {
		return TraceContext{}, errors.New("invalid trace identifiers")
	}
	return tc, nil
}

func firstValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func normalizeHex(raw string, length int) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if len(raw) > length {
		raw = raw[:length]
	}
	if len(raw) == length {
		return raw
	}
	padding := make([]byte, length-len(raw))
	for i := range padding {
		padding[i] = '0'
	}
	return string(padding) + raw
}

func randomHex(bytesLen int) string {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)
}
