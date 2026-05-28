package httpx

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/example/rms/shared/platform/observability"
	"github.com/example/rms/shared/platform/security"
)

type middlewareKey struct{}

// Middleware represents a standard HTTP middleware.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares in order.
func Chain(handler http.Handler, middlewares ...Middleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// RequestID ensures every inbound request carries a request ID.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace := observability.TraceFromContext(r.Context())
		if trace.RequestID == "" {
			trace = observability.NewTraceContext()
		}
		if requestID := strings.TrimSpace(r.Header.Get("X-Request-Id")); requestID != "" {
			trace.RequestID = requestID
		}
		ctx := observability.ContextWithTrace(r.Context(), trace)
		w.Header().Set("X-Request-Id", trace.RequestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Tracing propagates trace headers across HTTP requests.
func Tracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := observability.ExtractHTTPContext(r.Context(), r.Header)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// SecurityHeaders adds a baseline set of security headers.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		next.ServeHTTP(w, r)
	})
}

// Recovery converts panics into 500 responses.
func Recovery(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.Error("panic recovered", "panic", recovered)
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RateLimit applies a token bucket keyed by request identity.
func RateLimit(limiter *security.Limiter, keyFunc func(*http.Request) string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limiter == nil {
				next.ServeHTTP(w, r)
				return
			}
			key := ""
			if keyFunc != nil {
				key = keyFunc(r)
			}
			if key == "" {
				key = r.RemoteAddr
			}
			if !limiter.Allow(key) {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Logging emits basic request logs.
func Logging(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			writer := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(writer, r)
			trace := observability.TraceFromContext(r.Context())
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", writer.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", trace.RequestID,
				"trace_id", trace.TraceID,
			)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// JSON writes a JSON response.
func JSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// DecodeJSON reads a JSON request body.
func DecodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}

// HealthHandler returns a liveness/readiness endpoint.
func HealthHandler(service string, ready bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := "unhealthy"
		code := http.StatusServiceUnavailable
		if ready {
			status = "healthy"
			code = http.StatusOK
		}
		JSON(w, code, map[string]string{
			"service": service,
			"status":  status,
		})
	}
}

// MetricsHandler exposes the provided registry.
func MetricsHandler(registry *observability.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if registry == nil {
			_, _ = io.WriteString(w, "")
			return
		}
		_, _ = io.WriteString(w, registry.Render())
	}
}

// ErrorJSON standardizes JSON error responses.
func ErrorJSON(w http.ResponseWriter, status int, message string) {
	JSON(w, status, map[string]string{
		"error": message,
	})
}

// ContextValue fetches the trace context from the request.
func ContextValue(ctx context.Context) observability.TraceContext {
	return observability.TraceFromContext(ctx)
}
