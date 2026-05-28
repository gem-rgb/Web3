package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/example/rms/shared/platform/config"
	"github.com/example/rms/shared/platform/grpcx"
	platformhttp "github.com/example/rms/shared/platform/httpx"
	"github.com/example/rms/shared/platform/logging"
	"github.com/example/rms/shared/platform/observability"
	"github.com/example/rms/shared/platform/security"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type clientCredential struct {
	Secret   string
	TenantID string
	Roles    []string
}

type authService struct {
	logger   *slog.Logger
	registry *observability.Registry
	limiter  *security.Limiter
	signer   *security.JWTSigner
	verifier *security.JWTVerifier
	clients  map[string]clientCredential
}

type tokenRequest struct {
	ClientID     string            `json:"client_id"`
	ClientSecret string            `json:"client_secret"`
	Subject      string            `json:"subject"`
	TenantID     string            `json:"tenant_id"`
	Roles        []string          `json:"roles"`
	Scopes       []string          `json:"scopes"`
	SessionID    string            `json:"session_id"`
	Metadata     map[string]string `json:"metadata"`
}

type tokenResponse struct {
	AccessToken string               `json:"access_token"`
	TokenType   string               `json:"token_type"`
	ExpiresIn   int64                `json:"expires_in"`
	Claims      security.JWTClaims   `json:"claims"`
}

type introspectRequest struct {
	Token string `json:"token"`
}

type introspectResponse struct {
	Active bool                 `json:"active"`
	Claims *security.JWTClaims   `json:"claims,omitempty"`
	Error  string               `json:"error,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loader := config.New("RMS")
	logger := logging.New("auth-service", loader.String("ENVIRONMENT", "development"))
	registry := observability.NewRegistry()
	limiter := security.NewLimiter(20, 50)

	secret := loader.String("AUTH_SECRET", "dev-auth-secret")
	issuer := loader.String("AUTH_ISSUER", "rms-auth")
	audience := loader.String("AUTH_AUDIENCE", "rms-platform")
	ttl := loader.Duration("AUTH_TTL", 15*time.Minute)

	service := &authService{
		logger:   logger,
		registry: registry,
		limiter:  limiter,
		signer:   security.NewJWTSigner([]byte(secret), issuer, audience, ttl),
		verifier: security.NewJWTVerifier([]byte(secret), issuer, audience),
		clients:  parseClients(loader.String("AUTH_CLIENTS", "trading-platform:dev-secret:institutional:trader,allocator")),
	}

	httpServer := &http.Server{
		Addr:         loader.String("HTTP_ADDR", ":8081"),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Handler: platformhttp.Chain(
			service.routes(),
			platformhttp.Tracing,
			platformhttp.RequestID,
			platformhttp.Logging(logger),
			platformhttp.SecurityHeaders,
			platformhttp.Recovery(logger),
			platformhttp.RateLimit(limiter, func(r *http.Request) string {
				if clientID := strings.TrimSpace(r.Header.Get("X-Client-Id")); clientID != "" {
					return clientID
				}
				return r.RemoteAddr
			}),
		),
	}

	go func() {
		logger.Info("starting auth http server", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("auth http server stopped", "error", err)
		}
	}()

	grpcAddr := loader.String("GRPC_ADDR", ":50061")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to open grpc listener", "error", err)
		os.Exit(1)
	}
	grpcServer := grpcx.NewServer(grpcx.ServerConfig{
		ServiceName: "auth-service",
		Logger:      logger,
		Metrics:     registry,
		Limiter:     limiter,
	})
	healthServer := health.NewServer()
	healthServer.SetServingStatus("auth-service", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("starting auth grpc server", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("auth grpc server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
	logger.Info("auth service stopped")
}

func (s *authService) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"service": "auth-service",
			"status":  "ok",
		})
	})
	mux.HandleFunc("/metrics", platformhttp.MetricsHandler(s.registry))
	mux.HandleFunc("/v1/token", s.issueToken)
	mux.HandleFunc("/v1/introspect", s.introspectToken)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"issuer":                s.signer.Issuer,
			"token_endpoint":        "/v1/token",
			"introspection_endpoint": "/v1/introspect",
			"jwks_uri":               "/.well-known/jwks.json",
		})
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		platformhttp.JSON(w, http.StatusOK, map[string]any{
			"keys": []any{},
		})
	})
	return mux
}

func (s *authService) issueToken(w http.ResponseWriter, r *http.Request) {
	s.registry.Counter("rms_auth_token_requests_total", "token issuance requests").Inc()
	if r.Method != http.MethodPost {
		platformhttp.ErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req tokenRequest
	if err := platformhttp.DecodeJSON(r, &req); err != nil {
		platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ClientID == "" || req.ClientSecret == "" {
		platformhttp.ErrorJSON(w, http.StatusBadRequest, "client_id and client_secret are required")
		return
	}

	credential, ok := s.clients[req.ClientID]
	if !ok || credential.Secret != req.ClientSecret {
		s.registry.Counter("rms_auth_token_rejections_total", "token issuance rejections").Inc()
		s.logger.Warn("token request rejected", "client_id", req.ClientID, "remote_addr", r.RemoteAddr)
		platformhttp.ErrorJSON(w, http.StatusUnauthorized, "invalid client credentials")
		return
	}

	claims := security.JWTClaims{
		Subject:   firstNonEmpty(req.Subject, req.ClientID),
		ClientID:  req.ClientID,
		TenantID:  firstNonEmpty(req.TenantID, credential.TenantID),
		Roles:     mergeStrings(credential.Roles, req.Roles),
		Scopes:    req.Scopes,
		SessionID: req.SessionID,
		Metadata:  req.Metadata,
	}
	token, err := s.signer.Sign(claims)
	if err != nil {
		platformhttp.ErrorJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.logger.Info("token issued", "client_id", req.ClientID, "tenant_id", claims.TenantID, "roles", strings.Join(claims.Roles, ","))

	parsed, _ := s.verifier.Verify(token)
	var responseClaims security.JWTClaims
	if parsed != nil {
		responseClaims = *parsed
	} else {
		responseClaims = claims
	}
	response := tokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(s.signer.TTL.Seconds()),
		Claims:      responseClaims,
	}
	platformhttp.JSON(w, http.StatusOK, response)
}

func (s *authService) introspectToken(w http.ResponseWriter, r *http.Request) {
	s.registry.Counter("rms_auth_introspection_total", "token introspection requests").Inc()
	if r.Method != http.MethodPost {
		platformhttp.ErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req introspectRequest
	if err := platformhttp.DecodeJSON(r, &req); err != nil {
		platformhttp.ErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	token := security.ParseBearer(firstNonEmpty(req.Token, r.Header.Get("Authorization")))
	if token == "" {
		platformhttp.ErrorJSON(w, http.StatusBadRequest, "token is required")
		return
	}

	claims, err := s.verifier.Verify(token)
	if err != nil {
		s.logger.Warn("token introspection failed", "remote_addr", r.RemoteAddr, "error", err.Error())
		platformhttp.JSON(w, http.StatusOK, introspectResponse{
			Active: false,
			Error:  err.Error(),
		})
		return
	}
	s.logger.Info("token introspection succeeded", "subject", claims.Subject, "tenant_id", claims.TenantID)

	platformhttp.JSON(w, http.StatusOK, introspectResponse{
		Active: true,
		Claims: claims,
	})
}

func parseClients(raw string) map[string]clientCredential {
	clients := map[string]clientCredential{}
	for _, entry := range strings.Split(raw, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) < 2 {
			continue
		}
		clientID := strings.TrimSpace(parts[0])
		secret := strings.TrimSpace(parts[1])
		tenantID := ""
		rolesPart := ""
		if len(parts) > 2 {
			tenantID = strings.TrimSpace(parts[2])
		}
		if len(parts) > 3 {
			rolesPart = strings.Join(parts[3:], ":")
		}
		clients[clientID] = clientCredential{
			Secret:   secret,
			TenantID: tenantID,
			Roles:    splitCSV(rolesPart),
		}
	}
	return clients
}

func splitCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func mergeStrings(base, extra []string) []string {
	if len(extra) == 0 {
		return append([]string(nil), base...)
	}
	seen := map[string]struct{}{}
	merged := make([]string, 0, len(base)+len(extra))
	for _, value := range base {
		key := strings.ToLower(strings.TrimSpace(value))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, value)
	}
	for _, value := range extra {
		key := strings.ToLower(strings.TrimSpace(value))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, value)
	}
	return merged
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
