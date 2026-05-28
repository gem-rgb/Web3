package ruleengine

import (
	"context"
	"fmt"
	"sync"

	"github.com/example/rms/shared/platform/grpcx"
	pb "github.com/example/rms/shared/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client fetches rule snapshots from the rule engine service.
type Client struct {
	addr string

	mu   sync.Mutex
	conn *grpc.ClientConn
}

// NewClient returns a lazy rule engine gRPC client.
func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

// Close closes the cached gRPC connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// FetchSnapshot requests the active rules from the rule engine service and maps them into an immutable snapshot.
func (c *Client) FetchSnapshot(ctx context.Context, ruleType, tenantID, accountID string) (Snapshot, error) {
	if c == nil {
		return Snapshot{}, fmt.Errorf("rule engine client is nil")
	}
	conn, err := c.connection(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	req := &pb.GetActiveRulesRequest{
		RuleType:  ruleType,
		AccountId: accountID,
		TenantId:  tenantID,
	}
	resp := &pb.GetActiveRulesResponse{}
	if err := conn.Invoke(ctx, "/rms.rule.RuleEngineService/GetActiveRules", req, resp); err != nil {
		return Snapshot{}, err
	}
	return FromProto(resp.Rules, fmt.Sprintf("remote-%d", resp.Timestamp)), nil
}

// Reload asks the rule-engine service to refresh its catalog.
func (c *Client) Reload(ctx context.Context, tenantID, source string) error {
	if c == nil {
		return fmt.Errorf("rule engine client is nil")
	}
	conn, err := c.connection(ctx)
	if err != nil {
		return err
	}
	req := &pb.RuleReloadRequest{TenantID: tenantID, Source: source}
	resp := &pb.RuleReloadResponse{}
	return conn.Invoke(ctx, "/rms.rule.RuleEngineService/ReloadRules", req, resp)
}

func (c *Client) connection(ctx context.Context) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := grpc.DialContext(
		ctx,
		c.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcx.ClientUnaryInterceptor()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}
