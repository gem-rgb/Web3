package redisx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is a minimal RESP client with single-connection pipelining support.
// It is intentionally lightweight so the server tree does not depend on a
// third-party Redis driver.
type Client struct {
	addr        string
	username    string
	password    string
	db          int
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// Option mutates a Client before first use.
type Option func(*Client)

// WithCredentials configures Redis auth values.
func WithCredentials(username, password string) Option {
	return func(c *Client) {
		c.username = strings.TrimSpace(username)
		c.password = strings.TrimSpace(password)
	}
}

// WithDB selects a database index after connection.
func WithDB(db int) Option {
	return func(c *Client) {
		c.db = db
	}
}

// WithTimeouts configures dial/read/write deadlines.
func WithTimeouts(dial, read, write time.Duration) Option {
	return func(c *Client) {
		if dial > 0 {
			c.dialTimeout = dial
		}
		if read > 0 {
			c.readTimeout = read
		}
		if write > 0 {
			c.writeTimeout = write
		}
	}
}

// New returns a new Redis client.
func New(addr string, opts ...Option) *Client {
	client := &Client{
		addr:         strings.TrimSpace(addr),
		dialTimeout:  250 * time.Millisecond,
		readTimeout:  250 * time.Millisecond,
		writeTimeout: 250 * time.Millisecond,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(client)
		}
	}
	return client
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.reader = nil
	c.writer = nil
	return err
}

// Do executes a single Redis command and returns the decoded reply.
func (c *Client) Do(ctx context.Context, args ...any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.connectLocked(ctx); err != nil {
		return nil, err
	}
	if err := c.setDeadlineLocked(ctx, c.writeTimeout); err != nil {
		return nil, err
	}
	if err := writeCommand(c.writer, args...); err != nil {
		_ = c.resetLocked()
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		_ = c.resetLocked()
		return nil, err
	}
	if err := c.setDeadlineLocked(ctx, c.readTimeout); err != nil {
		return nil, err
	}
	reply, err := readReply(c.reader)
	if err != nil {
		_ = c.resetLocked()
		return nil, err
	}
	return reply, nil
}

// Pipeline batches commands onto the same connection before reading replies.
func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{client: c}
}

// Ping verifies the connection is healthy.
func (c *Client) Ping(ctx context.Context) error {
	reply, err := c.Do(ctx, "PING")
	if err != nil {
		return err
	}
	if s, ok := reply.(string); ok && strings.EqualFold(s, "PONG") {
		return nil
	}
	return fmt.Errorf("unexpected ping reply: %#v", reply)
}

// Get returns the string value for a key.
func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	reply, err := c.Do(ctx, "GET", key)
	if err != nil {
		return "", false, err
	}
	if reply == nil {
		return "", false, nil
	}
	value, ok := reply.(string)
	if !ok {
		return "", false, fmt.Errorf("unexpected GET reply %T", reply)
	}
	return value, true, nil
}

// MGet returns multiple string values in one round trip.
func (c *Client) MGet(ctx context.Context, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys)+1)
	args = append(args, "MGET")
	for _, key := range keys {
		args = append(args, key)
	}
	reply, err := c.Do(ctx, args...)
	if err != nil {
		return nil, err
	}
	values, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected MGET reply %T", reply)
	}
	result := make([]string, 0, len(values))
	for _, item := range values {
		if item == nil {
			result = append(result, "")
			continue
		}
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected MGET item %T", item)
		}
		result = append(result, text)
	}
	return result, nil
}

// Set stores a string value with an optional TTL.
func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	args := []any{"SET", key, value}
	if ttl > 0 {
		args = append(args, "PX", int64(ttl/time.Millisecond))
	}
	reply, err := c.Do(ctx, args...)
	if err != nil {
		return err
	}
	if text, ok := reply.(string); ok && strings.EqualFold(text, "OK") {
		return nil
	}
	return fmt.Errorf("unexpected SET reply: %#v", reply)
}

// SetNX stores a value only when the key is missing.
func (c *Client) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	args := []any{"SET", key, value, "NX"}
	if ttl > 0 {
		args = append(args, "PX", int64(ttl/time.Millisecond))
	}
	reply, err := c.Do(ctx, args...)
	if err != nil {
		return false, err
	}
	if reply == nil {
		return false, nil
	}
	text, ok := reply.(string)
	if !ok {
		return false, fmt.Errorf("unexpected SET NX reply: %#v", reply)
	}
	return strings.EqualFold(text, "OK"), nil
}

// Incr increments an integer key.
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	reply, err := c.Do(ctx, "INCR", key)
	if err != nil {
		return 0, err
	}
	switch value := reply.(type) {
	case int64:
		return value, nil
	case string:
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil {
			return 0, parseErr
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected INCR reply %T", reply)
	}
}

// Expire sets a TTL on a key.
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	reply, err := c.Do(ctx, "EXPIRE", key, int64(ttl/time.Second))
	if err != nil {
		return false, err
	}
	switch value := reply.(type) {
	case int64:
		return value == 1, nil
	case string:
		return value == "1", nil
	default:
		return false, fmt.Errorf("unexpected EXPIRE reply %T", reply)
	}
}

// Del removes keys and returns the number deleted.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	args := make([]any, 0, len(keys)+1)
	args = append(args, "DEL")
	for _, key := range keys {
		args = append(args, key)
	}
	reply, err := c.Do(ctx, args...)
	if err != nil {
		return 0, err
	}
	switch value := reply.(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected DEL reply %T", reply)
	}
}

// HSet writes a hash map.
func (c *Client) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	args := []any{"HSET", key}
	for field, value := range fields {
		args = append(args, field, value)
	}
	reply, err := c.Do(ctx, args...)
	if err != nil {
		return 0, err
	}
	switch value := reply.(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected HSET reply %T", reply)
	}
}

// HGetAll returns all hash fields as a string map.
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	reply, err := c.Do(ctx, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	values, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected HGETALL reply %T", reply)
	}
	result := make(map[string]string, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		field, ok := values[i].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected HGETALL field %T", values[i])
		}
		value, ok := values[i+1].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected HGETALL value %T", values[i+1])
		}
		result[field] = value
	}
	return result, nil
}

// Exists returns true when all provided keys exist.
func (c *Client) Exists(ctx context.Context, keys ...string) (bool, error) {
	args := make([]any, 0, len(keys)+1)
	args = append(args, "EXISTS")
	for _, key := range keys {
		args = append(args, key)
	}
	reply, err := c.Do(ctx, args...)
	if err != nil {
		return false, err
	}
	switch value := reply.(type) {
	case int64:
		return value > 0, nil
	case string:
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil {
			return false, parseErr
		}
		return parsed > 0, nil
	default:
		return false, fmt.Errorf("unexpected EXISTS reply %T", reply)
	}
}

// Publish emits a message to a Redis pub/sub channel.
func (c *Client) Publish(ctx context.Context, channel, message string) (int64, error) {
	reply, err := c.Do(ctx, "PUBLISH", channel, message)
	if err != nil {
		return 0, err
	}
	switch value := reply.(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected PUBLISH reply %T", reply)
	}
}

// Pipeline batches multiple commands over the same connection.
type Pipeline struct {
	client *Client
	cmds   [][]any
}

// Add appends a command to the pipeline.
func (p *Pipeline) Add(args ...any) {
	if p == nil || len(args) == 0 {
		return
	}
	p.cmds = append(p.cmds, append([]any(nil), args...))
}

// Exec writes all commands before reading any replies.
func (p *Pipeline) Exec(ctx context.Context) ([]any, error) {
	if p == nil || p.client == nil {
		return nil, errors.New("pipeline is not configured")
	}
	p.client.mu.Lock()
	defer p.client.mu.Unlock()

	if err := p.client.connectLocked(ctx); err != nil {
		return nil, err
	}
	if err := p.client.setDeadlineLocked(ctx, p.client.writeTimeout); err != nil {
		return nil, err
	}
	for _, cmd := range p.cmds {
		if err := writeCommand(p.client.writer, cmd...); err != nil {
			_ = p.client.resetLocked()
			return nil, err
		}
	}
	if err := p.client.writer.Flush(); err != nil {
		_ = p.client.resetLocked()
		return nil, err
	}
	if err := p.client.setDeadlineLocked(ctx, p.client.readTimeout); err != nil {
		return nil, err
	}
	results := make([]any, 0, len(p.cmds))
	for range p.cmds {
		reply, err := readReply(p.client.reader)
		if err != nil {
			_ = p.client.resetLocked()
			return nil, err
		}
		results = append(results, reply)
	}
	return results, nil
}

func (c *Client) connectLocked(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	if c.password != "" {
		if err := c.setDeadlineLocked(ctx, c.writeTimeout); err != nil {
			_ = c.resetLocked()
			return err
		}
		if c.username != "" {
			if err := writeCommand(c.writer, "AUTH", c.username, c.password); err != nil {
				_ = c.resetLocked()
				return err
			}
		} else {
			if err := writeCommand(c.writer, "AUTH", c.password); err != nil {
				_ = c.resetLocked()
				return err
			}
		}
		if err := c.writer.Flush(); err != nil {
			_ = c.resetLocked()
			return err
		}
		if err := c.setDeadlineLocked(ctx, c.readTimeout); err != nil {
			_ = c.resetLocked()
			return err
		}
		if _, err := readReply(c.reader); err != nil {
			_ = c.resetLocked()
			return err
		}
	}
	if c.db > 0 {
		if err := c.setDeadlineLocked(ctx, c.writeTimeout); err != nil {
			_ = c.resetLocked()
			return err
		}
		if err := writeCommand(c.writer, "SELECT", c.db); err != nil {
			_ = c.resetLocked()
			return err
		}
		if err := c.writer.Flush(); err != nil {
			_ = c.resetLocked()
			return err
		}
		if err := c.setDeadlineLocked(ctx, c.readTimeout); err != nil {
			_ = c.resetLocked()
			return err
		}
		if _, err := readReply(c.reader); err != nil {
			_ = c.resetLocked()
			return err
		}
	}
	return nil
}

func (c *Client) resetLocked() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.reader = nil
	c.writer = nil
	return err
}

func (c *Client) setDeadlineLocked(ctx context.Context, fallback time.Duration) error {
	if c.conn == nil {
		return errors.New("redis connection is not established")
	}
	if deadline, ok := ctx.Deadline(); ok {
		return c.conn.SetDeadline(deadline)
	}
	if fallback <= 0 {
		return c.conn.SetDeadline(time.Time{})
	}
	return c.conn.SetDeadline(time.Now().Add(fallback))
}

func writeCommand(w *bufio.Writer, args ...any) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		payload := encodeArg(arg)
		if _, err := fmt.Fprintf(w, "$%d\r\n", len(payload)); err != nil {
			return err
		}
		if _, err := w.Write(payload); err != nil {
			return err
		}
		if _, err := w.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return nil
}

func encodeArg(arg any) []byte {
	switch value := arg.(type) {
	case []byte:
		return value
	case string:
		return []byte(value)
	case fmt.Stringer:
		return []byte(value.String())
	default:
		return []byte(fmt.Sprint(value))
	}
}

func readReply(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '+':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		return string(line), nil
	case '-':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(line))
	case ':':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, err
		}
		return value, nil
	case '$':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, nil
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		return string(payload[:length]), nil
	case '*':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, nil
		}
		values := make([]any, 0, count)
		for i := 0; i < count; i++ {
			item, err := readReply(r)
			if err != nil {
				return nil, err
			}
			values = append(values, item)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected redis reply prefix %q", prefix)
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("malformed redis line ending")
	}
	return line[:len(line)-2], nil
}
