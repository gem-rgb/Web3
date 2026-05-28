# Risk Evaluation Service

This service is responsible for evaluating incoming orders against risk rules in real-time.

## Responsibilities

- Receive order requests via gRPC/REST
- Evaluate orders against configured risk rules
- Publish order events to Kafka for downstream processing
- Return approval/rejection decisions with low latency
- Cache frequently accessed account and instrument data

## Key Components

1. **Order Intake Layer** - gRPC and REST endpoints for receiving orders
2. **Rule Evaluation Engine** - Applies risk rules to incoming orders
3. **Risk Rule Repository** - Fetches active rules from database/cache
4. **Account Cache Layer** - Redis cache for account information
5. **Instrument Cache Layer** - Redis cache for symbol metadata
6. **Kafka Producer** - Publishes order events to risk-evaluated topic
7. **Metrics Collector** - Prometheus metrics for latency and throughput

## Configuration

See `config.yaml` for service configuration.

## Dependencies

- PostgreSQL (for rule persistence)
- Redis (for caching)
- Kafka (for event streaming)
- Prometheus client (for metrics)

## Running the Service

```bash
go run main.go
```

## API

### gRPC Service

```protobuf
service RiskEvaluationService {
  rpc EvaluateOrder (OrderRequest) returns (OrderResponse);
  rpc BatchEvaluateOrders (BatchOrderRequest) returns (BatchOrderResponse);
}
```

### REST Endpoints

- `POST /v1/orders/evaluate` - Evaluate a single order
- `POST /v1/orders/batch-evaluate` - Evaluate multiple orders
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics