# Market Data Consumer Service

This service is responsible for consuming real-time market data from external feeds (e.g., exchanges, data vendors) and making it available to other services in the RMS.

## Responsibilities

- Connect to market data feeds (WebSocket, FIX, etc.)
- Normalize and validate incoming market data
- Publish market data updates to Kafka for consumption by other services
- Cache latest market data in Redis for low-latency access
- Provide historical market data queries
- Handle market data feed failures and reconnections

## Key Components

1. **Market Data Feed Connectors** - Pluggable adapters for different data sources
2. **Market Data Normalization Engine** - Converts various feed formats to common format
3. **Data Validation Layer** - Checks for data quality and consistency
4. **Kafka Producer** - Publishes market data updates to market-data topic
5. **Redis Cache Layer** - Stores latest market data for quick access
6. **PostgreSQL Storage** - Persists historical market data for replay and analysis
7. **Metrics Collector** - Prometheus metrics for data latency and throughput

## Configuration

See `config.yaml` for service configuration.

## Dependencies

- Kafka (for event streaming)
- Redis (for caching)
- PostgreSQL (for historical data storage)
- External market data feed subscriptions
- Prometheus client (for metrics)

## Running the Service

```bash
go run main.go
```

## API

### gRPC Service

```protobuf
service MarketDataConsumerService {
  rpc SubscribeToMarketData (MarketDataSubscriptionRequest) returns (stream MarketDataUpdate);
  rpc GetLatestMarketData (MarketDataRequest) returns (MarketDataResponse);
  rpc GetMarketDataHistory (MarketDataHistoryRequest) returns (MarketDataHistoryResponse);
}
```

### REST Endpoints (if implemented)

- `GET /v1/marketdata/latest?symbols=AAPL,GOOG` - Get latest market data for symbols
- `GET /v1/marketdata/history?symbol=AAPL&start=123&end=456` - Get historical market data
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics