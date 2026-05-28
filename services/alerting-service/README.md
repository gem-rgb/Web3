# Alerting Service

This service is responsible for generating and sending alerts based on risk rule violations and system events.

## Responsibilities

- Receive alert events from various services (risk evaluation, margin calculation, etc.)
- Send alerts via multiple channels (email, SMS, Slack, PagerDuty, etc.)
- Store alerts for historical retrieval and audit
- Provide APIs for querying alerts by account, rule, or time range
- Stream real-time alerts to dashboards and monitoring systems

## Key Components

1. **Alert Intake Layer** - gRPC and Kafka consumers for receiving alert events
2. **Alert Processing Engine** - Formats alerts and determines routing
3. **Notification Channels** - Pluggable integrations with external notification services
4. **Alert Storage** - PostgreSQL for persistent alert storage
5. **Alert Cache Layer** - Redis cache for recent alerts
6. **Kafka Producer** - Publishes processed alerts to alert-streamed topic
7. **Metrics Collector** - Prometheus metrics for alert throughput and latency

## Configuration

See `config.yaml` for service configuration.

## Dependencies

- PostgreSQL (for alert persistence)
- Redis (for caching)
- Kafka (for event streaming)
- External notification services (SMTP, Twilio, Slack webhook, etc.)
- Prometheus client (for metrics)

## Running the Service

```bash
go run main.go
```

## API

### gRPC Service

```protobuf
service AlertingService {
  rpc SendAlert (Alert) returns (AlertResponse);
  rpc GetAlertsForAccount (AccountAlertRequest) returns (AccountAlertResponse);
  rpc GetAlertsForRule (RuleAlertRequest) returns (RuleAlertResponse);
  rpc StreamAlerts (AlertSubscriptionRequest) returns (stream Alert);
}
```

### REST Endpoints (if implemented)

- `POST /v1/alerts` - Send a new alert
- `GET /v1/alerts/account/{account_id}` - Get alerts for an account
- `GET /v1/alerts/rule/{rule_id}` - Get alerts for a rule
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics