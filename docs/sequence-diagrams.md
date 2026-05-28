# RMS Sequence Diagrams

This file preserves phase 1 sequence references.
The canonical phase 2 diagrams live in
[`phase-2-risk-pipeline.md`](phase-2-risk-pipeline.md).

## Pre-Trade Risk Evaluation

```mermaid
sequenceDiagram
  autonumber
  participant Trader as Trading System
  participant GW as API Gateway
  participant Risk as Risk Evaluation
  participant Alert as Alerting
  participant Audit as Audit Logging
  participant Kafka as Kafka

  Trader->>GW: SubmitOrder(order)
  GW->>Risk: EvaluateOrder(order)
  Risk->>Risk: Apply risk rules
  alt Approved
    Risk-->>GW: approved
    Risk->>Audit: LogEvent(order evaluated)
    Risk->>Kafka: Publish rms.orders.evaluated.v1
    GW-->>Trader: OrderResponse(approved)
  else Rejected
    Risk-->>GW: rejected + violations
    Risk->>Audit: LogEvent(order rejected)
    Risk->>Alert: SendAlert(rejection)
    Risk->>Kafka: Publish rms.orders.rejected.v1
    GW-->>Trader: OrderResponse(rejected)
  end
```

## Post-Trade Exposure Update

```mermaid
sequenceDiagram
  autonumber
  participant Exec as Execution Feed
  participant Pos as Position Tracking
  participant Exp as Exposure Aggregation
  participant Margin as Margin Engine
  participant GW as API Gateway

  Exec->>Pos: UpdatePosition(fill)
  Pos->>Exp: PositionUpdate(position)
  Exp->>Margin: ExposureChanged(account)
  Margin->>GW: Margin refresh on account lookup
  GW-->>Exec: Optional status / dashboard propagation
```

## Market Data Fan-Out

```mermaid
sequenceDiagram
  autonumber
  participant Feed as Market Data Feed
  participant MD as Market Data Consumer
  participant GW as API Gateway
  participant Dash as Admin Dashboard

  Feed->>MD: Normalized quote/trade ticks
  MD->>GW: Latest market data request
  GW-->>Dash: StreamMarketData updates
  MD-->>Dash: Subscription fan-out
```
