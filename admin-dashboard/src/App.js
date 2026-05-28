import React, { useEffect, useState } from 'react';

const summaryCards = [
  { label: 'Risk evaluations', value: '50,482', delta: '+8.4% in 1m', tone: 'emerald' },
  { label: 'Rejected orders', value: '218', delta: '4.3% reject rate', tone: 'amber' },
  { label: 'Open exposures', value: '$84.6M', delta: '12 accounts above 80%', tone: 'cyan' },
  { label: 'Kill switches', value: '2 armed', delta: '1 circuit breaker open', tone: 'red' },
];

const riskEvents = [
  {
    time: '09:30:01 ET',
    account: 'ACC-10492',
    symbol: 'NVDA',
    action: 'Rejected',
    reason: 'Max order size exceeded',
    severity: 'HIGH',
  },
  {
    time: '09:30:03 ET',
    account: 'ACC-55210',
    symbol: 'AAPL',
    action: 'Approved',
    reason: 'Pre-trade controls passed',
    severity: 'LOW',
  },
  {
    time: '09:30:04 ET',
    account: 'ACC-77011',
    symbol: 'TSLA',
    action: 'Rejected',
    reason: 'Wash-trade pattern detected',
    severity: 'CRITICAL',
  },
  {
    time: '09:30:06 ET',
    account: 'ACC-19002',
    symbol: 'MSFT',
    action: 'Approved',
    reason: 'Margin coverage sufficient',
    severity: 'LOW',
  },
];

const policyRows = [
  ['Max order size', '10,000 shares', 'Enabled'],
  ['Daily loss limit', '5% NAV', 'Enabled'],
  ['Leverage ceiling', '4.0x', 'Enabled'],
  ['Concentration cap', '25% of account', 'Enabled'],
  ['Frequency throttle', '240 orders/min', 'Enabled'],
  ['Market hours', '09:30-16:00 ET', 'Enabled'],
];

const healthRows = [
  { service: 'API Gateway', status: 'Healthy', latency: '2.1ms', region: 'us-east-1' },
  { service: 'Risk Evaluation', status: 'Healthy', latency: '4.8ms', region: 'us-east-1' },
  { service: 'Exposure Aggregation', status: 'Healthy', latency: '5.5ms', region: 'us-east-1' },
  { service: 'Market Data', status: 'Degraded', latency: '9.9ms', region: 'multi-feed' },
];

const pipeline = [
  'Order intake',
  'Rule engine',
  'Risk decision',
  'Kafka publish',
  'Exposure + margin',
  'Audit + alerts',
];

function App() {
  const [focusIndex, setFocusIndex] = useState(0);
  const [gatewayHealth, setGatewayHealth] = useState('unknown');
  const [gatewayMetrics, setGatewayMetrics] = useState({
    requests: 0,
    approved: 0,
    rejected: 0,
    throttled: 0,
  });

  useEffect(() => {
    const timer = setInterval(() => {
      setFocusIndex((index) => (index + 1) % riskEvents.length);
    }, 2200);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    let cancelled = false;

    const loadGatewayState = async () => {
      try {
        const [healthResponse, metricsResponse] = await Promise.all([
          fetch('http://localhost:8080/health'),
          fetch('http://localhost:8080/metrics'),
        ]);

        const healthJson = await healthResponse.json();
        const metricsText = await metricsResponse.text();
        const requests = parseMetric(metricsText, 'rms_gateway_requests_total');
        const approved = parseMetric(metricsText, 'rms_gateway_approved_total');
        const rejected = parseMetric(metricsText, 'rms_gateway_rejected_total');
        const throttled = parseMetric(metricsText, 'rms_gateway_throttled_total');

        if (!cancelled) {
          setGatewayHealth(healthJson.status || 'unknown');
          setGatewayMetrics({ requests, approved, rejected, throttled });
        }
      } catch (error) {
        if (!cancelled) {
          setGatewayHealth('offline');
        }
      }
    };

    loadGatewayState();
    const timer = setInterval(loadGatewayState, 5000);

    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  const focus = riskEvents[focusIndex];
  const now = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

  return (
    <div className="dashboard-shell">
      <div className="backdrop backdrop-a" />
      <div className="backdrop backdrop-b" />
      <main className="dashboard">
        <header className="topbar">
          <div>
            <p className="eyebrow">Institutional RMS</p>
            <h1>Control tower for live risk, exposure, and audit operations.</h1>
          </div>
          <div className="status-chip">
            <span className="status-dot" />
            Live feed synchronized
            <strong>{now}</strong>
          </div>
        </header>

        <section className="gateway-strip">
          <div className={`gateway-badge gateway-${gatewayHealth}`}>
            Gateway {gatewayHealth}
          </div>
          <span>{gatewayMetrics.requests} order evaluations</span>
          <span>{gatewayMetrics.approved} approved</span>
          <span>{gatewayMetrics.rejected} rejected</span>
          <span>{gatewayMetrics.throttled} throttled</span>
        </section>

        <section className="summary-grid">
          {summaryCards.map((card) => (
            <article key={card.label} className={`metric-card metric-${card.tone}`}>
              <p>{card.label}</p>
              <h2>{card.value}</h2>
              <span>{card.delta}</span>
            </article>
          ))}
        </section>

        <section className="pipeline-card">
          <div className="pipeline-header">
            <div>
              <p className="eyebrow">Processing path</p>
              <h3>How the platform moves an order through institutional controls</h3>
            </div>
            <span className="badge">Kafka + gRPC</span>
          </div>
          <div className="pipeline">
            {pipeline.map((step, index) => (
              <div key={step} className="pipeline-step">
                <span className="step-index">{index + 1}</span>
                <span>{step}</span>
              </div>
            ))}
          </div>
        </section>

        <section className="content-grid">
          <article className="panel panel-large">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Risk tape</p>
                <h3>Orders flowing through the pre-trade gate</h3>
              </div>
              <span className={`pill pill-${focus.severity.toLowerCase()}`}>{focus.severity}</span>
            </div>
            <div className="focus-card">
              <div>
                <label>Focused event</label>
                <strong>{focus.account}</strong>
              </div>
              <div>
                <label>Instrument</label>
                <strong>{focus.symbol}</strong>
              </div>
              <div>
                <label>Decision</label>
                <strong>{focus.action}</strong>
              </div>
              <div>
                <label>Reason</label>
                <strong>{focus.reason}</strong>
              </div>
            </div>
            <div className="event-list">
              {riskEvents.map((event, index) => (
                <div key={`${event.account}-${event.symbol}`} className={`event-row ${index === focusIndex ? 'active' : ''}`}>
                  <div className="event-time">{event.time}</div>
                  <div>
                    <strong>{event.account}</strong>
                    <span>{event.symbol}</span>
                  </div>
                  <div>
                    <strong>{event.action}</strong>
                    <span>{event.reason}</span>
                  </div>
                  <span className={`pill pill-${event.severity.toLowerCase()}`}>{event.severity}</span>
                </div>
              ))}
            </div>
          </article>

          <aside className="side-column">
            <article className="panel">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Policy matrix</p>
                  <h3>Configured controls</h3>
                </div>
              </div>
              <div className="policy-list">
                {policyRows.map(([label, value, status]) => (
                  <div key={label} className="policy-row">
                    <div>
                      <strong>{label}</strong>
                      <span>{value}</span>
                    </div>
                    <span className="policy-status">{status}</span>
                  </div>
                ))}
              </div>
            </article>

            <article className="panel">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Health</p>
                  <h3>Service posture</h3>
                </div>
              </div>
              <div className="health-list">
                {healthRows.map((row) => (
                  <div key={row.service} className="health-row">
                    <div>
                      <strong>{row.service}</strong>
                      <span>{row.region}</span>
                    </div>
                    <div className="health-meta">
                      <span className={`health-pill ${row.status === 'Healthy' ? 'healthy' : 'degraded'}`}>{row.status}</span>
                      <span>{row.latency}</span>
                    </div>
                  </div>
                ))}
              </div>
            </article>
          </aside>
        </section>
      </main>
    </div>
  );
}

export default App;

function parseMetric(body, metricName) {
  const match = body.match(new RegExp(`^${metricName}\\s+([0-9.]+)$`, 'm'));
  return match ? Number(match[1]) : 0;
}
