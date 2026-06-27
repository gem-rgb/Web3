import {
  healthRows,
  pipeline,
  policyRows,
  riskEvents,
  summaryCards,
  type GatewayHealth,
  type GatewayMetrics,
} from './dashboard-data'
import { useCurrentTime, useGatewayState, useRotatingIndex } from './dashboard-hooks'

function DashboardHeader({ currentTime }: { currentTime: string }) {
  return (
    <header className="topbar">
      <div>
        <p className="eyebrow">Institutional RMS</p>
        <h1>Control tower for live risk, exposure, and audit operations.</h1>
      </div>
      <div className="status-chip">
        <span className="status-dot" />
        Live feed synchronized
        <strong>{currentTime}</strong>
      </div>
    </header>
  )
}

function GatewayStrip({ health, metrics }: { health: GatewayHealth; metrics: GatewayMetrics }) {
  return (
    <section className="gateway-strip">
      <div className={`gateway-badge gateway-${health}`}>Gateway {health}</div>
      <span>
        <strong>{metrics.requests}</strong> order evaluations
      </span>
      <span>
        <strong>{metrics.approved}</strong> approved
      </span>
      <span>
        <strong>{metrics.rejected}</strong> rejected
      </span>
      <span>
        <strong>{metrics.throttled}</strong> throttled
      </span>
    </section>
  )
}

function SummaryGrid() {
  return (
    <section className="summary-grid">
      {summaryCards.map((card) => (
        <article key={card.label} className={`metric-card metric-${card.tone}`}>
          <p>{card.label}</p>
          <h2>{card.value}</h2>
          <span>{card.delta}</span>
        </article>
      ))}
    </section>
  )
}

function PipelineCard() {
  return (
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
  )
}

function RiskTapePanel({ focusIndex }: { focusIndex: number }) {
  const focus = riskEvents[focusIndex] ?? riskEvents[0]

  return (
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
          <div
            key={`${event.account}-${event.symbol}-${index}`}
            className={`event-row ${index === focusIndex ? 'active' : ''}`}
          >
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
  )
}

function PolicyPanel() {
  return (
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
  )
}

function HealthPanel() {
  return (
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
              <span className={`health-pill ${row.status === 'Healthy' ? 'healthy' : 'degraded'}`}>
                {row.status}
              </span>
              <span>{row.latency}</span>
            </div>
          </div>
        ))}
      </div>
    </article>
  )
}

function Dashboard() {
  const focusIndex = useRotatingIndex(riskEvents.length, 2200)
  const currentTime = useCurrentTime()
  const gatewayState = useGatewayState()

  return (
    <div className="dashboard-shell">
      <div className="backdrop backdrop-a" />
      <div className="backdrop backdrop-b" />
      <main className="dashboard">
        <DashboardHeader currentTime={currentTime} />
        <GatewayStrip health={gatewayState.health} metrics={gatewayState.metrics} />
        <SummaryGrid />
        <PipelineCard />
        <section className="content-grid">
          <RiskTapePanel focusIndex={focusIndex} />
          <aside className="side-column">
            <PolicyPanel />
            <HealthPanel />
          </aside>
        </section>
      </main>
    </div>
  )
}

export default Dashboard
