export interface SummaryCard {
  label: string
  value: string
  delta: string
  tone: 'emerald' | 'amber' | 'cyan' | 'red'
}

export interface RiskEvent {
  time: string
  account: string
  symbol: string
  action: 'Approved' | 'Rejected'
  reason: string
  severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
}

export interface ServiceHealth {
  service: string
  status: 'Healthy' | 'Degraded' | 'Offline' | 'Unknown'
  latency: string
  region: string
}

export interface GatewayMetrics {
  requests: number
  approved: number
  rejected: number
  throttled: number
}

export type GatewayHealth = 'healthy' | 'degraded' | 'offline' | 'unknown'

export interface GatewayState {
  health: GatewayHealth
  metrics: GatewayMetrics
}

export const summaryCards: SummaryCard[] = [
  { label: 'Risk evaluations', value: '50,482', delta: '+8.4% in 1m', tone: 'emerald' },
  { label: 'Rejected orders', value: '218', delta: '4.3% reject rate', tone: 'amber' },
  { label: 'Open exposures', value: '$84.6M', delta: '12 accounts above 80%', tone: 'cyan' },
  { label: 'Kill switches', value: '2 armed', delta: '1 circuit breaker open', tone: 'red' },
]

export const riskEvents: RiskEvent[] = [
  {
    time: '09:30:01 ET',
    account: 'ACC-10482',
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
]

export const policyRows = [
  ['Max order size', '10,000 shares', 'Enabled'],
  ['Daily loss limit', '5% NAV', 'Enabled'],
  ['Leverage ceiling', '4.0x', 'Enabled'],
  ['Concentration cap', '25% of account', 'Enabled'],
  ['Frequency throttle', '240 orders/min', 'Enabled'],
  ['Market hours', '09:30-16:00 ET', 'Enabled'],
]

export const healthRows: ServiceHealth[] = [
  { service: 'API Gateway', status: 'Healthy', latency: '2.1ms', region: 'us-east-1' },
  { service: 'Risk Evaluation', status: 'Healthy', latency: '4.8ms', region: 'us-east-1' },
  { service: 'Exposure Aggregation', status: 'Healthy', latency: '5.5ms', region: 'us-east-1' },
  { service: 'Market Data', status: 'Degraded', latency: '9.9ms', region: 'multi-feed' },
]

export const pipeline = [
  'Order intake',
  'Rule engine',
  'Risk decision',
  'Kafka publish',
  'Exposure + margin',
  'Audit + alerts',
]

const emptyGatewayMetrics: GatewayMetrics = {
  requests: 0,
  approved: 0,
  rejected: 0,
  throttled: 0,
}

const metricNames = {
  requests: 'rms_gateway_requests_total',
  approved: 'rms_gateway_approved_total',
  rejected: 'rms_gateway_rejected_total',
  throttled: 'rms_gateway_throttled_total',
} as const

export function parseMetric(body: string, metricName: string): number {
  const regex = new RegExp(`^${metricName}\\s+([0-9.]+)$`, 'm')
  const match = body.match(regex)
  return match ? Number(match[1]) : 0
}

export function parseGatewayMetrics(metricsText: string): GatewayMetrics {
  return {
    requests: parseMetric(metricsText, metricNames.requests),
    approved: parseMetric(metricsText, metricNames.approved),
    rejected: parseMetric(metricsText, metricNames.rejected),
    throttled: parseMetric(metricsText, metricNames.throttled),
  }
}

export function createInitialGatewayState(): GatewayState {
  return {
    health: 'unknown',
    metrics: emptyGatewayMetrics,
  }
}

export async function fetchGatewayState(apiBase: string): Promise<GatewayState> {
  const [healthResponse, metricsResponse] = await Promise.all([
    fetch(`${apiBase}/health`),
    fetch(`${apiBase}/metrics`),
  ])

  if (!healthResponse.ok || !metricsResponse.ok) {
    throw new Error('Server returned error status')
  }

  const healthJson = (await healthResponse.json()) as { status?: string }
  const metricsText = await metricsResponse.text()

  return {
    health: healthJson.status === 'ok' ? 'healthy' : 'degraded',
    metrics: parseGatewayMetrics(metricsText),
  }
}
