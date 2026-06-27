import { useEffect, useState } from 'react'

import {
  createInitialGatewayState,
  fetchGatewayState,
  type GatewayState,
} from './dashboard-data'

function formatCurrentTime(): string {
  return new Date().toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

export function useRotatingIndex(length: number, intervalMs: number): number {
  const [index, setIndex] = useState(0)

  useEffect(() => {
    if (length === 0) {
      return undefined
    }

    const timer = setInterval(() => {
      setIndex((current) => (current + 1) % length)
    }, intervalMs)

    return () => clearInterval(timer)
  }, [intervalMs, length])

  return length === 0 ? 0 : index % length
}

export function useCurrentTime(): string {
  const [currentTime, setCurrentTime] = useState('')

  useEffect(() => {
    const updateClock = () => {
      setCurrentTime(formatCurrentTime())
    }

    const timer = setInterval(updateClock, 1000)
    updateClock()

    return () => clearInterval(timer)
  }, [])

  return currentTime
}

export function useGatewayState(): GatewayState {
  const [gatewayState, setGatewayState] = useState<GatewayState>(() => createInitialGatewayState())

  useEffect(() => {
    let cancelled = false
    const apiBase = (import.meta.env.VITE_API_BASE_URL as string) || 'http://localhost:8080'

    const refreshGatewayState = async () => {
      try {
        const nextState = await fetchGatewayState(apiBase)

        if (!cancelled) {
          setGatewayState(nextState)
        }
      } catch {
        if (!cancelled) {
          setGatewayState((current) => ({
            ...current,
            health: 'offline',
          }))
        }
      }
    }

    refreshGatewayState()
    const timer = setInterval(refreshGatewayState, 5000)

    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [])

  return gatewayState
}
