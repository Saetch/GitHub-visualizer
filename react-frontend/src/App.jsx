import { useState, useCallback, useRef } from 'react'
import { ActivityMap } from './components/ActivityMap'
import { StatsHud } from './components/StatsHud'

const WS_URLS = [
  'ws://cluster.laptop/distr-ws/ws',
  'ws://localhost:9001/ws',
]

/**
 * App
 *
 * Thin orchestration layer. Owns the counters that feed the HUD and
 * passes the WebSocket URL down to ActivityMap.
 *
 * Tries cluster-laptop first, then falls back to localhost.
 */
export default function App() {
  const [activePings, setActivePings] = useState(0)
  const [totalReceived, setTotalReceived] = useState(0)
  const [connectionState, setConnectionState] = useState('connecting')
  const [wsUrlIndex, setWsUrlIndex] = useState(0)

  const wsUrl = WS_URLS[wsUrlIndex]

  // Tick the HUD counters at a comfortable rate (not every RAF frame)
  const hudIntervalRef = useRef(null)
  const pingManagerRef = useRef(null)
  const hasConnectedRef = useRef(false)

  // ActivityMap exposes its PingManager via an optional ref callback
  const onMapReady = useCallback((manager) => {
    pingManagerRef.current = manager

    // Poll for active ping count a few times per second
    clearInterval(hudIntervalRef.current)
    hudIntervalRef.current = setInterval(() => {
      setActivePings(manager.size)
    }, 250)
  }, [])

  const handleFeatureReceived = useCallback(() => {
    hasConnectedRef.current = true
    setTotalReceived((n) => n + 1)
    setConnectionState('open')
  }, [])

  const handleConnectionState = useCallback((state) => {
    setConnectionState(state)

    // If the preferred endpoint fails before we ever receive data,
    // fall back to the next URL.
    if (
      !hasConnectedRef.current &&
      wsUrlIndex < WS_URLS.length - 1 &&
      (state === 'closed' || state === 'error' || state === 'failed')
    ) {
      setWsUrlIndex((i) => i + 1)
      setConnectionState('connecting')
    }
  }, [wsUrlIndex])

  return (
    <div style={{ width: '100vw', height: '100vh', position: 'relative', background: 'var(--bg)' }}>
      <ActivityMap
        wsUrl={wsUrl}
        lifetimeMs={4000}
        maxPings={2500}
        onManagerReady={onMapReady}
        onFeatureReceived={handleFeatureReceived}
        onConnectionState={handleConnectionState}
      />

      <StatsHud
        activePings={activePings}
        totalReceived={totalReceived}
        connectionState={connectionState}
        wsUrl={wsUrl}
      />
    </div>
  )
}
