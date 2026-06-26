import { useState, useCallback, useRef } from 'react'
import { ActivityMap } from './components/ActivityMap'
import { StatsHud } from './components/StatsHud'

const WS_URL = 'ws://localhost:9001/ws'

/**
 * App
 *
 * Thin orchestration layer. Owns the counters that feed the HUD and
 * passes the WebSocket URL down to ActivityMap.
 *
 * Swap WS_URL to point at your real stream.
 */
export default function App() {
  const [activePings, setActivePings] = useState(0)
  const [totalReceived, setTotalReceived] = useState(0)
  const [connectionState, setConnectionState] = useState('connecting')

  // Tick the HUD counters at a comfortable rate (not every RAF frame)
  const hudIntervalRef = useRef(null)
  const pingManagerRef = useRef(null)

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
    setTotalReceived((n) => n + 1)
    setConnectionState('open')
  }, [])

  return (
    <div style={{ width: '100vw', height: '100vh', position: 'relative', background: 'var(--bg)' }}>
      <ActivityMap
        wsUrl={WS_URL}
        lifetimeMs={4000}
        maxPings={2500}
        onManagerReady={onMapReady}
        onFeatureReceived={handleFeatureReceived}
        onConnectionState={setConnectionState}
      />

      <StatsHud
        activePings={activePings}
        totalReceived={totalReceived}
        connectionState={connectionState}
        wsUrl={WS_URL}
      />
    </div>
  )
}
