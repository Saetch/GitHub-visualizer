import { useState, useEffect } from 'react'
import styles from './StatsHud.module.css'

/**
 * StatsHud
 *
 * Minimal status overlay. Accepts a `stats` object from the parent
 * and renders it without touching the map.
 *
 * @param {object} props
 * @param {number} props.activePings   Current ping count on screen
 * @param {number} props.totalReceived Lifetime event count
 * @param {'connecting'|'open'|'closed'|'error'} props.connectionState
 * @param {string} [props.wsUrl]
 */
export function StatsHud({ activePings, totalReceived, connectionState, wsUrl }) {
  const [elapsed, setElapsed] = useState(0)

  useEffect(() => {
    const t = setInterval(() => setElapsed((e) => e + 1), 1000)
    return () => clearInterval(t)
  }, [])

  const fmt = (n) => n.toLocaleString()

  const statusDot = {
    connecting: styles.dotYellow,
    open: styles.dotGreen,
    closed: styles.dotRed,
    error: styles.dotRed,
  }[connectionState] ?? styles.dotGrey

  return (
    <div className={styles.hud}>
      {/* Title */}
      <div className={styles.title}>
        <span className={styles.titleAccent}>GH</span> Activity
      </div>

      {/* Connection pill */}
      <div className={styles.row}>
        <span className={`${styles.dot} ${statusDot}`} />
        <span className={styles.label}>{connectionState}</span>
      </div>

      {/* Metrics */}
      <div className={styles.metric}>
        <span className={styles.value}>{fmt(activePings)}</span>
        <span className={styles.unit}>on screen</span>
      </div>
      <div className={styles.metric}>
        <span className={styles.value}>{fmt(totalReceived)}</span>
        <span className={styles.unit}>total events</span>
      </div>
      <div className={styles.metric}>
        <span className={styles.value}>{fmt(elapsed)}s</span>
        <span className={styles.unit}>elapsed</span>
      </div>

      {wsUrl && (
        <div className={styles.endpoint} title={wsUrl}>
          {wsUrl.replace(/^wss?:\/\//, '').slice(0, 28)}…
        </div>
      )}
    </div>
  )
}
