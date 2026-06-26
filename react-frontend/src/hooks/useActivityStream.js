import { useEffect, useRef, useCallback } from 'react'

/**
 * useActivityStream
 *
 * Connects to a WebSocket that streams GeoJSON Feature objects with
 * geometry.coordinates: [lon, lat]. Calls `onFeature` for each one.
 *
 * Handles:
 *   - NDJSON (newline-delimited) and single-object frames
 *   - FeatureCollection envelopes (emits each feature individually)
 *   - Auto-reconnect with exponential back-off
 *   - Optional `onStateChange` callback: 'connecting'|'open'|'closed'|'error'
 *
 * @param {string|null} url            WebSocket URL (null = skip)
 * @param {Function}    onFeature      (feature: GeoJSONFeature) => void
 * @param {object}      [opts]
 * @param {number}      [opts.maxRetries=10]
 * @param {number}      [opts.baseDelayMs=1000]
 * @param {Function}    [opts.onStateChange]  (state: string) => void
 */
export function useActivityStream(url, onFeature, opts = {}) {
  const { maxRetries = 10, baseDelayMs = 1000, onStateChange } = opts

  const onFeatureRef = useRef(onFeature)
  const onStateRef = useRef(onStateChange)
  useEffect(() => { onFeatureRef.current = onFeature }, [onFeature])
  useEffect(() => { onStateRef.current = onStateChange }, [onStateChange])

  const retryCountRef = useRef(0)
  const timerRef = useRef(null)
  const wsRef = useRef(null)

  const setState = (s) => onStateRef.current?.(s)

  const connect = useCallback(() => {
    if (!url) return
    setState('connecting')

    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      retryCountRef.current = 0
      setState('open')
    }

    ws.onmessage = (event) => {
      try {
        const lines = event.data.split('\n').filter(Boolean)
        for (const line of lines) {
          const parsed = JSON.parse(line)
          if (parsed.type === 'Feature') {
            onFeatureRef.current(parsed)
          } else if (parsed.type === 'FeatureCollection') {
            parsed.features?.forEach((f) => onFeatureRef.current(f))
          }
        }
      } catch {
        // drop malformed frames silently
      }
    }

    ws.onclose = (event) => {
      setState('closed')
      if (!event.wasClean) scheduleReconnect()
    }

    ws.onerror = () => {
      setState('error')
      ws.close()
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url])

  const scheduleReconnect = useCallback(() => {
    if (retryCountRef.current >= maxRetries) return
    const delay = baseDelayMs * 2 ** retryCountRef.current
    retryCountRef.current += 1
    timerRef.current = setTimeout(connect, delay)
  }, [connect, maxRetries, baseDelayMs])

  useEffect(() => {
    if (!url) return
    connect()
    return () => {
      clearTimeout(timerRef.current)
      wsRef.current?.close(1000, 'unmount')
    }
  }, [url, connect])
}
