import { useEffect, useRef, useCallback } from 'react'
import maplibregl from 'maplibre-gl'
import { PingManager } from '../utils/PingManager'
import { useActivityStream } from '../hooks/useActivityStream'
import styles from './ActivityMap.module.css'

/**
 * ActivityMap
 *
 * Renders a full-viewport MapLibre map in dark mode and overlays live
 * GitHub activity pings streamed from a WebSocket.
 *
 * Rendering approach:
 *   - One GeoJSON source (`activity-pings`) is registered on map load.
 *   - Two layers sit on top:
 *       • `pings-ring` — an expanding translucent ring that fades out
 *       • `pings-dot`  — a solid circle for the hit point
 *   - A requestAnimationFrame loop calls PingManager.tick() every frame,
 *     calls source.setData() with the fresh FeatureCollection, and re-requests.
 *   - Pings carry an `age` property (0→1) used in style expressions to
 *     drive size and opacity interpolation entirely on the GPU — no React
 *     re-renders happen during animation.
 *
 * @param {object}   props
 * @param {string}   props.wsUrl                WebSocket URL
 * @param {string}   [props.styleUrl]           MapLibre style URL
 * @param {number}   [props.lifetimeMs=4000]    Ping lifetime in ms
 * @param {number}   [props.maxPings=500]       Rolling window cap
 * @param {Function} [props.onManagerReady]     (manager) => void
 * @param {Function} [props.onFeatureReceived]  () => void
 * @param {Function} [props.onConnectionState]  (state) => void
 */
export function ActivityMap({
  wsUrl,
  styleUrl = 'https://tiles.openfreemap.org/styles/dark',
  lifetimeMs = 4000,
  maxPings = 500,
  onManagerReady,
  onFeatureReceived,
  onConnectionState,
}) {
  const containerRef = useRef(null)
  const mapRef = useRef(null)
  const managerRef = useRef(null)
  const rafRef = useRef(null)
  const mapReadyRef = useRef(false)

  // ── Initialise map ──────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return

    const manager = new PingManager({ maxPings, lifetimeMs })
    managerRef.current = manager
    onManagerReady?.(manager)

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: styleUrl,
      center: [15, 30],
      zoom: 3.6,
      minZoom: 0.5,
      maxZoom: 12,
      attributionControl: false,
      pitchWithRotate: false,
    })

    mapRef.current = map

    map.on('load', () => {
      map.getStyle().layers
          .filter(l => l.type === 'symbol' && (
              l.id.includes('label') ||
              l.id.includes('region') ||
              l.id.includes('state')

          ))
          .forEach(l => map.setLayerZoomRange(l.id, 5, 24))
      map.getStyle().layers.filter(l => l.id.includes('place')  ).forEach(l => map.setLayerZoomRange(l.id, 4, 24))

      // ── Source ────────────────────────────────────────────────────────────
      map.addSource('activity-pings', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      })

      // ── Layer: expanding ring ─────────────────────────────────────────────
      map.addLayer({
        id: 'pings-ring',
        type: 'circle',
        source: 'activity-pings',
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['get', 'age'],
            0, 4,
            1, 30,
          ],
          'circle-color': 'transparent',
          'circle-stroke-width': [
            'interpolate', ['linear'], ['get', 'age'],
            0, 3,
            0.5, 1.5,
            1, 0,
          ],
          'circle-stroke-color': '#7c6aff',
          'circle-stroke-opacity': [
            'interpolate', ['linear'], ['get', 'age'],
            0, 0.9,
            0.5, 0.4,
            1, 0,
          ],
          'circle-pitch-alignment': 'map',
        },
      })

      // ── Layer: solid core dot ─────────────────────────────────────────────
      map.addLayer({
        id: 'pings-dot',
        type: 'circle',
        source: 'activity-pings',
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['get', 'age'],
            0, 5,
            0.25, 3.5,
            1, 2,
          ],
          'circle-color': '#b0a4ff',
          'circle-opacity': [
            'interpolate', ['linear'], ['get', 'age'],
            0, 1,
            0.45, 0.75,
            1, 0,
          ],
          'circle-blur': 0.25,
          'circle-pitch-alignment': 'map',
        },
      })

      mapReadyRef.current = true
      startLoop()
    })

    return () => {
      stopLoop()
      mapReadyRef.current = false
      map.remove()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // ── RAF animation loop ──────────────────────────────────────────────────
  const startLoop = useCallback(() => {
    const frame = () => {
      if (mapReadyRef.current && mapRef.current) {
        const source = mapRef.current.getSource('activity-pings')
        if (source) {
          source.setData(managerRef.current.tick())
        }
      }
      rafRef.current = requestAnimationFrame(frame)
    }
    rafRef.current = requestAnimationFrame(frame)
  }, [])

  const stopLoop = useCallback(() => {
    if (rafRef.current) {
      cancelAnimationFrame(rafRef.current)
      rafRef.current = null
    }
  }, [])

  // ── Consume WebSocket stream ────────────────────────────────────────────
  const handleFeature = useCallback((feature) => {
    managerRef.current?.add(feature)
    onFeatureReceived?.()
  }, [onFeatureReceived])

  useActivityStream(wsUrl, handleFeature, {
    onStateChange: onConnectionState,
  })

  return <div ref={containerRef} className={styles.map} />
}
