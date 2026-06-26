/**
 * PingManager
 *
 * Maintains a rolling window of GeoJSON features for the map source.
 * Each ping gets a unique id, a timestamp, and an age (0 → 1 over its lifetime).
 * The manager is intentionally framework-agnostic — it just holds state and
 * exposes a method to flush a snapshot to a MapLibre GeoJSON source.
 *
 * Architecture rationale:
 *   - Keeping pings in a plain JS object (not React state) avoids triggering
 *     re-renders on every WebSocket message; MapLibre manages its own rendering.
 *   - Pings are pruned by age in the animation loop, not on insertion, so we
 *     only iterate the collection once per frame.
 */
export class PingManager {
  /**
   * @param {object} opts
   * @param {number} [opts.maxPings=500]       Hard cap on simultaneous pings
   * @param {number} [opts.lifetimeMs=4000]    How long each ping lives (ms)
   */
  constructor({ maxPings = 500, lifetimeMs = 4000 } = {}) {
    this.maxPings = maxPings
    this.lifetimeMs = lifetimeMs
    /** @type {Map<string, PingEntry>} */
    this.pings = new Map()
    this._counter = 0
  }

  /**
   * Add a new GeoJSON Feature.
   * @param {GeoJSONFeature} feature  Must have geometry.coordinates: [lon, lat]
   */
  add(feature) {
    if (this.pings.size >= this.maxPings) {
      // Evict the oldest entry
      const oldest = this.pings.keys().next().value
      this.pings.delete(oldest)
    }

    const id = `ping-${Date.now()}-${this._counter++}`
    this.pings.set(id, {
      id,
      coordinates: feature.geometry.coordinates,
      properties: feature.properties ?? {},
      createdAt: Date.now(),
    })
  }

  /**
   * Prune expired pings and return a fresh GeoJSON FeatureCollection snapshot.
   * Call this every animation frame.
   * @returns {GeoJSONFeatureCollection}
   */
  tick() {
    const now = Date.now()
    const features = []

    for (const [id, ping] of this.pings) {
      const age = (now - ping.createdAt) / this.lifetimeMs

      if (age >= 1) {
        this.pings.delete(id)
        continue
      }

      features.push({
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: ping.coordinates,
        },
        properties: {
          ...ping.properties,
          id,
          age,           // 0 (new) → 1 (dying) — drives opacity/radius in style
          opacity: 1 - age,
          radius: age,   // ring expands outward as ping ages
        },
      })
    }

    return { type: 'FeatureCollection', features }
  }

  /** How many pings are currently alive */
  get size() {
    return this.pings.size
  }

  clear() {
    this.pings.clear()
  }
}
