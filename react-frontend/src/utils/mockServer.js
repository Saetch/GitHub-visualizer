/**
 * MockActivityServer
 *
 * Drop-in WebSocket server mock for development.
 * Replaces a real WS URL with an in-browser EventTarget-based fake.
 *
 * Usage:
 *   import { installMockServer } from './utils/mockServer'
 *   installMockServer('ws://localhost:8080/events')
 *
 * After calling this, any `new WebSocket('ws://localhost:8080/events')`
 * will get synthetic GitHub activity pings scattered across the globe.
 *
 * Remove this file (and its import) when connecting to a real stream.
 */

const CITIES = [
  [-74.006, 40.7128],   // New York
  [-0.1276, 51.5074],   // London
  [139.6917, 35.6895],  // Tokyo
  [2.3522, 48.8566],    // Paris
  [151.2093, -33.8688], // Sydney
  [77.2090, 28.6139],   // Delhi
  [-43.1729, -22.9068], // Rio
  [-99.1332, 19.4326],  // Mexico City
  [37.6173, 55.7558],   // Moscow
  [116.4074, 39.9042],  // Beijing
  [18.0686, 59.3293],   // Stockholm
  [-46.6333, -23.5505], // São Paulo
  [103.8198, 1.3521],   // Singapore
  [28.9784, 41.0082],   // Istanbul
  [-87.6298, 41.8781],  // Chicago
  [12.4964, 41.9028],   // Rome
  [4.9041, 52.3676],    // Amsterdam
  [126.9780, 37.5665],  // Seoul
  [72.8777, 19.0760],   // Mumbai
  [30.3752, 59.9311],   // St Petersburg
]

const EVENT_TYPES = ['PushEvent', 'PullRequestEvent', 'IssuesEvent', 'WatchEvent', 'ForkEvent', 'CreateEvent']

function randomNear([lon, lat], jitter = 2) {
  return [
    lon + (Math.random() - 0.5) * jitter,
    Math.max(-85, Math.min(85, lat + (Math.random() - 0.5) * jitter)),
  ]
}

function makeFeature() {
  const city = CITIES[Math.floor(Math.random() * CITIES.length)]
  const [lon, lat] = randomNear(city)
  return JSON.stringify({
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [lon, lat] },
    properties: {
      type: EVENT_TYPES[Math.floor(Math.random() * EVENT_TYPES.length)],
      repo: `user/repo-${Math.floor(Math.random() * 9999)}`,
    },
  })
}

export function installMockServer(wsUrl) {
  const OriginalWebSocket = window.WebSocket

  window.WebSocket = class MockWebSocket extends EventTarget {
    constructor(url) {
      super()
      this.url = url
      this.readyState = 0 // CONNECTING

      if (url !== wsUrl) {
        // Pass through to real WebSocket for other URLs
        return new OriginalWebSocket(url)
      }

      // Simulate async connection
      setTimeout(() => {
        this.readyState = 1 // OPEN
        this.dispatchEvent(new Event('open'))
        this._startEmitting()
      }, 50)
    }

    _startEmitting() {
      this._interval = setInterval(() => {
        if (this.readyState !== 1) return
        // Emit 1-4 events per tick to simulate bursts
        const burst = Math.ceil(Math.random() * 4)
        for (let i = 0; i < burst; i++) {
          const msg = new MessageEvent('message', { data: makeFeature() })
          this.dispatchEvent(msg)
          if (this.onmessage) this.onmessage(msg)
        }
      }, 300 + Math.random() * 400)
    }

    close(code = 1000) {
      clearInterval(this._interval)
      this.readyState = 3 // CLOSED
      const ev = new CloseEvent('close', { wasClean: code === 1000, code })
      this.dispatchEvent(ev)
      if (this.onclose) this.onclose(ev)
    }

    // no-op — mock server doesn't need to receive messages
    send() {}

    // Proxy on* properties to addEventListener
    set onopen(fn) { this.addEventListener('open', fn) }
    set onclose(fn) { this.addEventListener('close', fn) }
    set onerror(fn) { this.addEventListener('error', fn) }
  }
}
