# GitHub Activity Globe

A real-time world map that shows GitHub activity as pings — powered by MapLibre GL JS with a dark OpenFreeMap style, fed by a WebSocket stream of GeoJSON features.

## Quick start

```bash
npm install
npm run dev
```

The dev server starts at `http://localhost:3000`. A mock WebSocket server is active in dev mode, generating synthetic ping bursts at realistic city coordinates — no backend needed to get started.

---

## Architecture

```
App
├── ActivityMap          # MapLibre container + RAF animation loop
│   ├── useActivityStream  # WebSocket → GeoJSON feature handler
│   └── PingManager       # Rolling window state, age calculation
└── StatsHud             # Polled status overlay (no map coupling)
```

### Data flow

```
WebSocket stream
      │
      ▼  (NDJSON or JSON frames)
useActivityStream
      │
      ▼  onFeature(GeoJSONFeature)
PingManager.add()
      │
      ▼  requestAnimationFrame loop
PingManager.tick()  ──►  source.setData(FeatureCollection)
                                  │
                                  ▼
                          MapLibre renders layers
                          (age-driven expressions on GPU)
```

### Why RAF instead of React state?

MapLibre manages its own WebGL context and re-renders. Updating React state on every WebSocket message (potentially hundreds per second) would flood the reconciler. Instead:

- `PingManager` is a plain JS class held in a `useRef` — zero React overhead.
- The `requestAnimationFrame` loop calls `source.setData()` directly on the map, which is exactly what MapLibre expects.
- React only updates for the HUD stats, polled every 250ms.

### Ping lifecycle

Each ping carries an `age` property (0 → 1 over its lifetime) that MapLibre style expressions use to interpolate radius, opacity, and stroke width on the GPU — no per-ping JS computation per frame.

```
age=0      age=0.3     age=0.7     age=1
 •─────    •────────   •──────    (removed)
 ring: small           ring: large, faded
 dot: bright           dot: dim
```

---

## Connecting your real stream

1. **Remove the mock** — in `src/App.jsx`, delete the `installMockServer` block and its import.
2. **Set your WS URL** — change `WS_URL` to your endpoint.
3. **Stream format** — send newline-delimited JSON (NDJSON) or plain JSON frames. Each frame must be one of:
   - A GeoJSON `Feature` with `geometry.type: "Point"` and `geometry.coordinates: [lon, lat]`
   - A GeoJSON `FeatureCollection` (each feature is emitted individually)
   - Multiple features as separate NDJSON lines in one frame

```json
{"type":"Feature","geometry":{"type":"Point","coordinates":[-0.1276,51.5074]},"properties":{"type":"PushEvent","repo":"user/repo"}}
```

---

## Configuration

All tunables live in `src/App.jsx`:

| Prop | Default | Description |
|------|---------|-------------|
| `wsUrl` | `ws://localhost:8080/github-events` | WebSocket endpoint |
| `styleUrl` | OpenFreeMap dark | Any MapLibre-compatible style URL |
| `lifetimeMs` | `4000` | How long each ping is visible |
| `maxPings` | `500` | Max simultaneous pings (oldest evicted) |

### Swap the map style

Any MapLibre-compatible style URL works. Some options:

```js
// OpenFreeMap (default, free, no API key)
styleUrl="https://tiles.openfreemap.org/styles/dark"

// Stadia Maps (free tier, dark)
styleUrl="https://tiles.stadiamaps.com/styles/alidade_smooth_dark.json"

// Protomaps (self-hostable)
styleUrl="https://api.protomaps.com/styles/v2/dark.json?key=YOUR_KEY"
```

---

## File structure

```
src/
├── App.jsx                        # Root: wires map + HUD, owns counters
├── index.css                      # Global reset + CSS custom properties
├── main.jsx                       # React entry point
│
├── components/
│   ├── ActivityMap.jsx            # MapLibre init, layers, RAF loop
│   ├── ActivityMap.module.css
│   ├── StatsHud.jsx               # Overlay panel (pure React, no map dep)
│   └── StatsHud.module.css
│
├── hooks/
│   └── useActivityStream.js       # WebSocket + auto-reconnect + NDJSON parser
│
└── utils/
    ├── PingManager.js             # Rolling window, age calculation, tick()
    └── mockServer.js             # Dev-only synthetic event generator
```
