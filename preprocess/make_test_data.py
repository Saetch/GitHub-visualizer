#!/usr/bin/env python3
"""
make_test_data.py — generate synthetic country_cells.bin + country_meta.json
to smoke-test the Rust sampler without needing the real GHS raster.

Three fake countries:
  US  (high developer count) — cells centred on continental US
  DE  (medium)               — cells centred on Germany
  JP  (low)                  — cells centred on Japan
"""
import json
import struct
import numpy as np
from pathlib import Path

OUT = Path(__file__).parent.parent / "data"
OUT.mkdir(parents=True, exist_ok=True)

rng = np.random.default_rng(42)

countries = [
    {
        "iso2": "US",
        "name": "United States of America",
        "github_developers": 30_298_253,
        # 200 cells, scattered across continental US
        "lons": rng.uniform(-125.0, -67.0, 200).astype(np.float32),
        "lats": rng.uniform(25.0,   49.0,  200).astype(np.float32),
        "pops": rng.exponential(1000.0, 200).astype(np.float32),
    },
    {
        "iso2": "DE",
        "name": "Germany",
        "github_developers": 4_858_805,
        "lons": rng.uniform(6.0,  15.0, 80).astype(np.float32),
        "lats": rng.uniform(47.5, 55.0, 80).astype(np.float32),
        "pops": rng.exponential(500.0, 80).astype(np.float32),
    },
    {
        "iso2": "JP",
        "name": "Japan",
        "github_developers": 4_973_908,
        "lons": rng.uniform(130.0, 145.0, 60).astype(np.float32),
        "lats": rng.uniform(31.0,  45.0,  60).astype(np.float32),
        "pops": rng.exponential(800.0, 60).astype(np.float32),
    },
]

# --- Write binary ---
bin_path = OUT / "country_cells.bin"
with open(bin_path, "wb") as f:
    f.write(struct.pack("<I", len(countries)))
    for c in countries:
        pops = c["pops"]
        weights = (pops / pops.sum()).astype(np.float32)
        n = len(c["lons"])
        iso2 = c["iso2"].encode()
        f.write(struct.pack("<B", len(iso2)))
        f.write(iso2)
        f.write(struct.pack("<I", n))
        arr = np.column_stack([c["lons"], c["lats"], weights]).astype(np.float32)
        f.write(arr.tobytes())

print(f"Wrote {bin_path}")

# --- Write meta ---
meta = {
    c["iso2"]: {
        "name": c["name"],
        "github_developers": c["github_developers"],
        "n_cells": len(c["lons"]),
    }
    for c in countries
}
meta_path = OUT / "country_meta.json"
with open(meta_path, "w") as f:
    json.dump(meta, f, indent=2)

print(f"Wrote {meta_path}")
print("Done — run the Rust sampler now.")
