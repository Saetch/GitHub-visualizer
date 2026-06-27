#!/usr/bin/env python3
"""
preprocess.py — Phase 1: build per-country population density index
========================================================================
Downloads:
  1. Natural Earth 10m country borders (shapefile)
  2. GHS-POP 2020 1km WGS84 raster (GeoTIFF, ~460 MB on disk after extract)

Outputs (written to ../data/):
  country_cells.bin   — compact binary index (see format below)
  country_meta.json   — country metadata (ISO2, name, total weight)

Binary format (little-endian):
  Header:
    u32  n_countries
  Per country:
    u8   iso2_len
    [u8; iso2_len]  iso2 bytes
    u32  n_cells
    Per cell (interleaved):
      f32  lon
      f32  lat
      f32  weight   (normalised to sum=1.0 within country)

Usage:
  pip install rasterio fiona shapely pyproj geopandas numpy
  python preprocess.py
  python preprocess.py --raster /path/to/file.tif   # skip raster download
  python preprocess.py --shapefile /path/to/file.shp # skip shapefile download
"""

import argparse
import json
import struct
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUT_DIR = Path(__file__).parent.parent / "data"

NE_ZIP_URL = (
    "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip"
)
GHS_ZIP_URL = (
    "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/"
    "GHS_POP_GLOBE_R2023A/"
    "GHS_POP_E2020_GLOBE_R2023A_4326_30ss/V1-0/"
    "GHS_POP_E2020_GLOBE_R2023A_4326_30ss_V1_0.zip"
)
GHS_TIF_NAME = "GHS_POP_E2020_GLOBE_R2023A_4326_30ss_V1_0.tif"

# ---------------------------------------------------------------------------
# Natural Earth ISO_A2 quirks
# ---------------------------------------------------------------------------
# NE stores some features as ISO_A2 = "-99" for political or administrative
# reasons. Resolution order:
#   1. ISO_A2       — primary field
#   2. ISO_A2_EH    — "Exceptionally Handled" (fixes FR, NO, TW in some releases)
#   3. ADM0_A3      — NE's own reliable 3-letter code (fixes TW, GF, GP, MQ,
#                     RE, YT, BQ when both ISO fields are -99)
#   4. NAME lookup  — last resort for Kosovo and any remaining edge cases
#   5. SKIP         — disputed/uninhabited zones (Spratly Is., Bir Tawil, etc.)

# ADM0_A3 → ISO2 for features where both ISO_A2 and ISO_A2_EH are "-99"
ADM0_TO_ISO2 = {
    "GUF": "GF",   # French Guiana
    "GLP": "GP",   # Guadeloupe
    "MTQ": "MQ",   # Martinique
    "REU": "RE",   # Réunion
    "MYT": "YT",   # Mayotte
    "BES": "BQ",   # Bonaire, Sint Eustatius, Saba (Caribbean Netherlands)
    "TWN": "TW",   # Taiwan
    "KOS": "XK",   # Kosovo (variant 1)
    "XKX": "XK",   # Kosovo (variant 2)
}

# Name-fragment → ISO2 as final fallback
OVERSEAS_BY_NAME = {
    "Kosovo":               "XK",
    "Réunion":              "RE",
    "Reunion":              "RE",
    "Guadeloupe":           "GP",
    "Martinique":           "MQ",
    "French Guiana":        "GF",
    "Guyane":               "GF",
    "Mayotte":              "YT",
    "Bonaire":              "BQ",
    "Sint Eustatius":       "BQ",
    "Saba":                 "BQ",
}

# EU member states whose population cells we merge to form the EU geometry.
# GitHub's "EU" code represents traffic that geolocates to the EU IP block
# but cannot be attributed to a specific country — so we spread it across
# all EU member states proportionally to their population density.
EU_MEMBER_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR",
    "DE","GR","HU","IE","IT","LV","LT","LU","MT","NL",
    "PL","PT","RO","SK","SI","ES","SE",
}

# Vatican (VA) is 0.44 km² — too small for a 1km raster cell.
# Fall back to a single synthetic cell at the centre of Vatican City.
VATICAN_LON, VATICAN_LAT = 12.4534, 41.9029

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def download(url: str, dest: Path, label: str) -> None:
    print(f"  Downloading {label}…")
    def _progress(count, block, total):
        if total > 0:
            pct = min(100, count * block * 100 // total)
            print(f"\r    {pct:3d}%", end="", flush=True)
    urllib.request.urlretrieve(url, dest, reporthook=_progress)
    print(f"\r    Done ({dest.stat().st_size // 1024 // 1024} MB)   ")


def extract_first_shp(zip_path: Path, out_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    shps = list(out_dir.glob("*.shp"))
    if not shps:
        raise FileNotFoundError("No .shp in NE zip")
    return shps[0]


def extract_tif(zip_path: Path, out_dir: Path, tif_name: str) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.endswith(".tif")]
        if not members:
            raise FileNotFoundError("No .tif in GHS zip")
        target = next((m for m in members if tif_name in m), members[0])
        print(f"  Extracting {target} …")
        zf.extract(target, out_dir)
    return out_dir / target


# ---------------------------------------------------------------------------
# Shapefile → geometry map
# ---------------------------------------------------------------------------

def load_geometries(shp_path: Path) -> dict:
    """
    Returns dict: iso2 -> {"name": str, "geom": shapely geometry}

    Resolution order for iso2:
      1. ISO_A2      — primary field
      2. ISO_A2_EH   — "Exceptionally Handled" (fixes FR, NO in some NE releases)
      3. ADM0_A3     — NE 3-letter code via ADM0_TO_ISO2 table
                       (fixes TW, GF, GP, MQ, RE, YT, BQ, XK)
      4. NAME lookup — final catch-all for remaining edge cases
      5. SKIP        — disputed/uninhabited zones with no ISO2
    """
    import fiona
    from shapely.geometry import shape
    from shapely.ops import unary_union

    countries = {}

    def _merge(iso2, name, geom):
        if iso2 not in countries:
            countries[iso2] = {"name": name, "geom": geom}
        else:
            countries[iso2]["geom"] = unary_union([countries[iso2]["geom"], geom])

    with fiona.open(shp_path) as src:
        for feat in src:
            p       = feat["properties"]
            name    = p.get("NAME") or p.get("name") or ""
            iso2    = (p.get("ISO_A2")    or "").strip()
            iso2_eh = (p.get("ISO_A2_EH") or "").strip()
            adm0_a3 = (p.get("ADM0_A3")   or "").strip()

            # Step 1 → 2: ISO_A2_EH when primary is -99
            if iso2 == "-99" or not iso2:
                iso2 = iso2_eh

            geom = shape(feat["geometry"])

            # Step 3: ADM0_A3 lookup (catches TW, GF, GP, MQ, RE, YT, BQ, XK)
            if iso2 == "-99" or not iso2:
                iso2 = ADM0_TO_ISO2.get(adm0_a3, "")

            # Step 4: name-fragment lookup (last resort)
            if iso2 == "-99" or not iso2:
                iso2 = next(
                    (code for frag, code in OVERSEAS_BY_NAME.items()
                     if frag.lower() in name.lower()),
                    ""
                )

            # Step 5: skip unresolvable (disputed zones, glaciers, etc.)
            if not iso2 or iso2 == "-99":
                print(f"  SKIP: cannot resolve ISO2 for '{name}' "
                      f"(ADM0_A3={adm0_a3!r})")
                continue

            _merge(iso2, name, geom)

    print(f"  Loaded {len(countries)} country geometries")
    return countries


# ---------------------------------------------------------------------------
# Raster masking
# ---------------------------------------------------------------------------

def build_raster_index(countries: dict, tif_path: Path) -> dict:
    """
    Mask GHS-POP raster per country geometry.
    Returns dict: iso2 -> {name, lons, lats, weights (raw pop counts)}
    """
    import rasterio
    import rasterio.mask

    index = {}
    n = len(countries)

    with rasterio.open(tif_path) as src:
        nodata = src.nodata if src.nodata is not None else -1
        print(f"  Raster CRS: {src.crs}, shape: {src.height}×{src.width}, nodata={nodata}")

        for i, (iso2, info) in enumerate(countries.items()):
            geom = info["geom"]
            try:
                masked, transform = rasterio.mask.mask(
                    src, [geom.__geo_interface__], crop=True, nodata=nodata
                )
                data = masked[0].astype(np.float32)
                data[data == nodata] = 0
                data[data < 0] = 0

                rows, cols = np.where(data > 0)
                if len(rows) == 0:
                    continue

                weights = data[rows, cols]
                xs, ys  = rasterio.transform.xy(transform, rows, cols)

                index[iso2] = {
                    "name":    info["name"],
                    "lons":    np.array(xs, dtype=np.float32),
                    "lats":    np.array(ys, dtype=np.float32),
                    "weights": weights,
                }

                if (i + 1) % 20 == 0 or i < 5:
                    print(
                        f"  [{i+1:3d}/{n}] {iso2:5s} "
                        f"{info['name'][:30]:30s}  {len(rows):>8,} cells"
                    )

            except Exception as e:
                print(f"  WARN [{iso2}] {info['name']}: {e}")

    return index


# ---------------------------------------------------------------------------
# Special-case handlers
# ---------------------------------------------------------------------------

def inject_eu(index: dict) -> None:
    """
    Build the EU entry by merging all EU member state cells.
    GitHub's 'EU' code = traffic that resolves to EU IP space but not a
    specific country, so we distribute it across EU members by population.
    """
    merged_lons, merged_lats, merged_weights = [], [], []
    found = []
    for iso2 in EU_MEMBER_ISO2:
        if iso2 in index:
            found.append(iso2)
            merged_lons.append(index[iso2]["lons"])
            merged_lats.append(index[iso2]["lats"])
            merged_weights.append(index[iso2]["weights"])

    if not merged_lons:
        print("  WARN: No EU member cells found — EU entry will be empty")
        return

    print(f"\n  EU entry: merging {len(found)}/{len(EU_MEMBER_ISO2)} member states")
    missing_eu = EU_MEMBER_ISO2 - set(found)
    if missing_eu:
        print(f"  WARN: EU members missing from raster index: {sorted(missing_eu)}")

    index["EU"] = {
        "name":    "European Union (unattributed traffic)",
        "lons":    np.concatenate(merged_lons),
        "lats":    np.concatenate(merged_lats),
        "weights": np.concatenate(merged_weights),
    }


def inject_vatican(index: dict) -> None:
    """Vatican is smaller than a 1km raster cell — inject a single synthetic point."""
    if "VA" not in index:
        print("  VA (Vatican): no raster cells found — injecting synthetic point")
        index["VA"] = {
            "name":    "Vatican City",
            "lons":    np.array([VATICAN_LON], dtype=np.float32),
            "lats":    np.array([VATICAN_LAT], dtype=np.float32),
            "weights": np.array([1.0],         dtype=np.float32),
        }


# Synthetic fallback points for territories that fail raster masking.
# These are used only when the raster produces zero cells (geometry mismatch,
# territory too small, or NE polygon issue).
# Each entry: iso2 -> (name, [(lon, lat, relative_weight), ...])
# Weights within each entry are relative — they get normalised later.
SYNTHETIC_FALLBACKS = {
    # Taiwan: major population centres (Taipei, New Taipei, Taichung, Kaohsiung, Tainan)
    "TW": ("Taiwan", [
        (121.5654, 25.0330, 2.6),   # Taipei metro
        (121.4657, 25.0122, 3.9),   # New Taipei (largest city)
        (120.6736, 24.1477, 2.8),   # Taichung
        (120.3010, 22.6273, 2.7),   # Kaohsiung
        (120.2270, 22.9999, 1.9),   # Tainan
        (120.9647, 24.8066, 1.1),   # Taoyuan
    ]),
    # French Guiana: almost all population in Cayenne coastal strip
    "GF": ("French Guiana", [
        (-52.3261, 4.9372, 1.0),    # Cayenne
        (-52.6463, 5.1622, 0.3),    # Kourou
        (-53.6347, 5.4778, 0.2),    # Saint-Laurent-du-Maroni
    ]),
    # Guadeloupe: two main islands
    "GP": ("Guadeloupe", [
        (-61.5800, 16.0000, 1.2),   # Pointe-à-Pitre (Grande-Terre)
        (-61.7200, 16.0500, 0.8),   # Basse-Terre
    ]),
    # Martinique: Fort-de-France dominates
    "MQ": ("Martinique", [
        (-61.0589, 14.6037, 1.0),   # Fort-de-France
        (-60.9000, 14.7500, 0.3),   # Le Robert
    ]),
    # Réunion: Saint-Denis and south coast
    "RE": ("Réunion", [
        (55.4513, -20.8823, 1.0),   # Saint-Denis
        (55.4833, -21.1167, 0.5),   # Saint-Pierre
        (55.5500, -21.3333, 0.3),   # Saint-Louis
    ]),
    # Mayotte: Mamoudzou
    "YT": ("Mayotte", [
        (45.2272, -12.7806, 1.0),   # Mamoudzou
        (45.1386, -12.8445, 0.4),   # Koungou
    ]),
    # Bonaire, Sint Eustatius and Saba: Kralendijk is by far the largest
    "BQ": ("Caribbean Netherlands", [
        (-68.2683, 12.1448, 0.7),   # Kralendijk (Bonaire)
        (-62.9853, 17.4887, 0.2),   # Oranjestad (Sint Eustatius)
        (-63.2333, 17.6333, 0.1),   # The Bottom (Saba)
    ]),
}


def inject_missing_territories(index: dict) -> None:
    """
    For territories that produced zero raster cells (usually because NE's polygon
    doesn't align with the raster grid at 1km resolution, or the territory is a
    small island), inject synthetic population-weighted points.
    Only injects if the code is not already present in index.
    """
    for iso2, (name, points) in SYNTHETIC_FALLBACKS.items():
        if iso2 in index:
            continue  # raster masking worked fine
        lons    = np.array([p[0] for p in points], dtype=np.float32)
        lats    = np.array([p[1] for p in points], dtype=np.float32)
        weights = np.array([p[2] for p in points], dtype=np.float32)
        print(f"  {iso2} ({name}): injecting {len(points)} synthetic point(s)")
        index[iso2] = {"name": name, "lons": lons, "lats": lats, "weights": weights}


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_binary(index: dict, out_path: Path) -> None:
    print(f"\nWriting binary index → {out_path} …")
    with open(out_path, "wb") as f:
        f.write(struct.pack("<I", len(index)))
        for iso2, info in index.items():
            iso2_bytes = iso2.encode("utf-8")
            weights = info["weights"].copy()
            w_sum = weights.sum()
            if w_sum > 0:
                weights /= w_sum          # normalise to sum=1.0

            n_cells = len(info["lons"])
            f.write(struct.pack("<B", len(iso2_bytes)))
            f.write(iso2_bytes)
            f.write(struct.pack("<I", n_cells))
            arr = np.column_stack([info["lons"], info["lats"], weights]).astype(np.float32)
            f.write(arr.tobytes())

    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"  Written {len(index)} countries — {size_mb:.1f} MB")


def write_meta(index: dict, github_data: dict, out_path: Path) -> None:
    meta = {
        iso2: {
            "name":               info["name"],
            "github_developers":  github_data.get(iso2, 0),
            "n_cells":            int(len(info["lons"])),
        }
        for iso2, info in index.items()
    }
    with open(out_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  Metadata written → {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Preprocess geo data for git_geo sampler")
    ap.add_argument("--raster",    help="Path to already-downloaded GHS GeoTIFF")
    ap.add_argument("--shapefile", help="Path to already-extracted .shp file")
    ap.add_argument("--out",       default=str(OUT_DIR), help="Output directory")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="git_geo_"))

    # ── Step 0: GitHub CSV ──────────────────────────────────────────────────
    print("[0/3] Reading GitHub developer data …")
    gh_url = "https://raw.githubusercontent.com/github/innovationgraph/main/data/developers.csv"
    with urllib.request.urlopen(gh_url) as r:
        lines = r.read().decode().splitlines()

    github_data = {}
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) == 4 and parts[2] == "2025" and parts[3] == "4":
            github_data[parts[1]] = int(parts[0])

    print(f"  Loaded {len(github_data)} entries for 2025 Q4")
    github_iso2s = set(github_data.keys())

    # ── Step 1: Natural Earth shapefile ─────────────────────────────────────
    if args.shapefile:
        shp_path = Path(args.shapefile)
        print(f"[1/3] Using provided shapefile: {shp_path}")
    else:
        print("[1/3] Downloading Natural Earth shapefile …")
        ne_zip = tmp_dir / "ne_countries.zip"
        ne_dir = tmp_dir / "ne"
        ne_dir.mkdir()
        download(NE_ZIP_URL, ne_zip, "Natural Earth 10m countries")
        shp_path = extract_first_shp(ne_zip, ne_dir)
        print(f"  Shapefile: {shp_path}")

    # ── Step 1b: GHS-POP raster ─────────────────────────────────────────────
    if args.raster:
        tif_path = Path(args.raster)
        print(f"[1b/3] Using provided raster: {tif_path}")
    else:
        print("[1b/3] Downloading GHS-POP 2020 1km raster (~460 MB unzipped) …")
        print("  URL:", GHS_ZIP_URL)
        ghs_zip = tmp_dir / "ghs_pop.zip"
        download(GHS_ZIP_URL, ghs_zip, "GHS-POP 2020 1km WGS84")
        tif_path = extract_tif(ghs_zip, tmp_dir / "ghs", GHS_TIF_NAME)

    # ── Step 2: load shapefile geometries ───────────────────────────────────
    print("\n[2/3] Reading shapefile …")
    countries = load_geometries(shp_path)

    # ── Step 3: mask raster per country ─────────────────────────────────────
    print("\n[3/3] Rasterising population per country …")
    index = build_raster_index(countries, tif_path)

    # ── Special cases ────────────────────────────────────────────────────────
    inject_eu(index)
    inject_vatican(index)
    inject_missing_territories(index)

    # ── Filter to GitHub ISO2 codes only ─────────────────────────────────────
    filtered = {k: v for k, v in index.items() if k in github_iso2s}
    missing  = github_iso2s - set(filtered.keys())
    if missing:
        print(f"\n  WARN: still no data for: {sorted(missing)}")
        print("  These will have github_developers weight but no cells → excluded from sampling")
    print(f"\n  Final index: {len(filtered)}/{len(github_iso2s)} GitHub codes covered")

    # ── Write ─────────────────────────────────────────────────────────────────
    write_binary(filtered, out_dir / "country_cells.bin")
    write_meta(filtered,   github_data, out_dir / "country_meta.json")
    print("\nPreprocessing complete!")


if __name__ == "__main__":
    main()
