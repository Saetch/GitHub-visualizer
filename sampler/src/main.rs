/*!
git_geo_sampler — Phase 2
============================
Loads the preprocessed binary index produced by preprocess.py and samples
N (lon, lat) coordinates according to the joint distribution:

    P(lon, lat)  =  P(country)  ×  P(lon, lat | country)

where
    P(country)         ∝ github_developers[country]   (2025 Q4)
    P(lon,lat|country) ∝ population_density_cell      (GHS-POP 2020 1 km)

Usage:
    git_geo_sampler [OPTIONS]

Options:
    --n <N>             Number of samples  [default: 1000]
    --cells <PATH>      Path to country_cells.bin  [default: ../data/country_cells.bin]
    --meta <PATH>       Path to country_meta.json  [default: ../data/country_meta.json]
    --output <PATH>     Output CSV path            [default: samples.csv]
    --seed <SEED>       Optional u64 random seed
    --help              Print this help
*/

use anyhow::{Context, Result};
use rand::prelude::{RngCore, SeedableRng, StdRng, thread_rng};
use rand_distr::Distribution;
use rand_distr::WeightedIndex;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Minimal CLI parser (no clap)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Args {
    n:      usize,
    cells:  PathBuf,
    meta:   PathBuf,
    output: PathBuf,
    seed:   Option<u64>,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            n:      1000,
            cells:  PathBuf::from("../data/country_cells.bin"),
            meta:   PathBuf::from("../data/country_meta.json"),
            output: PathBuf::from("samples.csv"),
            seed:   None,
        }
    }
}

fn parse_args() -> Result<Args> {
    let mut a = Args::default();
    let raw: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < raw.len() {
        match raw[i].as_str() {
            "--help" | "-h" => {
                println!("{}", include_str!("../README_USAGE.txt"));
                std::process::exit(0);
            }
            "--n" => {
                i += 1;
                a.n = raw[i].parse().context("--n must be a positive integer")?;
            }
            "--cells" => { i += 1; a.cells = PathBuf::from(&raw[i]); }
            "--meta"  => { i += 1; a.meta  = PathBuf::from(&raw[i]); }
            "--output" | "-o" => { i += 1; a.output = PathBuf::from(&raw[i]); }
            "--seed"  => {
                i += 1;
                a.seed = Some(raw[i].parse().context("--seed must be a u64")?);
            }
            other => anyhow::bail!("Unknown argument: {}", other),
        }
        i += 1;
    }
    Ok(a)
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CountryCells {
    iso2:    String,
    name:    String,
    lons:    Vec<f32>,
    lats:    Vec<f32>,
    /// Per-cell weights normalised to sum ≈ 1.0 within the country
    weights: Vec<f32>,
}

#[derive(Deserialize, Debug)]
struct CountryMeta {
    name:               String,
    github_developers:  u64,
    #[allow(dead_code)]
    n_cells:            usize,
}

// ---------------------------------------------------------------------------
// Binary reader
// ---------------------------------------------------------------------------

fn read_u8(r: &mut impl Read) -> Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32_le(r: &mut impl Read) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Load the binary index produced by preprocess.py.
fn load_cells(path: &PathBuf) -> Result<Vec<CountryCells>> {
    let f = File::open(path)
        .with_context(|| format!("Cannot open cells file: {}", path.display()))?;
    let mut r = BufReader::new(f);

    let n_countries = read_u32_le(&mut r)? as usize;
    eprintln!("Loading {} countries from binary index …", n_countries);

    let mut countries = Vec::with_capacity(n_countries);

    for _ in 0..n_countries {
        let iso2_len = read_u8(&mut r)? as usize;
        let mut iso2_bytes = vec![0u8; iso2_len];
        r.read_exact(&mut iso2_bytes)?;
        let iso2 = String::from_utf8(iso2_bytes)?;

        let n_cells = read_u32_le(&mut r)? as usize;

        let byte_count = n_cells * 3 * 4;
        let mut raw = vec![0u8; byte_count];
        r.read_exact(&mut raw)?;

        let mut lons    = Vec::with_capacity(n_cells);
        let mut lats    = Vec::with_capacity(n_cells);
        let mut weights = Vec::with_capacity(n_cells);

        for i in 0..n_cells {
            let base = i * 12;
            lons.push(f32::from_le_bytes(raw[base..base+4].try_into().unwrap()));
            lats.push(f32::from_le_bytes(raw[base+4..base+8].try_into().unwrap()));
            weights.push(f32::from_le_bytes(raw[base+8..base+12].try_into().unwrap()));
        }

        countries.push(CountryCells {
            iso2,
            name: String::new(),
            lons,
            lats,
            weights,
        });
    }

    Ok(countries)
}

fn apply_meta(countries: &mut Vec<CountryCells>, meta: &HashMap<String, CountryMeta>) {
    for c in countries.iter_mut() {
        if let Some(m) = meta.get(&c.iso2) {
            c.name = m.name.clone();
        } else {
            c.name = format!("({})", c.iso2);
        }
    }
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

/// Build a country-level WeightedIndex from github developer counts.
fn build_country_dist(
    countries: &[CountryCells],
    meta: &HashMap<String, CountryMeta>,
) -> Result<WeightedIndex<f64>> {
    let weights: Vec<f64> = countries
        .iter()
        .map(|c| meta.get(&c.iso2).map(|m| m.github_developers as f64).unwrap_or(0.0))
        .collect();

    let total: f64 = weights.iter().sum();
    if total == 0.0 {
        anyhow::bail!("All country weights are zero — check country_meta.json");
    }

    WeightedIndex::new(&weights).context("Failed to build country WeightedIndex")
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = parse_args()?;

    // ---- Load binary index ----
    let mut countries = load_cells(&args.cells)?;

    // ---- Load metadata ----
    let meta_str = std::fs::read_to_string(&args.meta)
        .with_context(|| format!("Cannot read meta file: {}", args.meta.display()))?;
    let meta: HashMap<String, CountryMeta> =
        serde_json::from_str(&meta_str).context("Failed to parse country_meta.json")?;

    apply_meta(&mut countries, &meta);
    eprintln!(
        "Loaded {} countries with cell data, {} with GitHub dev stats",
        countries.len(),
        meta.len(),
    );

    // ---- Build country distribution ----
    let country_dist = build_country_dist(&countries, &meta)?;

    // ---- RNG ----
    let mut rng: Box<dyn RngCore> = match args.seed {
        Some(s) => Box::new(StdRng::seed_from_u64(s)),
        None    => Box::new(thread_rng()),
    };

    // ---- Sample ----
    eprintln!("Sampling {} points …", args.n);

    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create output: {}", args.output.display()))?;
    let mut writer = BufWriter::new(out_file);
    writeln!(writer, "lon,lat,iso2,country_name")?;

    for _ in 0..args.n {
        let cidx = country_dist.sample(&mut *rng);
        let country = &countries[cidx];
        // sample_cell needs a concrete Rng — use thread_rng for the inner call
        let (lon, lat) = {
            let dist = WeightedIndex::new(&country.weights)
                .expect("Invalid cell weights");
            let idx = dist.sample(&mut *rng);
            (country.lons[idx], country.lats[idx])
        };
        writeln!(
            writer,
            "{:.5},{:.5},{},{}",
            lon, lat, country.iso2, country.name
        )?;
    }

    writer.flush()?;
    eprintln!("Done! Wrote {} samples to {}", args.n, args.output.display());

    Ok(())
}
