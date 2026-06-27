git_geo_sampler — sample geographic origins of git events

USAGE:
  git_geo_sampler [OPTIONS]

OPTIONS:
  --n <N>          Number of samples to generate  [default: 1000]
  --cells <PATH>   Path to country_cells.bin      [default: ../data/country_cells.bin]
  --meta <PATH>    Path to country_meta.json      [default: ../data/country_meta.json]
  --output <PATH>  Output CSV file path           [default: samples.csv]
  --seed <SEED>    u64 random seed for reproducibility
  --help           Show this help message

OUTPUT:
  CSV with columns: lon,lat,iso2,country_name
