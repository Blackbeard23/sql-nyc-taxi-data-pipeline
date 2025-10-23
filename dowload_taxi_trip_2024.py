from pathlib import Path
import pandas as pd

year = 2024
out_dir = Path.cwd() / "data"
out_dir.mkdir(parents=True, exist_ok=True)

download_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


for m in range(1, 13):
    url = download_url.format(year=year, month=m)
    out_path = out_dir / f"yellow_tripdata_{year}-{m:02d}.csv"
    print(f"Downloading+converting {url} -> {out_path.name}")

    df = pd.read_parquet(url, engine='pyarrow')

    df.to_csv(out_path, index=False)

    print(f'Download complete for {year}-{m:02d}')

print("Done.")
