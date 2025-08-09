from __future__ import annotations
#!/usr/bin/env python
"""
Download CMEMS Mediterranean surface currents (uo, vo) and Stokes drift (ustokes, vstokes)
into data/raw/ using the Copernicus Marine Toolbox.

Prereqs:
  pip install copernicusmarine
  copernicusmarine login   # once, enter your CMEMS credentials

Env overrides (optional):
  CMEMS_BBOX   = "28,36,31,35"         # lon_min,lon_max,lat_min,lat_max
  CMEMS_HOURS  = "72"                  # forecast horizon in hours
  CMEMS_START  = "YYYY-MM-DDTHH:00Z"   # start time UTC (defaults to now rounded hour)
  CMEMS_DATASET_PHY = "<dataset_id>"   # override dataset id
  CMEMS_DATASET_WAV = "<dataset_id>"   # override dataset id
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable
from pathlib import Path

from copernicusmarine import subset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bluecorridor.cmems")

# === Defaults: CMEMS Med hourly analysis-forecast datasets (surface) ===
# If CMEMS changes names later, set CMEMS_DATASET_PHY / CMEMS_DATASET_WAV in your env.
DATASET_PHY_DEFAULT = "cmems_mod_med_phy-cur_anfc_4.2km_PT1H-m"   # surface currents
DATASET_WAV_DEFAULT = "cmems_mod_med_wav_anfc_4.2km_PT1H-i"       # Stokes drift

RAW_DIR = Path("data/raw")
OUT_PHY = RAW_DIR / "cmems_phy_surface.nc"
OUT_WAV = RAW_DIR / "cmems_wav_stokes.nc"


def get_bbox() -> tuple[float, float, float, float]:
    s = os.getenv("CMEMS_BBOX", "28,36,31,35")
    lon_min, lon_max, lat_min, lat_max = [float(x) for x in s.split(",")]
    return lon_min, lon_max, lat_min, lat_max


def get_time_window() -> tuple[str, str]:
    start_env = os.getenv("CMEMS_START")
    if start_env:
        start = datetime.fromisoformat(start_env.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    hours = int(os.getenv("CMEMS_HOURS", "72"))
    end = start + timedelta(hours=hours)
    # Toolbox accepts datetime or ISO strings; we’ll pass ISO strings.
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def pull_dataset(dataset_id: str, variables: Iterable[str], outfile: Path,
                 bbox: tuple[float, float, float, float],
                 start_iso: str, end_iso: str) -> None:
    outfile.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Subsetting {dataset_id} vars={list(variables)} → {outfile.name}")
    ds = subset(
        dataset_id=dataset_id,
        variables=list(variables),
        minimum_longitude=bbox[0], maximum_longitude=bbox[1],
        minimum_latitude=bbox[2], maximum_latitude=bbox[3],
        start_datetime=start_iso, end_datetime=end_iso,
    )
    ds.to_netcdf(outfile)
    if not outfile.exists() or outfile.stat().st_size == 0:
        raise SystemExit(f"Failed to write {outfile}")
    logger.info(f"Saved → {outfile}")


def main():
    bbox = get_bbox()
    start_iso, end_iso = get_time_window()
    logger.info(f"BBOX={bbox} START={start_iso} END={end_iso}")

    dataset_phy = os.getenv("CMEMS_DATASET_PHY", DATASET_PHY_DEFAULT)
    dataset_wav = os.getenv("CMEMS_DATASET_WAV", DATASET_WAV_DEFAULT)

    # Variable names in these datasets:
    vars_phy = ["uo", "vo"]            # eastward/northward surface current
    vars_wav = ["ustokes", "vstokes"]  # Stokes drift components

    pull_dataset(dataset_phy, vars_phy, OUT_PHY, bbox, start_iso, end_iso)
    pull_dataset(dataset_wav, vars_wav, OUT_WAV, bbox, start_iso, end_iso)


if __name__ == "__main__":
    main()
