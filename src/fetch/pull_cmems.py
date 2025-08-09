from __future__ import annotations

"""
Pull CMEMS Med Physics (surface currents) & Waves (Stokes drift) using the
Copernicus Marine Toolbox. Writes two NetCDFs under data/raw/ for stitching.

Env (optional):
  CMEMS_BBOX   = "28,36,31,35"   # lon_min,lon_max,lat_min,lat_max
  CMEMS_HOURS  = "72"            # forecast horizon
  CMEMS_START  = ISO8601 UTC     # default: now (rounded hour)
  CMEMS_USER / CMEMS_PASS        # if not using interactive login

Requires: pip install copernicusmarine
First time: copernicusmarine login
"""
import os
import datetime as dt
from pathlib import Path
import yaml

from src.util.logging_setup import setup_logger
logger = setup_logger("bluecorridor.cmems")

# Official toolbox
from copernicusmarine import subset, describe

# Product IDs (stable)
PRODUCT_PHY = "MEDSEA_ANALYSIS_FORECAST_PHY_006_013"   # Med physics (currents)
PRODUCT_WAV = "MEDSEA_ANALYSIS_FORECAST_WAV_006_017"   # Med waves (Stokes)

# Output paths
OUT_PHY = Path("data/raw/cmems_phy_surface.nc")
OUT_WAV = Path("data/raw/cmems_wav_stokes.nc")


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    return v


def _bbox() -> tuple[float, float, float, float]:
    s = _env("CMEMS_BBOX", "28,36,31,35")
    lon_min, lon_max, lat_min, lat_max = [float(x) for x in s.split(",")]
    return lon_min, lon_max, lat_min, lat_max


def _time_window() -> tuple[str, str]:
    start_s = _env("CMEMS_START")
    if start_s:
        start = dt.datetime.fromisoformat(start_s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    else:
        start = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=dt.timezone.utc)
    hours = int(_env("CMEMS_HOURS", "72"))
    end = start + dt.timedelta(hours=hours)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def _pick_dataset(product_id: str, prefer_keywords: tuple[str, ...]) -> str:
    info = describe(prod
