# TODO: Implement Motu/OPeNDAP fetch for CMEMS Med Physics/Waves
from pathlib import Path
from src.util.logging_setup import setup_logger
logger = setup_logger()

def main():
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    logger.info("Stub CMEMS fetch complete (replace with Motu client).")
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
from __future__ import annotations
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
    # start: now (rounded down to hour) unless CMEMS_START is provided
    start_s = _env("CMEMS_START")
    if start_s:
        start = dt.datetime.fromisoformat(start_s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    else:
        start = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=dt.timezone.utc)
    hours = int(_env("CMEMS_HOURS", "72"))
    end = start + dt.timedelta(hours=hours)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def _pick_dataset(product_id: str, prefer_keywords: tuple[str, ...]) -> str:
    """
    Inspect product datasets and pick one that looks like surface/hourly analysis-forecast.
    If no keyword match, use the first dataset as a fallback.
    """
    info = describe(product_id=product_id)
    datasets = info.get("datasets", []) or []
    if not datasets:
        raise SystemExit(f"No datasets listed for product {product_id}")

    # Try keyword-based selection
    kw = [k.lower() for k in prefer_keywords]
    for ds in datasets:
        dsid = ds.get("dataset_id", "")
        text = " ".join((dsid, " ".join(ds.get("keywords", [])))).lower()
        if all(k in text for k in kw):
            logger.info(f"[{product_id}] Selected dataset by keywords: {dsid}")
            return dsid

    # Fallback: first dataset
    dsid = datasets[0].get("dataset_id")
    logger.info(f"[{product_id}] Using fallback dataset: {dsid}")
    return dsid


def _subset_one(dataset_id: str, variables: list[str], out_nc: Path,
                bbox: tuple[float, float, float, float],
                start_iso: str, end_iso: str) -> None:
    out_nc.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Subsetting {dataset_id} vars={variables} → {out_nc.name}")
    subset(
        dataset_id=dataset_id,
        variables=variables,
        minimum_longitude=bbox[0], maximum_longitude=bbox[1],
        minimum_latitude=bbox[2], maximum_latitude=bbox[3],
        start_datetime=start_iso,
        end_datetime=end_iso,
        output_filename=str(out_nc),
        overwrite=True,
    )
    if not out_nc.exists() or out_nc.stat().st_size == 0:
        raise SystemExit(f"Failed to save {out_nc}")
    logger.info(f"Saved → {out_nc}")


def main():
    # Optional config file support: variables or dataset overrides
    cfg_vars_phy = ["uo", "vo"]          # default current component names
    cfg_vars_wav = ["ustokes", "vstokes"]  # default stokes component names
    try:
        with open("config/layers.yaml", "r") as f:
            cfg = yaml.safe_load(f) or {}
        # allow overrides if present
        cfg_vars_phy = cfg.get("variables", {}).get("phy", cfg_vars_phy)
        cfg_vars_wav = cfg.get("variables", {}).get("wav", cfg_vars_wav)
        ds_phy_cfg = cfg.get("fields", {}).get("dataset_phy", "") or None
        ds_wav_cfg = cfg.get("fields", {}).get("dataset_wav", "") or None
    except Exception:
        ds_phy_cfg = ds_wav_cfg = None

    bbox = _bbox()
    start_iso, end_iso = _time_window()
    logger.info(f"BBOX={bbox} START={start_iso} END={end_iso}")

    # Choose datasets (either from config or by discovery)
    dataset_phy = ds_phy_cfg or _pick_dataset(PRODUCT_PHY, prefer_keywords=("surface", "hour", "forecast"))
    dataset_wav = ds_wav_cfg or _pick_dataset(PRODUCT_WAV, prefer_keywords=("stokes", "surface", "hour", "forecast"))

    # Pull physics (surface currents)
    _subset_one(dataset_phy, cfg_vars_phy, OUT_PHY, bbox, start_iso, end_iso)

    # Pull waves (stokes drift)
    _subset_one(dataset_wav, cfg_vars_wav, OUT_WAV, bbox, start_iso, end_iso)


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
