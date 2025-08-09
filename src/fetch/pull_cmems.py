from __future__ import annotations
#!/usr/bin/env python
"""
Pull CMEMS currents and wave Stokes drift into data/raw/
"""

from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable

from copernicusmarine import subset, describe
import xarray as xr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# === CONFIG ===
BBOX = (28.0, 36.0, 31.0, 35.0)  # lon_min, lon_max, lat_min, lat_max
HOURS_AHEAD = 72

# CMEMS product IDs (MedSea forecast)
PRODUCT_PHY = "MEDSEA_ANALYSISFORECAST_PHY_006_013"
PRODUCT_WAV = "MEDSEA_ANALYSISFORECAST_WAV_006_017"

# Local storage
RAW_DIR = os.path.join("data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

def _list_datasets(product_id: str) -> list[tuple[str, list[str]]]:
    """
    Return [(dataset_id, keywords[]), ...] for a product.
    """
    cat = describe(product_id=product_id)  # returns CopernicusMarineCatalogue
    items = []
    for ds in getattr(cat, "datasets", []) or []:
        dsid = getattr(ds, "dataset_id", "")
        kws = getattr(ds, "keywords", None) or []
        kws = list(kws) if isinstance(kws, (list, tuple, set)) else []
        items.append((dsid, kws))
    return items

def _pick_dataset(product_id: str, prefer_keywords: tuple[str, ...]) -> str:
    """
    Prefer a dataset whose keywords+id contain all prefer_keywords (case-insensitive).
    Fallback: first dataset.
    """
    items = _list_datasets(product_id)
    if not items:
        raise SystemExit(f"No datasets listed for product {product_id}")
    want = [k.lower() for k in prefer_keywords]
    for dsid, kws in items:
        hay = (dsid + " " + " ".join(kws)).lower()
        if all(k in hay for k in want):
            logger.info(f"[{product_id}] Selected dataset by keywords: {dsid}")
            return dsid
    dsid = items[0][0]
    logger.info(f"[{product_id}] Using fallback dataset: {dsid}")
    return dsid

def _fetch_and_save(product_id: str, dataset_id: str, variables: Iterable[str], outfile: str,
                    start: datetime, end: datetime, bbox: tuple[float, float, float, float]):
    """
    Download subset from CMEMS and save to NetCDF.
    """
    logger.info(f"Fetching {product_id} / {dataset_id} → {outfile}")
    ds = subset(
        product_id=product_id,
        dataset_id=dataset_id,
        variables=variables,
        minimum_longitude=bbox[0],
        maximum_longitude=bbox[1],
        minimum_latitude=bbox[2],
        maximum_latitude=bbox[3],
        start_datetime=start,
        end_datetime=end,
    )
    ds.to_netcdf(outfile)
    logger.info(f"Saved {outfile}")

def main():
    now = datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=HOURS_AHEAD)
    logger.info(f"BBOX={BBOX} START={start.isoformat()} END={end.isoformat()}")

    # Optional: list datasets and exit
    if os.getenv("CMEMS_LIST", "0") == "1":
        for pid in (PRODUCT_PHY, PRODUCT_WAV):
            rows = _list_datasets(pid)
            logger.info(f"Datasets for {pid}:")
            for dsid, kws in rows:
                logger.info(f"  - {dsid} | keywords={kws}")
        return

    # Dataset IDs from config/env or auto-pick by keywords
    ds_phy_cfg = os.getenv("CMEMS_DATASET_PHY")
    ds_wav_cfg = os.getenv("CMEMS_DATASET_WAV")
    dataset_phy = ds_phy_cfg or _pick_dataset(PRODUCT_PHY, ("surface", "hour", "forecast"))
    dataset_wav = ds_wav_cfg or _pick_dataset(PRODUCT_WAV, ("stokes", "surface", "hour"))

    # Variables to request
    vars_phy = ["uo", "vo"]                # CMEMS currents
    vars_wav = ["ustokes", "vstokes"]      # Stokes drift

    _fetch_and_save(PRODUCT_PHY, dataset_phy, vars_phy,
                    os.path.join(RAW_DIR, "cmems_phy_surface.nc"), start, end, BBOX)
    _fetch_and_save(PRODUCT_WAV, dataset_wav, vars_wav,
                    os.path.join(RAW_DIR, "cmems_wav_stokes.nc"), start, end, BBOX)

if __name__ == "__main__":
    main()
