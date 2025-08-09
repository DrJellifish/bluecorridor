"""
Download GFS 0.25° 10 m winds (UGRD/VGRD) for a bbox & forecast hours
and save a single GRIB2 per run. No API key required (NOMADS filter).
"""
from __future__ import annotations
import os, sys, math, time, datetime as dt
from pathlib import Path
import httpx
from urllib.parse import urlencode

from src.util.logging_setup import setup_logger
logger = setup_logger("bluecorridor.gfs")

NOMADS_BASE = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

def gfs_cycle(now_utc: dt.datetime) -> tuple[str,str]:
    """Return latest GFS cycle date (YYYYMMDD) and hour (00/06/12/18) available."""
    hh = (now_utc.hour // 6) * 6
    return now_utc.strftime("%Y%m%d"), f"{hh:02d}"

def build_url(yyyymmdd: str, hh: str, fff: int, bbox: tuple[float,float,float,float]) -> str:
    lon_min, lon_max, lat_min, lat_max = bbox
    params = {
        "dir": f"/gfs.{yyyymmdd}/{hh}/atmos",
        "file": f"gfs.t{hh}z.pgrb2.0p25.f{fff:03d}",
        "lev_10_m_above_ground": "on",
        "var_UGRD": "on",
        "var_VGRD": "on",
        # geographic subset
        "subregion": "",
        "leftlon": lon_min, "rightlon": lon_max,
        "toplat": lat_max, "bottomlat": lat_min,
    }
    return f"{NOMADS_BASE}?{urlencode(params)}"

def download_winds(out_dir: Path, hours: list[int], bbox: tuple[float,float,float,float], timeout=60):
    out_dir.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.utcnow()
    ymd, hh = gfs_cycle(now)
    client = httpx.Client(timeout=timeout, follow_redirects=True)
    saved = []
    for fff in hours:
        url = build_url(ymd, hh, fff, bbox)
        out = out_dir / f"gfs_{ymd}_{hh}_f{fff:03d}_10m_uv.grib2"
        if out.exists() and out.stat().st_size > 0:
            logger.info(f"Exists: {out.name}")
            saved.append(out)
            continue
        logger.info(f"GET {url}")
        r = client.get(url)
        if r.status_code != 200 or len(r.content) < 10000:
            logger.error(f"Bad response ({r.status_code}) for f{fff:03d}; content={len(r.content)} bytes")
            continue
        out.write_bytes(r.content)
        logger.info(f"Saved → {out}")
        saved.append(out)
        time.sleep(0.5)  # be polite
    client.close()
    if not saved:
        raise SystemExit("No GFS files saved. Check NOMADS availability or bbox.")
    return saved

def main():
    # Levant-ish default bbox: lon_min, lon_max, lat_min, lat_max
    bbox = tuple(map(float, os.getenv("GFS_BBOX", "28,36,31,35").split(",")))  # 28E–36E, 31N–35N
    horizon = int(os.getenv("GFS_HOURS", "24"))
    step = int(os.getenv("GFS_STEP", "3"))  # GFS is 3-hourly beyond f0
    hours = list(range(0, horizon+1, step))
    out_dir = Path(os.getenv("GFS_OUT", "data/raw/gfs"))
    logger.info(f"BBOX={bbox} HOURS={hours}")
    download_winds(out_dir, hours, bbox)

if __name__ == "__main__":
    main()
