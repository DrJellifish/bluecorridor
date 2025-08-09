"""
Download GFS 0.25° 10 m winds (UGRD/VGRD) for a bbox & forecast hours via NOMADS.
Auto-fallback to previous cycles when the latest isn't staged yet.
"""
from __future__ import annotations
import os, time, datetime as dt
from pathlib import Path
from urllib.parse import urlencode
import httpx

from src.util.logging_setup import setup_logger
logger = setup_logger("bluecorridor.gfs")

NOMADS_BASE = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

def cycle_str(dt_utc: dt.datetime) -> tuple[str, str]:
    hh = (dt_utc.hour // 6) * 6
    return dt_utc.strftime("%Y%m%d"), f"{hh:02d}"

def build_url(yyyymmdd: str, hh: str, fff: int, bbox: tuple[float,float,float,float]) -> str:
    lon_min, lon_max, lat_min, lat_max = bbox
    params = {
        "dir": f"/gfs.{yyyymmdd}/{hh}/atmos",
        "file": f"gfs.t{hh}z.pgrb2.0p25.f{fff:03d}",
        "lev_10_m_above_ground": "on",
        "var_UGRD": "on",
        "var_VGRD": "on",
        "leftlon": lon_min, "rightlon": lon_max,
        "toplat": lat_max, "bottomlat": lat_min,
    }
    return f"{NOMADS_BASE}?{urlencode(params)}"

def try_download(client: httpx.Client, url: str, out: Path, min_bytes: int = 100_000) -> bool:
    r = client.get(url)
    if r.status_code != 200 or len(r.content) < min_bytes:
        snippet = r.text[:200].replace("\n", " ")
        logger.error(f"Bad response ({r.status_code}), {len(r.content)} bytes. Snippet: {snippet}")
        return False
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(r.content)
    logger.info(f"Saved → {out}")
    return True

def download_cycle(yyyymmdd: str, hh: str, hours: list[int], bbox: tuple[float,float,float,float], out_dir: Path) -> list[Path]:
    client = httpx.Client(timeout=90, follow_redirects=True)
    saved: list[Path] = []
    for fff in hours:
        url = build_url(yyyymmdd, hh, fff, bbox)
        out = out_dir / f"gfs_{yyyymmdd}_{hh}_f{fff:03d}_10m_uv.grib2"
        if out.exists() and out.stat().st_size > 0:
            logger.info(f"Exists: {out.name}")
            saved.append(out)
            continue
        logger.info(f"GET {url}")
        if try_download(client, url, out):
            saved.append(out)
        else:
            # if the first few hours fail, likely the whole cycle isn't ready
            if fff in (0, 3, 6):
                logger.warning(f"Early forecast hour f{fff:03d} failed; this cycle may not be staged yet.")
                client.close()
                return []
        time.sleep(0.5)
    client.close()
    return saved

def main():
    bbox = tuple(map(float, os.getenv("GFS_BBOX", "28,36,31,35").split(",")))  # lon_min, lon_max, lat_min, lat_max
    horizon = int(os.getenv("GFS_HOURS", "24"))
    step = int(os.getenv("GFS_STEP", "3"))
    hours = list(range(0, horizon + 1, step))
    out_dir = Path(os.getenv("GFS_OUT", "data/raw/gfs"))

    # Try latest cycle, then step back by 6h up to 18h
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    candidates = [now - dt.timedelta(hours=6*k) for k in range(0, 4)]  # now, -6h, -12h, -18h
    logger.info(f"BBOX={bbox} HOURS={hours}")

    for ts in candidates:
        ymd, hh = cycle_str(ts)
        logger.info(f"Trying GFS cycle {ymd} {hh}Z")
        saved = download_cycle(ymd, hh, hours, bbox, out_dir)
        if saved:
            logger.info(f"Downloaded {len(saved)} files from cycle {ymd} {hh}Z")
            return
        else:
            logger.info(f"No files from cycle {ymd} {hh}Z, trying previous cycle…")

    raise SystemExit("GFS download failed for all recent cycles. Try again later or widen the bbox.")

if __name__ == "__main__":
    main()
