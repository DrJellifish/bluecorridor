#!/usr/bin/env python3
from __future__ import annotations
"""
OpenDrift run using stitched drivers_72h.nc.

Inputs:
  - data/interim/drivers_72h.nc  (u_curr, v_curr, u_wind, v_wind, u_stokes, v_stokes)
  - config/run_default.yaml       (bbox, start/horizon, release points, ensemble size, etc.)
  - config/particles_bottle.yaml  (windage / bottle params)

Outputs:
  - data/outputs/tracks_latest.geojson (LineString per particle)
  - data/outputs/summary_latest.csv
"""

from pathlib import Path
from datetime import timedelta, datetime, timezone
import json
import math
import yaml
import numpy as np
import xarray as xr

from opendrift.models.oceandrift import OceanDrift
from opendrift.readers.reader_netCDF_CF_generic import Reader as CFReader

from src.util.logging_setup import setup_logger
logger = setup_logger("bluecorridor.opendrift")

DRIVERS = Path("data/interim/drivers_72h.nc")
CF_DRIVERS = Path("data/interim/drivers_cf.nc")
OUT_GEOJSON = Path("data/outputs/tracks_latest.geojson")
OUT_SUMMARY = Path("data/outputs/summary_latest.csv")


def load_cfg():
    with open("config/run_default.yaml", "r") as f:
        run = yaml.safe_load(f)
    with open("config/particles_bottle.yaml", "r") as f:
        part = yaml.safe_load(f)
    return run, part


def round_hour_utc(dt0: datetime | None = None) -> datetime:
    now = dt0 or datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0)


def make_cf_copy(infile: Path, outfile: Path) -> Path:
    """Map our variable names to CF names that the generic reader understands."""
    ds = xr.load_dataset(infile)

    # Normalize coord names
    ren = {}
    if "longitude" in ds.coords: ren["longitude"] = "lon"
    if "latitude" in ds.coords:  ren["latitude"] = "lat"
    ds = ds.rename(ren)

    mapping = {
        "u_curr": "eastward_sea_water_velocity",
        "v_curr": "northward_sea_water_velocity",
        "u_wind": "x_wind",
        "v_wind": "y_wind",
        "u_stokes": "sea_surface_wave_stokes_drift_x_velocity",
        "v_stokes": "sea_surface_wave_stokes_drift_y_velocity",
    }
    present = {k: v for k, v in mapping.items() if k in ds}
    ds_cf = ds.rename(present)

    # Minimal units
    for v in ds_cf.data_vars:
        if v.endswith("_velocity") or v in ("x_wind", "y_wind"):
            ds_cf[v].attrs.setdefault("units", "m s-1")

    outfile.parent.mkdir(parents=True, exist_ok=True)
    ds_cf.to_netcdf(outfile)
    return outfile


def seed_from_config(o: OceanDrift, run_cfg: dict):
    pts = run_cfg.get("release", {}).get("points", [])
    if not pts:
        raise SystemExit("No release points set in config/run_default.yaml: release.points")
    lats = np.array([float(p[0]) for p in pts])
    lons = np.array([float(p[1]) for p in pts])

    members = int(run_cfg.get("ensemble", {}).get("members", 20))
    t0 = round_hour_utc()
    spread = int(run_cfg.get("release", {}).get("time_spread_min", 0))

    for i in range(len(pts)):
        if members <= 1 or spread <= 0:
            times = np.array([t0], dtype="datetime64[ns]")
            n = max(1, members)
        else:
            times = np.array(
                [t0 + timedelta(minutes=int(k * spread / (members - 1))) for k in range(members)],
                dtype="datetime64[ns]"
            )
            n = members
        o.seed_elements(lon=float(lons[i]), lat=float(lats[i]), number=n, time=times)


def to_linestring_geojson(lon: np.ndarray, lat: np.ndarray, tlist: list[datetime]) -> dict:
    """
    Build one LineString per particle (skipping NaNs).
    lon/lat shape: [time, elements]
    """
    features = []
    n_el = lon.shape[1]
    for i in range(n_el):
        coords = []
        times = []
        for j in range(lon.shape[0]):
            x = float(lon[j, i])
            y = float(lat[j, i])
            if not (math.isfinite(x) and math.isfinite(y)):
                continue
            coords.append([x, y])
            times.append(tlist[j].replace(tzinfo=timezone.utc).isoformat())
        if len(coords) >= 2:
            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {"id": int(i), "times": times}
            })
    return {"type": "FeatureCollection", "features": features}


def main():
    if not DRIVERS.exists():
        raise SystemExit(f"Missing drivers file: {DRIVERS}")

    run_cfg, part_cfg = load_cfg()

    # 1) Prep CF-mapped drivers file
    cf_nc = make_cf_copy(DRIVERS, CF_DRIVERS)

    # 2) Create model + add reader
    o = OceanDrift(loglevel=20)  # INFO
    reader = CFReader(str(cf_nc))
    o.add_reader(reader)

    # 3) Config: coastline & windage & Stokes
    # Stop at land:
    o.set_config('general:coastline_action', 'stranding')
    # Allow seeding near coast if needed:
    if run_cfg.get("release", {}).get("allow_land", False):
        o.set_config('seed:ocean_only', False)

    # Windage (% of 10m wind speed)
    windage_pct = float(part_cfg.get("windage_pct", 2.0))  # try 2–5%
    o.set_config('seed:wind_drift_factor', windage_pct / 100.0)

    # Stokes drift if variables exist
    have_stokes = (
        'sea_surface_wave_stokes_drift_x_velocity' in reader.variables and
        'sea_surface_wave_stokes_drift_y_velocity' in reader.variables
    )
    o.set_config('drift:stokes_drift', bool(have_stokes))

    # 4) Seed
    seed_from_config(o, run_cfg)

    # 5) Run
    horizon_h = int(run_cfg.get("time", {}).get("horizon_hours", 72))
    o.run(duration=timedelta(hours=horizon_h),
          time_step=3600,                    # seconds
          outfile=None)

    # 6) Export tracks
    Path("data/outputs").mkdir(parents=True, exist_ok=True)
    lon = o.get_property('lon')   # [time, elements]
    lat = o.get_property('lat')
    tlist = o.get_time_array()    # list[datetime]

    geo = to_linestring_geojson(lon, lat, tlist)
    with open(OUT_GEOJSON, "w") as f:
        json.dump(geo, f)

    # 7) Summary
    with open(OUT_SUMMARY, "w") as f:
        f.write("metric,value\n")
        f.write(f"n_particles,{lon.shape[1]}\n")
        f.write(f"horizon_hours,{horizon_h}\n")
        f.write(f"windage_pct,{windage_pct}\n")
        f.write(f"use_stokes,{int(have_stokes)}\n")

    logger.info(f"Wrote → {OUT_GEOJSON}")
    logger.info(f"Wrote → {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
