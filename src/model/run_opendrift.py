#!/usr/bin/env python3
"""
OpenDrift run using our stitched drivers_72h.nc.

Inputs:
  - data/interim/drivers_72h.nc  (u_curr, v_curr, u_wind, v_wind, u_stokes, v_stokes)
  - config/run_default.yaml       (bbox, start/horizon, release points, ensemble size, etc.)
  - config/particles_bottle.yaml  (windage / leeway-ish params)

Outputs:
  - data/outputs/tracks_latest.geojson
  - data/outputs/summary_latest.csv
"""

from __future__ import annotations
from pathlib import Path
import json
import math
import yaml
import numpy as np
import xarray as xr
from datetime import timedelta, datetime, timezone

from opendrift.models.oceandrift import OceanDrift
from opendrift.readers.reader_netCDF_CF_generic import Reader as CFReader

from src.util.logging_setup import setup_logger
logger = setup_logger("bluecorridor.opendrift")

DRIVERS = Path("data/interim/drivers_72h.nc")
OUT_GEOJSON = Path("data/outputs/tracks_latest.geojson")
OUT_SUMMARY = Path("data/outputs/summary_latest.csv")


def _load_cfg():
    with open("config/run_default.yaml", "r") as f:
        run = yaml.safe_load(f)
    with open("config/particles_bottle.yaml", "r") as f:
        part = yaml.safe_load(f)
    return run, part


def _round_hour_utc(dt0: datetime | None = None) -> datetime:
    now = dt0 or datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0)


def _make_cf_copy(infile: Path, outfile: Path) -> Path:
    """
    Map our variable names to CF names that Generic CF reader understands.
    """
    ds = xr.load_dataset(infile)

    # Ensure standard coord names
    rename = {}
    if "longitude" in ds.coords: rename["longitude"] = "lon"
    if "latitude" in ds.coords:  rename["latitude"] = "lat"
    ds = ds.rename(rename)

    # Map to CF-ish names expected by the generic reader
    mapping = {
        "u_curr": "eastward_sea_water_velocity",
        "v_curr": "northward_sea_water_velocity",
        "u_wind": "x_wind",
        "v_wind": "y_wind",
        # CMEMS waves: we already mapped to u_stokes/v_stokes in stitcher;
        # CF standard names for Stokes drift are below:
        "u_stokes": "sea_surface_wave_stokes_drift_x_velocity",
        "v_stokes": "sea_surface_wave_stokes_drift_y_velocity",
    }
    ds_cf = ds.rename({k: v for k, v in mapping.items() if k in ds})

    # Minimal CF attrs to help the reader
    for v in ds_cf.data_vars:
        if v.endswith("_velocity"):
            ds_cf[v].attrs.setdefault("units", "m s-1")

    outfile.parent.mkdir(parents=True, exist_ok=True)
    ds_cf.to_netcdf(outfile)
    return outfile


def _seed_from_config(o: OceanDrift, run_cfg: dict):
    # Release points are lat,lon pairs in config; convert to lon/lat arrays
    pts = run_cfg.get("release", {}).get("points", [])
    if not pts:
        raise SystemExit("No release points set in config/run_default.yaml: release.points")
    lats = np.array([p[0] for p in pts], dtype=float)
    lons = np.array([p[1] for p in pts], dtype=float)

    members = int(run_cfg.get("ensemble", {}).get("members", 20))
    t0 = _round_hour_utc()
    time_spread = int(run_cfg.get("release", {}).get("time_spread_min", 0))

    # Seed N particles per site, optionally with a small time spread
    for i in range(len(pts)):
        times = np.array([t0 + timedelta(minutes=int(k * time_spread / max(1, members-1)))
                          for k in range(members)], dtype="datetime64[ns]")
        o.seed_elements(lon=float(lons[i]),
                        lat=float(lats[i]),
                        number=members,
                        time=times)


def main():
    if not DRIVERS.exists():
        raise SystemExit(f"Missing drivers file: {DRIVERS}")

    run_cfg, part_cfg = _load_cfg()

    # 1) Prepare a CF-mapped copy of drivers for the generic reader
    cf_nc = Path("data/interim/drivers_cf.nc")
    _make_cf_copy(DRIVERS, cf_nc)

    # 2) Build model and add environment readers
    o = OceanDrift(loglevel=20)  # INFO
    reader = CFReader(str(cf_nc))
    o.add_reader(reader)

    # 3) Particle/windage parameters
    # For bottle-rafts, treat windage as a simple percentage of 10m wind speed.
    # OpenDrift's OceanDrift supports adding a wind_drift_factor:
    windage_pct = float(part_cfg.get("windage_pct", 2.0))  # e.g., 0–5% typical
    o.set_config('drift:wind_drift_factor', windage_pct / 100.0)

    # Optionally include Stokes drift if present
    use_stokes = 'sea_surface_wave_stokes_drift_x_velocity' in reader.variables and \
                 'sea_surface_wave_stokes_drift_y_velocity' in reader.variables
    o.set_config('drift:include_stokes_drift', bool(use_stokes))

    # Beaching control (very simple; OpenDrift also has stranding)
    coastal_buffer_m = int(run_cfg.get("beaching", {}).get("coastal_buffer_m", 200))
    o.set_config('general:coastline_action', 'stranding')  # stop at coast

    # 4) Seed
    _seed_from_config(o, run_cfg)

    # 5) Run
    horizon_h = int(run_cfg.get("time", {}).get("horizon_hours", 72))
    o.run(duration=timedelta(hours=horizon_h),
          time_step=3600,            # seconds
          outfile=None)              # in-memory; we'll export ourselves

    # 6) Export GeoJSON (points per time-step)
    Path("data/outputs").mkdir(parents=True, exist_ok=True)
    lon = o.get_property('lon')      # shape: [time, elements]
    lat = o.get_property('lat')
    t = o.get_time_array()           # list[datetime]

    features = []
    for j, tj in enumerate(t):
        for i in range(lon.shape[1]):
            if np.isnan(lon[j, i]) or np.isnan(lat[j, i]):
                continue
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon[j, i]), float(lat[j, i])]},
                "properties": {"t": tj.replace(tzinfo=timezone.utc).isoformat(), "id": int(i)}
            })

    geo = {"type": "FeatureCollection", "features": features}
    with open(OUT_GEOJSON, "w") as f:
        json.dump(geo, f)

    # 7) Quick summary
    with open(OUT_SUMMARY, "w") as f:
        f.write("metric,value\n")
        f.write(f"n_particles,{lon.shape[1]}\n")
        f.write(f"horizon_hours,{horizon_h}\n")
        f.write(f"windage_pct,{windage_pct}\n")
        f.write(f"use_stokes,{int(use_stokes)}\n")

    logger.info(f"Wrote → {OUT_GEOJSON}")
    logger.info(f"Wrote → {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
