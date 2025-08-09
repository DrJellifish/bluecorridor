# src/fetch/stitch_fields.py
from __future__ import annotations
import os, glob, math, datetime as dt
from pathlib import Path
import numpy as np
import xarray as xr

from src.util.logging_setup import setup_logger
from src.util.io import save_netcdf
logger = setup_logger("bluecorridor.stitch")

RAW_DIR = Path("data/raw")
OUT_NC = Path("data/interim/drivers_72h.nc")

def _find_var(ds, candidates):
    for name in candidates:
        if name in ds:
            return name
    raise KeyError(f"None of {candidates} found in {list(ds.data_vars)}")

def _load_cmems_stokes() -> xr.Dataset:
    f = RAW_DIR / "cmems_wav_stokes.nc"
    ds = xr.open_dataset(f)
    # Add VSDX/VSDY here:
    u_name = _find_var(ds, ["ustokes", "uuss", "us", "VSDX", "vsdx"])
    v_name = _find_var(ds, ["vstokes", "vvss", "vs", "VSDY", "vsdy"])
    ds = ds.rename({u_name: "u_stokes", v_name: "v_stokes"})
    if "time" not in ds.coords:
        tname = [c for c in ds.coords if "time" in c][0]
        ds = ds.rename({tname: "time"})
    return ds[["u_stokes", "v_stokes"]]


def _load_cmems_stokes() -> xr.Dataset:
    f = RAW_DIR / "cmems_wav_stokes.nc"
    ds = xr.open_dataset(f)
    u_name = _find_var(ds, ["ustokes", "uuss", "us"])
    v_name = _find_var(ds, ["vstokes", "vvss", "vs"])
    ds = ds.rename({u_name: "u_stokes", v_name: "v_stokes"})
    if "time" not in ds.coords:
        tname = [c for c in ds.coords if "time" in c][0]
        ds = ds.rename({tname: "time"})
    # Some wave datasets store lat/lon as 1D; make sure consistent with currents
    return ds[["u_stokes", "v_stokes"]]

def _gfs_to_dataset(grib_files: list[str]) -> xr.Dataset:
    """Read GFS 10m winds from GRIB2 via cfgrib and build u_wind/v_wind on 'valid_time'."""
    if not grib_files:
        raise SystemExit("No GFS GRIB2 files in data/raw/gfs/")
    dss = []
    for fp in sorted(grib_files):
        # Each file has both 10u and 10v, but cfgrib usually needs separate opens
        ds_u = xr.open_dataset(
            fp, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "shortName": "10u"}}
        )
        ds_v = xr.open_dataset(
            fp, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "shortName": "10v"}}
        )
        # cfgrib usually has coords: time (cycle), step (forecast lead), latitude, longitude
        # Build a valid_time = time + step
        vt = (ds_u["time"].values + ds_u["step"].values).astype("datetime64[ns]")
        ds = xr.Dataset(
            {
                "u_wind": (("valid_time", "latitude", "longitude"), ds_u["u10"].values[None, ...]),
                "v_wind": (("valid_time", "latitude", "longitude"), ds_v["v10"].values[None, ...]),
            },
            coords={
                "valid_time": [vt],
                "latitude": ds_u["latitude"].values,
                "longitude": ds_u["longitude"].values,
            },
        )
        dss.append(ds)
        ds_u.close(); ds_v.close()
    gfs = xr.concat(dss, dim="valid_time").sortby("valid_time")
    # Normalize coord names to 'lat'/'lon' like CMEMS often uses
    gfs = gfs.rename({"latitude": "lat", "longitude": "lon"})
    return gfs

def _to_hourly(ds: xr.Dataset, time_name: str = "time") -> xr.Dataset:
    if time_name not in ds.coords:
        raise ValueError(f"Dataset missing time coord: {time_name}")
    # Regular hourly grid over min..max
    t0 = np.array(ds[time_name].values).min()
    t1 = np.array(ds[time_name].values).max()
    hourly = xr.date_range(str(t0), str(t1), freq="1H")
    return ds.interp({time_name: hourly})

def main():
    # --- Load CMEMS fields (target grid & time base) ---
    ds_curr = _load_cmems_currents()
    ds_stok = _load_cmems_stokes()

    # Harmonize lat/lon naming
    def rename_latlon(ds):
        m = {}
        if "latitude" in ds.coords: m["latitude"] = "lat"
        if "longitude" in ds.coords: m["longitude"] = "lon"
        return ds.rename(m)
    ds_curr = rename_latlon(ds_curr)
    ds_stok = rename_latlon(ds_stok)

    # Ensure same lat/lon dims between cmems datasets
    for d in ("lat", "lon"):
        if not np.array_equal(ds_curr[d], ds_stok[d]):
            # Regrid stokes to currents grid if needed
            ds_stok = ds_stok.interp({ "lat": ds_curr["lat"], "lon": ds_curr["lon"] })

    # Hourly CMEMS time for a clean target axis
    ds_curr_h = _to_hourly(ds_curr, "time")
    ds_stok_h = ds_stok.interp(time=ds_curr_h.time)

    # --- Load GFS winds and project to CMEMS grid/time ---
    gribs = sorted(glob.glob(str(RAW_DIR / "gfs" / "*.grib2")))
    gfs = _gfs_to_dataset(gribs)  # has coords: valid_time, lat, lon
    # Interp winds to CMEMS time & grid
    gfs_h = gfs.interp(
        valid_time=ds_curr_h.time,
        lat=ds_curr_h.lat,
        lon=ds_curr_h.lon,
        method="linear"
    ).rename({"valid_time": "time"})

    # --- Merge all drivers ---
    drivers = xr.Dataset(
        data_vars=dict(
            u_curr=ds_curr_h["u_curr"],
            v_curr=ds_curr_h["v_curr"],
            u_stokes=ds_stok_h["u_stokes"],
            v_stokes=ds_stok_h["v_stokes"],
            u_wind=gfs_h["u_wind"],
            v_wind=gfs_h["v_wind"],
        ),
        coords=dict(
            time=ds_curr_h["time"],
            lat=ds_curr_h["lat"],
            lon=ds_curr_h["lon"],
        ),
        attrs=dict(
            title="Blue Corridor Surface Drivers",
            notes="CMEMS physics & waves interpolated hourly; GFS 10m winds interpolated to CMEMS grid/time",
        )
    )

    OUT_NC.parent.mkdir(parents=True, exist_ok=True)
    save_netcdf(drivers, str(OUT_NC))
    logger.info(f"Wrote → {OUT_NC}  (vars: {list(drivers.data_vars)})")

if __name__ == "__main__":
    main()
