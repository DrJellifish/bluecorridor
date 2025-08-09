from pathlib import Path
import xarray as xr

def save_netcdf(ds: xr.Dataset, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path)

def open_netcdf(path: str) -> xr.Dataset:
    return xr.load_dataset(path)
