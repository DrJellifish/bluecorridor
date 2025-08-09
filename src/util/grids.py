import xarray as xr

def align_time(ds: xr.Dataset, freq="1H"):
    return ds.resample(time=freq).interpolate("linear")

def merge_drivers(drivers: list) -> xr.Dataset:
    return xr.merge(drivers, compat="override", join="outer")
