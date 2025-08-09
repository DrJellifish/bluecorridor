import xarray as xr
import numpy as np
from src.util.logging_setup import setup_logger
from src.util.io import save_netcdf
from src.util.grids import align_time

logger = setup_logger()

def main():
    # Placeholder synthetic drivers (replace with real merges)
    time = xr.date_range("2025-01-01", periods=72, freq="h")
    zeros = np.zeros(72)
    ds = xr.Dataset(
        {
            "u_curr": (("time"), zeros),
            "v_curr": (("time"), zeros),
            "u_stokes": (("time"), zeros),
            "v_stokes": (("time"), zeros),
            "u_wind": (("time"), zeros),
            "v_wind": (("time"), zeros),
        },
        coords={"time": time}
    )
    ds = align_time(ds)
    save_netcdf(ds, "data/interim/drivers_72h.nc")
    logger.info("Stitched drivers → data/interim/drivers_72h.nc")

if __name__ == "__main__":
    main()
