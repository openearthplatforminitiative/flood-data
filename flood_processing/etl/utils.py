import xarray as xr

def load_dataset(file_path, engine=None):
    if engine is None:
        engine = determine_engine(file_path)
    return xr.open_dataset(file_path, engine=engine)

def determine_engine(file_path):
    if file_path.endswith(".grib"):
        return "cfgrib"
    elif file_path.endswith(".nc"):
        return "netcdf4"
    else:
        raise ValueError(f"Unrecognized file extension for {file_path}")