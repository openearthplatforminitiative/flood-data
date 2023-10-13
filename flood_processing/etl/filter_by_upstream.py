from flood_processing.etl.utils import load_dataset

def get_filtered_discharge_from_files(discharge_file_path, upstream_file_path, threshold_area=250000, discharge_engine="cfgrib", upstream_engine="netcdf4"):

    ds_discharge = load_dataset(discharge_file_path, engine=discharge_engine)
    ds_upstream = load_dataset(upstream_file_path, engine=upstream_engine)

    return apply_upstream_threshold(ds_discharge, ds_upstream, threshold_area=threshold_area)

def apply_upstream_threshold(ds_discharge, ds_upstream, threshold_area=250000):

    buffer = 0.001  # A small buffer value to ensure that the discharge and upstream datasets overlap
    subset_uparea = ds_upstream['uparea'].sel(
        latitude=slice(ds_discharge.latitude.max() + buffer, ds_discharge.latitude.min() - buffer),
        longitude=slice(ds_discharge.longitude.min() - buffer, ds_discharge.longitude.max() + buffer))
    
    subset_uparea_aligned = subset_uparea.reindex(latitude=ds_discharge['dis24'].latitude, 
                                                  longitude=ds_discharge['dis24'].longitude, 
                                                  method='nearest')
    
    mask = subset_uparea_aligned >= threshold_area

    ds_discharge['dis24'] = ds_discharge['dis24'].where(mask)

    return ds_discharge