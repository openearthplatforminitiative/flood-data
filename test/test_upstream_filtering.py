import unittest
import numpy as np
from test.data.data_generation import (generate_upstream_filtering_test_data,
                                       create_ground_truth_upstream_filtering_dataframe)
from flood.etl.filter_by_upstream import apply_upstream_threshold
from flood.etl.raster_converter import RasterConverter

class TestUpstreamFiltering(unittest.TestCase):
    """
    To run this test, run the following command from the root directory:
    >>> python3 -m unittest test.test_upstream_filtering

    To run all tests, run the following command from the root directory:
    >>> python3 -m unittest discover test
    """

    def test_filter_discharge_by_uparea_simple_case(self):

        seed = 42
        num_forecasts = 50
        num_steps = 30
        upstream_threshold = 250000.0
        fill_upstream_threshold = 300000
        fill_discharge = 100.0
        num_random_cells = 100
        discharge_latitudes = np.linspace(5.725, -5.025, 216)
        discharge_longitudes = np.linspace(28.975, 40.725, 236)
        upstream_latitudes = np.linspace(89.975, -59.975, 3000)
        upstream_longitudes = np.linspace(-179.975, 179.975, 7200)

        converter = RasterConverter()

        ds_discharge, ds_upstream, random_lat_indices, random_lon_indices = \
            generate_upstream_filtering_test_data(discharge_latitudes, discharge_longitudes,
                                                  upstream_latitudes, upstream_longitudes, 
                                                  num_forecasts=num_forecasts, num_steps=num_steps, 
                                                  num_random_cells=num_random_cells, fill_discharge=fill_discharge, 
                                                  fill_upstream_threshold=fill_upstream_threshold, seed=seed)

        ground_truth_df = create_ground_truth_upstream_filtering_dataframe(ds_discharge, 
                                                                           random_lat_indices, 
                                                                           random_lon_indices, 
                                                                           upstream_latitudes,
                                                                           upstream_longitudes, 
                                                                           fill_discharge=fill_discharge)

        ground_truth_df = ground_truth_df.sort_values(by=list(ground_truth_df.columns), ascending=False)

        filtered_ds = apply_upstream_threshold(ds_discharge, ds_upstream, threshold_area=upstream_threshold)

        filtered_df = converter.dataset_to_dataframe(filtered_ds['dis24'], drop_na_subset=['dis24'], drop_index=False)

        filtered_df = filtered_df.sort_values(by=list(filtered_df.columns), ascending=False)

        self.assertTrue(np.isclose(ground_truth_df.values, filtered_df.values, atol=1e-6).all(), "DataFrames do not match!")

