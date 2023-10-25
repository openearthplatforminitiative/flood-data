import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from flood.spark.transforms import (create_round_udf, 
                                    compute_flood_tendency,
                                    compute_flood_intensity,
                                    compute_flood_peak_timing,
                                    compute_flood_threshold_percentages)
from flood.utils.config import get_config_val

CONFIG_FILE_PATH = './databricks/config.json'

class TestSparkUtilities(unittest.TestCase):
    """
    To run this test, run the following command from the root directory:
    >>> python3 -m unittest test.test_spark_transforms

    To run all tests, run the following command from the root directory:
    >>> python3 -m unittest discover test
    """

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for the tests
        cls.spark = SparkSession.builder \
            .appName("Spark Utilities Tests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession after all tests
        cls.spark.stop()

    # Helper method
    def get_conf_val(self, key):
        return get_config_val(key, config_filename=CONFIG_FILE_PATH)

    # @unittest.skip("Skipping test_rounding")
    def test_rounding(self):
        # Test the rounding functionality
        data = [Row(value=19.0750000024), Row(value=17.324999994)]
        df = self.spark.createDataFrame(data)
        round_udf = create_round_udf()
        rounded_df = df.withColumn("rounded_value", round_udf("value"))
        results = rounded_df.collect()

        # Check the results
        self.assertAlmostEqual(results[0].rounded_value, 19.075, 3)
        self.assertAlmostEqual(results[1].rounded_value, 17.325, 3)

    # @unittest.skip("Skipping test_dataframe_join")
    def test_dataframe_join(self):
        # Test that joining of several dataframes after performing rounding 
        # results in the expected dataframe.
        data1 = [Row(lat=19.0750000024, lon=17.324999994, val1=5)]
        data2 = [Row(lat=19.0749999999, lon=17.325000006, val2=10)]
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)
        precision = 3

        # Attempt to join without rounding
        joined_df_without_rounding = df1.join(df2, on=['lat', 'lon'], how='inner')
        results_without_rounding = joined_df_without_rounding.collect()

        # Check that the result is empty, which indicates no matching rows
        self.assertEqual(len(results_without_rounding), 0)
        
        # Apply rounding UDF
        round_udf = create_round_udf(precision=precision)
        df1_rounded = df1.withColumn("lat", round_udf("lat")).withColumn("lon", round_udf("lon"))
        df2_rounded = df2.withColumn("lat", round_udf("lat")).withColumn("lon", round_udf("lon"))
        
        # Join the dataframes after rounding
        joined_df = df1_rounded.join(df2_rounded, on=['lat', 'lon'], how='inner')
        results = joined_df.collect()

        # Check the results
        self.assertEqual(len(results), 1)
        self.assertAlmostEqual(results[0].lat, 19.075, precision)
        self.assertAlmostEqual(results[0].lon, 17.325, precision)
        self.assertEqual(results[0].val1, 5)
        self.assertEqual(results[0].val2, 10)

    # @unittest.skip("Skipping test_compute_flood_tendency")
    def test_compute_flood_tendency(self):
        
        FLOOD_TENDENCIES = self.get_conf_val('GLOFAS_FLOOD_TENDENCIES')
        INCREASING_VAL = FLOOD_TENDENCIES['increasing']
        STAGNANT_VAL = FLOOD_TENDENCIES['stagnant']
        DECREASING_VAL = FLOOD_TENDENCIES['decreasing']
        TENDENCY_COL_NAME = 'tendency'

        SCHEMA = ["latitude", "longitude", "control_dis",
                  "control_time", "control_valid_time",
                  "time", "valid_time", "step",
                  "p_above_2y", "p_above_5y", "p_above_20y",
                  "min_dis", "Q1_dis", "median_dis", "Q3_dis", "max_dis"]

        # Create synthetic data for testing
        data = [
            # Data for 'increasing' tendency
            (0.0, 0.5, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.1, 0.2, 0.3, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.5, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.1, 0.2, 0.3, 10.0, 11.0, 12.5, 13.0, 14.0),
            (0.0, 0.5, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-06", 3, 0.1, 0.2, 0.3, 10.5, 11.5, 15.0, 16.0, 17.0),

            # Data for 'decreasing' tendency
            (1.25, 1.0, 20.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.1, 0.2, 0.3, 17.0, 18.0, 17.5, 20.0, 21.0),
            (1.25, 1.0, 20.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.1, 0.2, 0.3, 16.0, 17.0, 17.8, 20.0, 21.5),
            (1.25, 1.0, 20.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-06", 3, 0.1, 0.2, 0.3, 16.5, 17.5, 18.0, 20.5, 21.5),

            # Data for 'stagnant' tendency
            (2.0, 2.6, 25.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.1, 0.2, 0.3, 23.0, 24.0, 25.0, 26.0, 27.0),
            (2.0, 2.6, 25.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.1, 0.2, 0.3, 24.0, 25.0, 26.0, 27.0, 28.0),
            (2.0, 2.6, 25.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-07", 3, 0.1, 0.2, 0.3, 23.5, 24.5, 26.5, 27.5, 28.5),

            # Boundary cases - they should be considered as 'stagnant' based on the conditions provided
            (3.0, 3.1, 30.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.1, 0.2, 0.3, 29.0, 29.5, 33.0, 34.0, 35.0),  # Exactly 1.10 * control_dis
            (3.0, 3.1, 30.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.1, 0.2, 0.3, 20.0, 21.5, 27.001, 28.0, 29.0),  # Exactly > 0.90 * control_dis
        ]

        # Create DataFrame using the synthetic data
        df = self.spark.createDataFrame(data, SCHEMA)

        # Using the function
        result_df = compute_flood_tendency(df, FLOOD_TENDENCIES, col_name=TENDENCY_COL_NAME)

        # Get the tendency values for the synthetic data
        resulting_first_tendency = result_df.filter((result_df['latitude'] == 0.0) & (result_df['longitude'] == 0.5)).first()[TENDENCY_COL_NAME]
        resulting_second_tendency = result_df.filter((result_df['latitude'] == 1.25) & (result_df['longitude'] == 1.0)).first()[TENDENCY_COL_NAME]
        resulting_third_tendency = result_df.filter((result_df['latitude'] == 2.0) & (result_df['longitude'] == 2.6)).first()[TENDENCY_COL_NAME]
        resulting_fourth_tendency = result_df.filter((result_df['latitude'] == 3.0) & (result_df['longitude'] == 3.1)).first()[TENDENCY_COL_NAME]

        # Assertions to check if the functionality works as expected
        self.assertEqual(resulting_first_tendency, INCREASING_VAL)
        self.assertEqual(resulting_second_tendency, DECREASING_VAL)
        self.assertEqual(resulting_third_tendency, STAGNANT_VAL)
        self.assertEqual(resulting_fourth_tendency, STAGNANT_VAL)

    # @unittest.skip("Skipping test_compute_flood_intensity")
    def test_compute_flood_intensity(self):

        FLOOD_INTENSITIES = self.get_conf_val('GLOFAS_FLOOD_INTENSITIES')
        PURPLE = FLOOD_INTENSITIES['purple']
        RED = FLOOD_INTENSITIES['red']
        YELLOW = FLOOD_INTENSITIES['yellow']
        GRAY = FLOOD_INTENSITIES['gray']
        INTENSITY_COL_NAME = 'intensity'

        SCHEMA = ["latitude", "longitude", "control_dis",
                  "control_time", "control_valid_time", 
                  "time", "valid_time", "step",
                  "p_above_2y", "p_above_5y", "p_above_20y",
                  "min_dis", "Q1_dis", "median_dis", "Q3_dis", "max_dis"]

        # Create synthetic data for testing
        data = [
            # Data for 'PURPLE' intensity: max(p_above_20y) >= 0.30
            (0.0, 0.5, 20.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.55, 0.45, 0.3, 15.0, 16.0, 17.0, 18.0, 19.0),
            (0.0, 0.5, 20.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.15, 0.25, 0.25, 16.0, 17.0, 18.0, 19.0, 20.0),

            # Data for 'RED' intensity: max(p_above_5y) >= 0.30 and max(p_above_20y) < 0.30
            (1.4, 1.0, 25.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.5, 0.3, 0.29, 20.0, 21.0, 22.0, 23.0, 24.0),
            (1.4, 1.0, 25.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.31, 0.29, 0.29, 21.0, 22.0, 23.0, 24.0, 25.0),

            # Data for 'YELLOW' intensity: max(p_above_2y) >= 0.30, and others < 0.30
            (2.225, 2.0, 30.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.30, 0.29, 0.29, 25.0, 26.0, 27.0, 28.0, 29.0),
            (2.225, 2.0, 30.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.29, 0.28, 0.18, 26.0, 27.0, 28.0, 29.0, 30.0),

            # Data for 'GREY' intensity: all probabilities < 0.30
            (3.0, 3.925, 35.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.25, 0.2, 0.15, 30.0, 31.0, 32.0, 33.0, 34.0),
            (3.0, 3.925, 35.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.29, 0.29, 0.29, 31.0, 32.0, 33.0, 34.0, 35.0),
        ]

        # Create DataFrame using the synthetic data
        df = self.spark.createDataFrame(data, SCHEMA)

        # Using the function 
        result_df = compute_flood_intensity(df, FLOOD_INTENSITIES, col_name=INTENSITY_COL_NAME)

        # Get the intensity values for the synthetic data
        resulting_first_intensity = result_df.filter((result_df['latitude'] == 0.0) & (result_df['longitude'] == 0.5)).first()[INTENSITY_COL_NAME]
        resulting_second_intensity = result_df.filter((result_df['latitude'] == 1.4) & (result_df['longitude'] == 1.0)).first()[INTENSITY_COL_NAME]
        resulting_third_intensity = result_df.filter((result_df['latitude'] == 2.225) & (result_df['longitude'] == 2.0)).first()[INTENSITY_COL_NAME]
        resulting_fourth_intensity = result_df.filter((result_df['latitude'] == 3.0) & (result_df['longitude'] == 3.925)).first()[INTENSITY_COL_NAME]

        # Assertions to check if the functionality works as expected
        self.assertEqual(resulting_first_intensity, PURPLE)
        self.assertEqual(resulting_second_intensity, RED)
        self.assertEqual(resulting_third_intensity, YELLOW)
        self.assertEqual(resulting_fourth_intensity, GRAY)

    # @unittest.skip("Skipping test_compute_peak_timing")
    def test_compute_peak_timing(self):

        FLOOD_PEAK_TIMINGS = self.get_conf_val('GLOFAS_FLOOD_PEAK_TIMINGS')
        BLACK_BORDER = FLOOD_PEAK_TIMINGS['black_border']
        GRAYED_COLOR = FLOOD_PEAK_TIMINGS['grayed_color']
        GRAY_BORDER = FLOOD_PEAK_TIMINGS['gray_border']
        PEAK_TIMING_COL_NAME = 'peak_timing'

        SCHEMA = ["latitude", "longitude", "control_dis", "control_time",
                  "control_valid_time", "time", "valid_time", "step",
                  "p_above_2y", "p_above_5y", "p_above_20y",
                  "min_dis", "Q1_dis", "median_dis", "Q3_dis", "max_dis"]

        data_black_border = [

            # Day 1 has high severity with p_above_20y = 0.33
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.5, 0.4, 0.33, 9.0, 10.0, 11.0, 12.0, 13.0),

            # Day 2 has the highest severity with p_above_20y = 0.31 and the largest median discharge = 14.0
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.50, 0.45, 0.31, 12.0, 13.0, 14.0, 15.0, 16.0),
            
            # Days 3-30 have lower severity than Day 2
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-05", 3, 1.0, 1.0, 0.29, 8.0, 9.0, 10.0, 11.0, 12.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-06", 4, 1.0, 0.3, 0.22, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-07", 5, 1.0, 0.9, 0.0, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-08", 6, 1.0, 0.8, 0.19, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-09", 7, 1.0, 0.4, 0.14, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-10", 8, 1.0, 0.67, 0.27, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-11", 9, 1.0, 0.21, 0.21, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-12", 10, 1.0, 0.32, 0.21, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-13", 11, 1.0, 0.45, 0.11, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-14", 12, 1.0, 0.55, 0.25, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-15", 13, 1.0, 1.0, 0.22, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-16", 14, 1.0, 1.0, 0.29, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-17", 15, 1.0, 0.99, 0.21, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-18", 16, 1.0, 0.98, 0.14, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-19", 17, 1.0, 0.4, 0.23, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-20", 18, 1.0, 0.5, 0.02, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-21", 19, 1.0, 0.42, 0.28, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-22", 20, 1.0, 0.15, 0.0, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-23", 21, 1.0, 0.84, 0.20, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-24", 22, 1.0, 0.29, 0.1, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-25", 23, 1.0, 0.027, 0.22, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-26", 24, 1.0, 0.13, 0.19, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-27", 25, 1.0, 0.88, 0.25, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-28", 26, 1.0, 0.25, 0.16, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-29", 27, 1.0, 0.83, 0.26, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-30", 28, 1.0, 0.92, 0.04, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-31", 29, 1.0, 0.76, 0.21, 17.0, 18.0, 19.0, 20.0, 21.0),
            (2.275, 2.0, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-11-01", 30, 1.0, 0.1, 0.01, 17.0, 18.0, 19.0, 20.0, 21.0),
        ]

        data_grayed_color = [
            
            # Days 1-10 have p_above_2y < 0.30, p_above_5y < 0.30, and p_above_20y < 0.30
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.20, 0.2, 0.10, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.0, 0.2, 0.11, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-05", 3, 0.21, 0.27, 0.29, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-06", 4, 0.29, 0.01, 0.27, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-07", 5, 0.0, 0.2, 0.18, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-08", 6, 0.10, 0.11, 0.11, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-09", 7, 0.11, 0.16, 0.0, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-10", 8, 0.19, 0.25, 0.0, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-11", 9, 0.01, 0.0, 0.01, 9.0, 10.0, 11.0, 12.0, 13.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-12", 10, 0.23, 0.1, 0.05, 9.0, 10.0, 11.0, 12.0, 13.0),

            # Day 11 has the highest severity with p_above_5y = 0.30, and it's > 10 days into the forecast
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-13", 11, 0.31, 0.30, 0.29, 17.0, 18.0, 19.0, 20.0, 21.0),

            # Remaining days have p_above_2y < 0.30, p_above_5y < 0.30, and p_above_20y < 0.30
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-14", 12, 0.29, 0.29, 0.10, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-15", 13, 0.20, 0.28, 0.19, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-16", 14, 0.21, 0.22, 0.28, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-17", 15, 0.0, 0.1, 0.0, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-18", 16, 0.03, 0.0, 0.03, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-19", 17, 0.03, 0.24, 0.0, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 18, 0.1, 0.23, 0.27, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 19, 0.2, 0.22, 0.01, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 20, 0.21, 0.12, 0.06, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 21, 0.29, 0.16, 0.18, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 22, 0.12, 0.19, 0.19, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 23, 0.15, 0.07, 0.03, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 24, 0.19, 0.17, 0.12, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 25, 0.05, 0.06, 0.28, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 26, 0.04, 0.14, 0.04, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 27, 0.0, 0.27, 0.16, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 28, 0.1, 0.22, 0.11, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 29, 0.2, 0.21, 0.29, 17.0, 18.0, 19.0, 20.0, 21.0),
            (0.0, 0.975, 10.0, "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-20", 30, 0.24, 0.20, 0.04, 17.0, 18.0, 19.0, 20.0, 21.0),
        ]

        data_gray_border = [

            # Day 1 has p_above_2y < 0.30, p_above_5y < 0.30, and p_above_20y < 0.30
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03", 1, 0.22, 0.20, 0.10, 9.0, 10.0, 11.0, 12.0, 13.0),

            # Day 2 has p_above_2y > 0.30 (ensuring some severity in the first 10 days) but not exceeding the severity on Day 7
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-04", 2, 0.71, 0.5, 0.45, 9.1, 10.1, 11.1, 12.1, 13.1),

            # Days 3-6 have p_above_2y < 0.30, p_above_5y < 0.30, and p_above_20y < 0.30
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-05", 3, 0.29, 0.1, 0.06, 9.1, 10.1, 11.1, 12.1, 13.1),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-06", 4, 0.0, 0.2, 0.19, 9.1, 10.1, 11.1, 12.1, 13.1),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-07", 5, 0.25, 0.23, 0.21, 9.1, 10.1, 11.1, 12.1, 13.1),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-08", 6, 0.21, 0.20, 0.19, 9.1, 10.1, 11.1, 12.1, 13.1),

            # Day 7 has the highest severity with p_above_20y = 0.46
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-09", 7, 0.76, 0.53, 0.46, 12.0, 13.0, 14.0, 15.0, 16.0),

            # Days 8-10 have p_above_2y < 0.30, p_above_5y < 0.30, and p_above_20y < 0.30
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-10", 8, 0.2, 0.1, 0.0, 8.0, 9.0, 10.0, 11.0, 12.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-11", 9, 0.29, 0.16, 0.08, 7.0, 8.0, 9.0, 10.0, 11.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-12", 10, 0.21, 0.12, 0.1, 6.0, 7.0, 8.0, 9.0, 10.0),

            # Days 11-30 can have any values since they're out of the first 10-day period
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-13", 11, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-14", 12, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-15", 13, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-17", 14, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-18", 15, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-19", 16, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-20", 17, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-21", 19, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-22", 20, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-23", 21, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-24", 22, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-25", 23, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-26", 24, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-27", 25, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-28", 26, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-29", 27, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-30", 28, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-31", 29, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
            (1.475, 1.025, 10.0, "2023-10-01", "2023-10-02", "2023-10-02", "2023-11-01", 30, 0.25, 0.20, 0.05, 17.0, 18.0, 19.0, 20.0, 21.0),
        ]

        data = data_black_border + data_grayed_color + data_gray_border

        # Create DataFrame using the synthetic data
        df = self.spark.createDataFrame(data, SCHEMA)
        
        # Compute peak timing
        result_df = compute_flood_peak_timing(df, FLOOD_PEAK_TIMINGS, col_name=PEAK_TIMING_COL_NAME)

        # Get the peak timing values for the synthetic data
        resulting_first_peak_timing = result_df.filter((result_df['latitude'] == 2.275) & (result_df['longitude'] == 2.0)).first()[PEAK_TIMING_COL_NAME]
        resulting_second_peak_timing = result_df.filter((result_df['latitude'] == 0.0) & (result_df['longitude'] == 0.975)).first()[PEAK_TIMING_COL_NAME]
        resulting_third_peak_timing = result_df.filter((result_df['latitude'] == 1.475) & (result_df['longitude'] == 1.025)).first()[PEAK_TIMING_COL_NAME]

        # Assertions to check if the functionality works as expected
        self.assertEqual(resulting_first_peak_timing, BLACK_BORDER)
        self.assertEqual(resulting_second_peak_timing, GRAYED_COLOR)
        self.assertEqual(resulting_third_peak_timing, GRAY_BORDER)

    # @unittest.skip("Skipping test_compute_flood_threshold_percentages")
    def test_compute_flood_threshold_percentages(self):

        SCHEMA = ["latitude", "longitude", "time", "valid_time", "step", 
                  "p_above_2y", "p_above_5y", "p_above_20y", 
                  "min_dis", "Q1_dis", "median_dis", "Q3_dis", "max_dis"]
        
        # Create mock forecast dataframe
        forecast_data = [

            # Ensemble 1
            (1, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 100.0),
            (2, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 50.0),
            (3, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 25.0),
            (4, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 75.0),
            (5, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 15.0),
            (6, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 105.0),
            (7, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 35.0),
            (8, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 65.0),
            (9, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 90.0),
            (10, 0.5, 0.5, '2023-10-01 00:00:00', 1, '2023-10-02 00:00:00', 120.0),

            # Ensemble 2
            (1, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 11.0),
            (2, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 9.0),
            (3, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 22.0),
            (4, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 22.0),
            (5, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 14.0),
            (6, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 15.0),
            (7, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 13.0),
            (8, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 7.0),
            (9, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 8.0),
            (10, 2.5, 0.5, '2023-10-05 00:00:00', 17, '2023-10-22 00:00:00', 5.0),
        ]

        forecast_df = self.spark.createDataFrame(forecast_data, ['number', 'latitude', 'longitude', 'time', 'step', 'valid_time', 'dis24'])

        # Create mock threshold dataframe
        threshold_data = [
            (0.5, 0.5, 20.0, 50.0, 120.0),
            (2.5, 0.5, 9.0, 11.0, 15.0),
        ]
        threshold_df = self.spark.createDataFrame(threshold_data, ['latitude', 'longitude', '2y_threshold', '5y_threshold', '20y_threshold'])

        threshold_vals = [2, 5, 20]

        result_df_approx = compute_flood_threshold_percentages(forecast_df, threshold_df, threshold_vals, accuracy_mode='approx')
        result_df_exact = compute_flood_threshold_percentages(forecast_df, threshold_df, threshold_vals, accuracy_mode='exact')

        expected_data_approx = [
            (0.5, 0.5, '2023-10-01 00:00:00', '2023-10-02 00:00:00', 1, 0.9, 0.7, 0.1, 15.0, 35.0, 65.0, 100.0, 120.0),
            (2.5, 0.5, '2023-10-05 00:00:00', '2023-10-22 00:00:00', 17, 0.7, 0.6, 0.3, 5.0, 8.0, 11.0, 15.0, 22.0)
        ]
        expected_df_approx = self.spark.createDataFrame(expected_data_approx, SCHEMA)

        expected_data_exact = [
            (0.5, 0.5, '2023-10-01 00:00:00', '2023-10-02 00:00:00', 1, 0.9, 0.7, 0.1, 15.0, 38.75, 70.0, 97.5, 120.0),
            (2.5, 0.5, '2023-10-05 00:00:00', '2023-10-22 00:00:00', 17, 0.7, 0.6, 0.3, 5.0, 8.25, 12.0, 14.75, 22.0)
        ]
        expected_df_exact = self.spark.createDataFrame(expected_data_exact, SCHEMA)

        # Assert that the resulting dataframe matches the expected dataframe.
        # Sort result by latitude and longitude
        self.assertEqual(result_df_approx.sort('latitude', 'longitude').collect(), expected_df_approx.collect())
        self.assertEqual(result_df_exact.sort('latitude', 'longitude').collect(), expected_df_exact.collect())

if __name__ == '__main__':
    unittest.main()
