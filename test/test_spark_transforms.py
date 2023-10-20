import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from flood.spark.transforms import create_round_udf

class TestSparkUtilities(unittest.TestCase):

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

if __name__ == '__main__':
    unittest.main()
