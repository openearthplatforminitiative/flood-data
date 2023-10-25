from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def round_to_precision(value, precision):
    """
    Round a value to the given number of decimal places.
    """
    return round(value, precision)

def create_round_udf(precision=3):
    """
    Create a UDF for rounding to the specified precision.
    """
    return F.udf(lambda x: round_to_precision(x, precision), DoubleType())

def compute_flood_tendency(df, flood_tendencies, col_name='tendency'):

    # Compute flood tendency once per grid cell
    grid_cell_tendency = (
        df
        .groupBy("latitude", "longitude")
        .agg(
            F.max('median_dis').alias('max_median_dis'),
            F.min('median_dis').alias('min_median_dis'),
            F.first('control_dis').alias('control_dis')
        )
    )

    # Define the tendency based on aggregated values
    tendency_condition = F.when(
        F.col('max_median_dis') > F.col('control_dis') * 1.10, 
        flood_tendencies['increasing']
    ).when(
        (F.col('min_median_dis') <= F.col('control_dis') * 0.90) &
        (F.col('max_median_dis') <= F.col('control_dis') * 1.10),
        flood_tendencies['decreasing']
    ).otherwise(flood_tendencies['stagnant'])

    return grid_cell_tendency.withColumn(col_name, tendency_condition)

def compute_flood_intensity(df, flood_intensities, col_name='intensity'):

    # Compute flood intensity once per grid cell
    grid_cell_intensity = (
        df
        .groupBy("latitude", "longitude")
        .agg(
            F.max('p_above_20y').alias('max_p_above_20y'),
            F.max('p_above_5y').alias('max_p_above_5y'),
            F.max('p_above_2y').alias('max_p_above_2y')
        )
    )

    # Define the color (flood intensity) based on aggregated values
    color_condition = F.when(
        grid_cell_intensity['max_p_above_20y'] >= 0.30, 
        flood_intensities['purple']
    ).when(
        (grid_cell_intensity['max_p_above_5y'] >= 0.30),
        flood_intensities['red']
    ).when(
        (grid_cell_intensity['max_p_above_2y'] >= 0.30),
        flood_intensities['yellow']
    ).otherwise(flood_intensities['gray'])

    return grid_cell_intensity.withColumn(col_name, color_condition)

def compute_flood_peak_timing(df, flood_peak_timings, col_name='peak_timing'):
    # 1. Filter rows between steps 1 to 10
    df_filtered = df.filter((F.col("step").between(1, 10)))
    
    # 2. Compute the maximum flood probability above the 2 year return period threshold for the first ten days
    df_max = df_filtered.groupBy("latitude", "longitude").agg(F.max("p_above_2y").alias("max_2y_start"))
    
    # 3. Join the max probabilities back to the main DataFrame
    df = df.join(df_max, ["latitude", "longitude"], "left")

   # Determine the conditions for each scenario
    df = df.withColumn("condition",
                       F.when(F.col("p_above_20y") >= 0.3, F.lit(1))
                        .when(F.col("p_above_5y") >= 0.3, F.lit(2))
                        .otherwise(F.lit(3)))

    # 4. Compute the step_of_highest_severity
    windowSpec = Window.partitionBy("latitude", "longitude")
    df = df.withColumn("peak_step", F.first("step").over(windowSpec.orderBy([F.asc("condition"), F.desc("median_dis")])))

    # 5. Determine the peak_timing column
    peak_condition = F.when(
        F.col("peak_step").between(1, 3), 
        flood_peak_timings['black_border']
    ).when(
        (F.col("peak_step") > 10) & (F.col("max_2y_start") < 0.30), 
        flood_peak_timings['grayed_color']
    ).otherwise(flood_peak_timings['gray_border'])
    
    df = df.withColumn(col_name, peak_condition)

    return df.select("latitude", "longitude", "peak_step", col_name).distinct()

# Define a function to compute the percentage exceeding thresholds for the forecasts dataframe
def compute_flood_threshold_percentages(forecast_df, threshold_df, threshold_vals, accuracy_mode='approx'):

    assert accuracy_mode in ['approx', 'exact'], "Accuracy mode must be either 'approx' or 'exact'."

    threshold_cols = [f"{int(threshold)}y_threshold" for threshold in threshold_vals]
    
    # Join forecast dataframe with threshold dataframe on latitude and longitude
    joined_df = forecast_df.join(threshold_df, on=['latitude', 'longitude'])
    
    for threshold, col_name in zip(threshold_vals, threshold_cols):
        exceed_col = f"exceed_{int(threshold)}y"
        joined_df = joined_df.withColumn(exceed_col, F.when(joined_df['dis24'] >= joined_df[col_name], 1).otherwise(0))
    
    # Aggregate to compute percentages
    agg_exprs = [
        F.mean(f"exceed_{int(threshold)}y").alias(f"p_above_{int(threshold)}y")
        for threshold in threshold_vals
    ]

    # Precompute values
    q1_dis = F.percentile_approx('dis24', 0.25).alias('Q1_dis') if accuracy_mode == 'approx'\
             else F.expr("percentile(dis24, array(0.25))")[0].alias('Q1_dis')
    median_dis = F.percentile_approx('dis24', 0.5).alias('median_dis') if accuracy_mode == 'approx'\
             else F.expr("percentile(dis24, array(0.5))")[0].alias('median_dis')
    q3_dis = F.percentile_approx('dis24', 0.75).alias('Q3_dis') if accuracy_mode == 'approx'\
             else F.expr("percentile(dis24, array(0.75))")[0].alias('Q3_dis')

    # Add 5-number summary computations for 'dis24' column
    agg_exprs.extend([
        F.min('dis24').alias('min_dis'),
        q1_dis,
        median_dis,
        q3_dis,
        F.max('dis24').alias('max_dis')
    ])
    
    results = joined_df.groupBy('latitude', 'longitude', 'time', 'valid_time', 'step').agg(*agg_exprs)
    
    return results