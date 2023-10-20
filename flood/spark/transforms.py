from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def round_to_precision(value, precision):
    """
    Round a value to the given number of decimal places.
    """
    return round(value, precision)

def create_round_udf(precision=3):
    """
    Create a UDF for rounding to the specified precision.
    """
    return udf(lambda x: round_to_precision(x, precision), DoubleType())