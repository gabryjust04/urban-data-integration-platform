import re
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType



def display(spark):
    df = spark.read.format("delta").load("storage/silver/air_quality")
    df.select(
        "parameter_name",
        "units_of_measure"
    ).distinct().show(
        truncate=False
    )