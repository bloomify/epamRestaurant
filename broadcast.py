from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, coalesce
from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, StructField

# Initialize Spark session
spark = SparkSession.builder.appName("Broadcast Join Example").getOrCreate()

# Define the schema explicitly
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("lat", FloatType(), True),
    StructField("lng", FloatType(), True),
])
# Sample larger DataFrame (original_df)
original_df = spark.createDataFrame([
    (1, "John", "NY", "USA", 25, None, None),  # Lat and Lng are missing here
    (2, "Alice", "CA", "USA", 30, None, None),
    (3, "Bob", "TX", "USA", 22, None, None),
    (4, "Charlie", "FL", "USA", 28, None, None),
    (5, "David", "WA", "USA", 35, None, None)
], schema)

schema2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("lat", FloatType(), True),
    StructField("lng", FloatType(), True),
])
# Smaller DataFrame (updated_df with lat and lng)
updated_df = spark.createDataFrame([
    (1, 40.7128, -74.0060),
    (3, 29.7604, -95.3698),
    (5, 47.6062, -122.3321)
], schema2)

updated_df = updated_df.withColumnRenamed("lat", "updated_lat").withColumnRenamed("lng", "updated_lng")

# Perform the join and broadcast smaller DataFrame
result_df = original_df.join(broadcast(updated_df), on="id", how="left")


# Overwrite lat and lng columns from the updated DataFrame
result_df = result_df.withColumn(
    "lat", coalesce(col("updated_lat"), col("lat"))
).withColumn(
    "lng", coalesce(col("updated_lng"), col("lng"))
)

# Select relevant columns and show the result
result_df.show()