# This is a task for Apache Spark learning purpose (EPAM)
import sys
import asyncio
import os
import zlib
import struct
import pygeohash as pgh
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from opencage.geocoder import OpenCageGeocode, RateLimitExceededError
from pyspark.sql.functions import col, udf, concat_ws, broadcast, coalesce, pandas_udf
from datetime import datetime
import pandas as pd


SOURCE_DIR = 'C:\\EPAM\\spark\\restaurant_csv\\restaurant_csv'
BADRECORDS_DIR = 'C:\\EPAM\\spark\\restaurant_csv\\badrecords'
WEATHER_SOURCE_DIR='C:\EPAM\spark\weather\weather'
JOINEDRESULT_DIR='file:///C:/EPAM/spark/joinedresult'
GEOCODER_KEY = 'fc7131c342ad4eeaa10c6fd1f7fc8540'
MAX_ITEMS = 100   # Set to 0 for unlimited
NUM_WORKERS = 3   # For 10 requests per second try 2-5



# This function reads the Restaurant data from the directory
# and populates the null values of latitude and longitude with real values by using OpenCage Service
# This function is intended to use on small dataset and local machine
def read_restaurant_csvfiles_populatewithdata(spark, source_dir, badrecords_dir):
    # Schema definition as StructType (more efficient than string schema parsing)
    rest_addr_schema = StructType([
        StructField("id", LongType(),True),
        StructField("franchise_id", LongType(), True),
        StructField("franchise_name", StringType(), True),
        StructField("restaurant_franchise_id", LongType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True)
    ])

    # Corrupt records are logged and included with null values for unreadable fields.
    # Capture bad records
    rest_addr_schema = spark.read.option("header", True) \
                                .option("inferSchema", True) \
                                .option("mode", "PERMISSIVE") \
                                .option("badRecordsPath", badrecords_dir) \
                                .csv(source_dir)

    # Ensure that the ID key is unique
    total_rows = rest_addr_schema.count()
    distinct_key_count = rest_addr_schema.select("ID").distinct().count()

    if total_rows == distinct_key_count:
        print("The ID key is unique.")
    else:
        print("The ID key is not unique.")

    rest_addr_schema.printSchema()

    # below statement is used to populate lng, lat fields from OpenCage internet service
    modified_df = rest_addr_schema \
        .filter((col("lat").isNull()) | (col("lng").isNull())) \
        .withColumn("fetched_data", fetch_udf(concat_ws(" ", col("franchise_name"), col("city"), col("country")))) \
        .withColumn("lat", col("fetched_data.lat")) \
        .withColumn("lng", col("fetched_data.lng")) \
        .drop("fetched_data")

    modified_df.show(n=5, truncate=False)
    print('The total number of modified rows: ' + str(modified_df.count()))

    # Rename columns to broadcast values to the original DataFrame
    modified_df = modified_df.select("id", "lat", "lng").withColumnRenamed("lat", "updated_lat").withColumnRenamed("lng", "updated_lng")

    # Broadcasting helps improve the performance of join operations
    # when one of the DataFrames is much smaller than the other
    result_df = rest_addr_schema.join(broadcast(modified_df), on="id", how="left")
    # Overwrite the 'name' column with values from modified_df
    result_df = result_df.withColumn(
        "lat",
        coalesce(col("updated_lat"), col("lat"))
    ).withColumn(
        "lng",
        coalesce(col("updated_lng"), col("lng"))
    )

    # drop unused columns
    result_df = result_df.drop("updated_lat", "updated_lng")

    #result_df.filter(col("id") == "85899345920").show()
    print('The total number of rows: ' + str(result_df.count()))

    geohash_udf = udf(lambda lat, lon: generate_geohash(lat, lon), StringType())
    result_df_geohash = result_df.withColumn("geohash", geohash_udf("lat", "lng"))

    result_df_geohash.show(n=5, truncate=False)

    return result_df_geohash

# Fetching geocodedata from OpenCage Service
def fetch_geocodedata_byaddress(qaddress):
    geocoder = OpenCageGeocode(GEOCODER_KEY)
    try:
        # no need to URI encode query, module does that for you
        results = geocoder.geocode(qaddress)
    except IOError:
        print('Error: File %s does not appear to exist.' % qaddress)
    except RateLimitExceededError as ex:
        print(ex)

    print(u'%f;%f' % (results[0]['geometry']['lat'],results[0]['geometry']['lng']))
    return results[0]['geometry']['lat'], results[0]['geometry']['lng']

# Create a UDF for Fetching Data via HTTP
@udf(returnType=StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True)
]))

def fetch_udf(address):
    lng, lat = fetch_geocodedata_byaddress(address)
    return {"lat": lat, "lng": lng}

# Define a UDF to calculate geohash
def generate_geohash(lat, lon, precision=4):
    return pgh.encode(lat, lon, precision)

def read_weather_files_populatewithdata(spark, source_dir, rest_df, result_dir):
    weather_df = spark.read.parquet(source_dir)
    #print('The total number of rows: ' + str(weather_df.count()))

    # !using Pandas_UDF took a longer time than Row-based UDF
    # @pandas_udf("string")
    # def compute_geohash(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    #     # Use geohashr to encode latitude and longitude with precision 4
    #     return lat_series.combine(lon_series, lambda lat, lon: geohashr.encode(lat, lon, 4))
    #
    # # Add the geohash column using the Pandas UDF
    # df = df.withColumn("geohash", compute_geohash(df["lat"], df["lng"]))
    geohash_udf = udf(lambda lat, lon: generate_geohash(lat, lon), StringType())
    weather_df = weather_df.withColumn("geohash", geohash_udf("lat", "lng"))

    # Repartition for better parallelism (adjust partition size based on your cluster's capacity)
    #df = df.repartition(8, "geohash")  # Adjust partition count

    weather_df = weather_df.dropDuplicates(["geohash"])
    weather_df.show(n=5, truncate=False)
    weather_df.printSchema()
    #print('The total number of rows: ' + str(df.count()))
    rest_df.show(n=5, truncate=False)
    #result_df = rest_df.join(weather_df, on="geohash", how="left")

    # Drop duplicate columns which persists on both DF: lat, lng
    rest_df = rest_df.select("id", "franchise_id", "franchise_name", "restaurant_franchise_id","country", "city", "geohash")
    #Avoid Wide Transformations, use broadcast
    result_df = weather_df.join(broadcast(rest_df), on="geohash", how="left")
    #result_df.show(n=5, truncate=False)
    result_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(result_dir)
    print(f"Data successfully saved to {result_dir} in Parquet format with partitioning.")

if __name__ == '__main__':

    # Start timer
    start_time = datetime.now()

    print('Apache Spark task...')

    # get the number of Cores. this program is launched on personal computer
    num_cores = os.cpu_count()
    print(f"Number of CPU cores: {num_cores}")


    spark = SparkSession.builder \
            .appName('RestaurantTask') \
            .getOrCreate()


    # reduce the number of partitions to avoid unnecessary overhead for shuffle operation.
    # Set to 8 based on os.cpu_count()
    spark.conf.set('spark.sql.shuffle.partitions', '8')
    spark.sparkContext.setLogLevel('WARN')

    #print(generate_geohash(37.620978, -96.229513))

    restaurant_df = read_restaurant_csvfiles_populatewithdata(spark, SOURCE_DIR, BADRECORDS_DIR)

    read_weather_files_populatewithdata(spark, WEATHER_SOURCE_DIR, restaurant_df, JOINEDRESULT_DIR)

    #print(fetch_geocodedata_byaddress('Savoria Dillon US'))

    # End timer
    end_time = datetime.now()

    # Calculate execution time
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time}")
