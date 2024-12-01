# This is a task for Apache Spark learning purpose (EPAM)
import sys
import asyncio
import os
import zlib
import struct
import geohashr
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from opencage.geocoder import OpenCageGeocode, RateLimitExceededError
from pyspark.sql.functions import col, udf, concat_ws, broadcast, coalesce


SOURCE_DIR = 'C:\\EPAM\\spark\\restaurant_csv\\restaurant_csv'
BADRECORDS_DIR = 'C:\\EPAM\\spark\\restaurant_csv\\badrecords'
GEOCODER_KEY = 'fc7131c342ad4eeaa10c6fd1f7fc8540'
MAX_ITEMS = 100   # Set to 0 for unlimited
NUM_WORKERS = 3   # For 10 requests per second try 2-5



# This function reads the Restaurant data from the directory
# and populates the null values of latitude and longitude by using OpenCage Service
# This function is intended to use on small dataset and local machine
def read_from_csvfiles(spark, source_dir, badrecords_dir):
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

    # Check that the key is unique
    total_rows = rest_addr_schema.count()
    distinct_key_count = rest_addr_schema.select("ID").distinct().count()

    # Check that the ID key is unique
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
    return geohashr.encode(lat, lon, precision)

if __name__ == '__main__':

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

    read_from_csvfiles(spark, SOURCE_DIR, BADRECORDS_DIR)

    #print(fetch_geocodedata_byaddress('Savoria Dillon US'))
