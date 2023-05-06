
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "trafficDataSet"

spark = SparkSession.builder.appName("sparkConsumerEx3").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

SCHEMA = StructType([
    StructField("LOCATION", StringType()),        # Location 
    StructField("VEH_VALUE",StringType()),        # Vehicle Value at the Junction
    StructField("SB_VALUE", StringType()),        # Vehicles in the South Bound Lane near the Junction
    StructField("NB_VALUE", StringType()),    	  # Vehicles in the North Bound Lane near the Junction
    StructField("EB_VALUE", StringType()),        # Vehicles in the East Bound Lane near the Junction
    StructField("WB_VALUE", StringType()),        # Vehicles in the West Bound Lane near the Junction
])

result = df\
    .select(
        F.from_json(
            # decode string as iso-8859-1
            F.decode(F.col("value"), "iso-8859-1"),
            SCHEMA
        ).alias("value")
    )\
    .select("value.*")\
    .select(
        F.col("LOCATION").alias("location"),F.col("VEH_VALUE").cast(IntegerType()).alias("VEH_VALUE_AT_JUNCTION"),
        F.col("SB_VALUE").cast(IntegerType()).alias("VEHICLES_AT_SOUTHBOUND_JUN"),
	F.col("NB_VALUE").cast(IntegerType()).alias("VEHICLES_AT_NORTHBOUND_JUN"),
	F.col("EB_VALUE").cast(IntegerType()).alias("VEHICLES_AT_EASTBOUND_JUN"),
        F.col("WB_VALUE").cast(IntegerType()).alias("VEHICLES_AT_WESTBOUND_JUN")
       ,expr('VEH_VALUE+SB_VALUE+EB_VALUE+NB_VALUE+WB_VALUE').alias("Total_Vehicle_Count"))


# Display the output
query = result.writeStream.outputMode("update").format("console").start()
query.awaitTermination()
