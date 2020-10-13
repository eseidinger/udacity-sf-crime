import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType()),
    StructField("original_crime_type_name", StringType()),
    StructField("report_date", StringType()),
    StructField("call_date", StringType()),
    StructField("offense_date", StringType()),
    StructField("call_time", StringType()),
    StructField("call_date_time", StringType()),
    StructField("disposition", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("agency_id", StringType()),
    StructField("address_type", StringType()),
    StructField("common_location", StringType())
])

radio_code_schema = StructType([
    StructField("disposition_code", StringType()),
    StructField("description", StringType())
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "pd.call.log") \
        .option("startingOffset", "earliest") \
        .option("maxOffsetsPerTrigger", 800) \
        .option("stopGracefullyOnShutdown", "true") \
        .option("spark.default.parallelism", 100) \
        .load()

        # .option("spark.default.parallelism", 20) \
        # .option("spark.streaming.backpressure.enabled", True) \
        # .option("spark.streaming.receiver.maxRate", 10) \

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(psf.to_timestamp("call_date_time").alias("call_date_time"), "original_crime_type_name", "disposition").distinct()

    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table \
        .select("call_date_time", "original_crime_type_name") \
        .withWatermark("call_date_time", "30 minutes") \
        .groupBy(psf.window("call_date_time", "60 minutes", "15 minutes"), "original_crime_type_name").count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    # radio_code_df = spark.read.json(radio_code_json_filepath)
    # the line above creates a corrupt data frame so I am using the code below to create the data frame from json
    with open(radio_code_json_filepath) as f:
        data = json.load(f)
        radio_code_rdd = spark.sparkContext.parallelize(data)
        radio_code_df = spark.createDataFrame(radio_code_rdd, radio_code_schema)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    # The agg_df does not have a disposition column, so I am using the distinct_table
    join_query = distinct_table.join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    # spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
