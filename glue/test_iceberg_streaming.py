import sys
import boto3
from datetime import datetime

from pyspark.context import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, MapType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# from pyspark.sql.functions import col, expr, when, lit, filter, to_timestamp
from pyspark.sql.functions import col, lit, input_file_name

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

conf_list = [("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
             ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
             ("spark.sql.catalog.glue_catalog.warehouse", "s3://sk-data-lake-001/data_lake_001/"),
             ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
             ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
             ("spark.sql.iceberg.handle-timestamp-without-timezone", "true"),
             ("spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables", "true"),
             ("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED"),
             ("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED"),
             ("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED"),
             ("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")]

spark_conf = SparkConf().setAll(conf_list)
spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Path to JSON file in S3
s3_input_path = "s3://learning-buk/sqs_landing/test_q/"

# Read data using AWS Glue with job bookmarks enabled
input_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [s3_input_path],
        "recurse": True,
        "groupFiles": "none"  # Processes individual files
    },
    format="json",
    transformation_ctx="input_dynamic_frame"
)

# Check if DynamicFrame is empty
if input_dynamic_frame.count() == 0:
    print("No files found in the specified S3 path. Exiting job.")
    # Exit gracefully when no files exist
else:
    read_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    read_date = datetime.now().strftime("%Y-%m-%d")
    
    # Convert to DataFrame
    input_df = input_dynamic_frame.toDF()
    
    # Add file name and file read timestamp columns
    df_with_meta = input_df.withColumn("file_name", input_file_name()) \
                           .withColumn("file_read_timestamp", lit(read_timestamp)) \
                           .withColumn("file_read_date", lit(read_date))
    
    # Flatten the DataFrame by selecting fields explicitly
    flattened_df = df_with_meta.select(
        col("id"),
        col("timestamp"),
        col("value"),
        col("status"),
        col("user"),
        col("priority"),
        col("source"),
        col("metadata.latitude").alias("latitude"),
        col("metadata.longitude").alias("longitude"),
        col("metadata.temperature").alias("temperature"),
        col("metadata.humidity").alias("humidity"),
        col("file_name"),
        col("file_read_timestamp"),
        col("file_read_date")
    )
    
    # Show the flattened DataFrame
    # flattened_df.show(truncate=False)
    
    table_name = 'test_streaming_data'
    glue_client = boto3.client('glue')
    table_exists = False
    try:
        response = glue_client.get_table(
                            DatabaseName='data_lake',
                            Name=table_name
                        )
        table_exists = True
    except Exception as e:
        print(e)
    print(f"{table_name} exists: {table_exists}")

    if table_exists:
        # Append new data to the existing table
        flattened_df.writeTo(f"glue_catalog.data_lake.{table_name}").append()
        print(f"data appended to {table_name}")
    else:
        # flattened_df.writeTo(f"glue_catalog.data_lake.{table_name}")\
        #                     .tableProperty("format-version", "2")\
        #                     .tableProperty("location", f"s3://sk-data-lake-001/data_lake_001/{table_name}")\
        #                     .create()
        flattened_df.createOrReplaceTempView("update")
        query = f"""
                    CREATE TABLE glue_catalog.data_lake.{table_name}
                    USING iceberg
                    PARTITIONED BY (file_read_date)
                    LOCATION 's3://sk-data-lake-001/data_lake_001/{table_name}'
                    TBLPROPERTIES ("format-version"="2")
                    AS SELECT * FROM update
                """
        spark.sql(query)
        print(f"table {table_name} created and data inserted")

job.commit()
