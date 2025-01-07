import sys

from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, MapType
from pyspark.sql.functions import col, expr, when, lit, filter, to_timestamp

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


# Define the schema explicitly
schema = StructType([
    StructField("id", StringType(), True),
    StructField("version", IntegerType(), True),
    StructField("createdat", StringType(), True),
    StructField("updatedat", StringType(), True),
    StructField(
        "history",
        ArrayType(MapType(StringType(), StringType()), True),  # Array of maps
        True
    ),
    StructField("status", StringType(), True),
    StructField("inspectiondefinitionid", StringType(), True),
    StructField("inspectiondefinitionversion", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("approvedby", StringType(), True),
    StructField("requirements", StringType(), True),
    StructField("associaterequired", BooleanType(), True),
    StructField("associateid", StringType(), True),
    StructField("digitalassetrequired", StringType(), True),
    StructField("digitalassetoptional", StringType(), True),
    StructField("digitalassetid", StringType(), True),
    StructField("equipmentrequired", BooleanType(), True),
    StructField("equipment", StringType(), True),
    StructField("locationrequired", BooleanType(), True)
])

s3_path = "s3://learning-buk/raw/file1.json"

input_df = spark.read.option("multiline","true").json(path=s3_path, schema=schema)

out_df = input_df.withColumn(
    "inspection_started_timestamp",
    to_timestamp(expr("""
        transform(
            filter(history, x -> x['systemconnent'] = 'INSPECTION_STARTED'),
            x -> x['timestamp']
        )[0]
    """))  # Extract the first matching timestamp, or null if none match
)

# out_df.show(truncate=False)
# out_df.printSchema()

drop_table_sql = "DROP TABLE IF EXISTS glue_catalog.data_lake.inspection_task"
spark.sql(drop_table_sql)

out_df.writeTo("glue_catalog.data_lake.inspection_task")\
                    .tableProperty("format-version", "2")\
                    .tableProperty("location", "s3://sk-data-lake-001/data_lake_001/inspection_task")\
                    .create()

job.commit()
