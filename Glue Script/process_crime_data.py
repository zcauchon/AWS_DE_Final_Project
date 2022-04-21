import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import month, dayofmonth, to_date, col

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node0 = glueContext.create_dynamic_frame.from_catalog(database="glue_crime_db", table_name="input", transformation_ctx="S3bucket_node0")

#Try converting dynamic frame to df and add month and day columns
df = (S3bucket_node0.toDF().withColumn("new_date", to_date("date", "MM/dd/yyyy hh:mm:ss a")))
df2 = (df.withColumn("month", month("new_date")))
df3 = (df2.withColumn("day", dayofmonth("new_date")))
S3bucket_node1 = DynamicFrame.fromDF(df3, glueContext, "S3bucket_node0")

# Script generated for node S3 bucket
# format_options={"compression": "snappy"},
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node1,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://bah2-final-project/processed/",
        "partitionKeys": ["year", "month"],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="S3bucket_node2",
)

job.commit()
