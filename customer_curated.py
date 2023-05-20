import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-customer-trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Acclerometer Trusted
AcclerometerTrusted_node1684396950247 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-accelerometer-trusted/"],
        "recurse": True,
    },
    transformation_ctx="AcclerometerTrusted_node1684396950247",
)

# Script generated for node Filter
Filter_node1684397572335 = Filter.apply(
    frame=CustomerTrusted_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1684397572335",
)

# Script generated for node Join
Join_node1684382265713 = Join.apply(
    frame1=AcclerometerTrusted_node1684396950247,
    frame2=Filter_node1684397572335,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1684382265713",
)

# Script generated for node Drop Fields
DropFields_node1684383173740 = DropFields.apply(
    frame=Join_node1684382265713,
    paths=["z", "user", "y", "x"],
    transformation_ctx="DropFields_node1684383173740",
)

# Script generated for node CustomersCurated
CustomersCurated_node1684383227043 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684383173740,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://stedi-customers-curated/", "partitionKeys": []},
    transformation_ctx="CustomersCurated_node1684383227043",
)

job.commit()
