import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1684377020479 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-accelerometer-landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1684377020479",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1684395128920 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-customer-trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1684395128920",
)

# Script generated for node Join
Join_node1684377566258 = Join.apply(
    frame1=AccelerometerLanding_node1684377020479,
    frame2=CustomerTrusted_node1684395128920,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1684377566258",
)

# Script generated for node Drop Customer Fields
DropCustomerFields_node1684377919907 = DropFields.apply(
    frame=Join_node1684377566258,
    paths=[
        "email",
        "phone",
        "lastUpdateDate",
        "customerName",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "birthDay",
        "serialNumber",
    ],
    transformation_ctx="DropCustomerFields_node1684377919907",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1684378194813 = glueContext.write_dynamic_frame.from_options(
    frame=DropCustomerFields_node1684377919907,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-accelerometer-trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1684378194813",
)

job.commit()
