import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CustomerLanding
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-customer-landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1684372786487 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1684372786487",
)

# Script generated for node Distinct Emails
DistinctEmails_node1684392659494 = DynamicFrame.fromDF(
    PrivacyFilter_node1684372786487.toDF().dropDuplicates(["email"]),
    glueContext,
    "DistinctEmails_node1684392659494",
)

# Script generated for node Drop Fields
DropFields_node1684392732214 = DropFields.apply(
    frame=DistinctEmails_node1684392659494,
    paths=["shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"],
    transformation_ctx="DropFields_node1684392732214",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1684393355450 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684392732214,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://stedi-customer-trusted", "partitionKeys": []},
    transformation_ctx="CustomerTrusted_node1684393355450",
)

job.commit()
