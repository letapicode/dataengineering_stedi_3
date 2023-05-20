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

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-step-trainer-landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1684398345077 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-customers-curated/"], "recurse": True},
    transformation_ctx="CustomerCurated_node1684398345077",
)

# Script generated for node Customer Rename Field
CustomerRenameField_node1684387883678 = RenameField.apply(
    frame=CustomerCurated_node1684398345077,
    old_name="serialNumber",
    new_name="cc_serialnumber",
    transformation_ctx="CustomerRenameField_node1684387883678",
)

# Script generated for node Join
Join_node1684387933385 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerRenameField_node1684387883678,
    keys1=["serialNumber"],
    keys2=["cc_serialnumber"],
    transformation_ctx="Join_node1684387933385",
)

# Script generated for node Drop Fields
DropFields_node1684388108402 = DropFields.apply(
    frame=Join_node1684387933385,
    paths=[
        "email",
        "phone",
        "cc_serialnumber",
        "timeStamp",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "lastUpdateDate",
    ],
    transformation_ctx="DropFields_node1684388108402",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1684388292222 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684388108402,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-step-trainer-trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1684388292222",
)

job.commit()
