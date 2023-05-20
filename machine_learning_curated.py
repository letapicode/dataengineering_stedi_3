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

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-customer-trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1684407537208 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-step-trainer-landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding_node1684407537208",
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

# Script generated for node Drop Fields
DropFields_node1684383173740 = DropFields.apply(
    frame=CustomerTrusted_node1,
    paths=["birthDay", "registrationDate", "customerName", "lastUpdateDate", "phone"],
    transformation_ctx="DropFields_node1684383173740",
)

# Script generated for node CustTrustRename
CustTrustRename_node1684406657430 = RenameField.apply(
    frame=DropFields_node1684383173740,
    old_name="serialNumber",
    new_name="cc_serialnumber",
    transformation_ctx="CustTrustRename_node1684406657430",
)

# Script generated for node AcCustJoin
AcCustJoin_node1684406702276 = Join.apply(
    frame1=CustTrustRename_node1684406657430,
    frame2=AcclerometerTrusted_node1684396950247,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="AcCustJoin_node1684406702276",
)

# Script generated for node TrainerAcCust Join
TrainerAcCustJoin_node1684407698502 = Join.apply(
    frame1=StepTrainerLanding_node1684407537208,
    frame2=AcCustJoin_node1684406702276,
    keys1=["serialNumber", "sensorReadingTime"],
    keys2=["cc_serialnumber", "timeStamp"],
    transformation_ctx="TrainerAcCustJoin_node1684407698502",
)

# Script generated for node Drop Fields
DropFields_node1684408214052 = DropFields.apply(
    frame=TrainerAcCustJoin_node1684407698502,
    paths=["cc_serialnumber", "shareWithResearchAsOfDate", "email"],
    transformation_ctx="DropFields_node1684408214052",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1684408337801 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684408214052,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-machine-learning-curated/",
        "partitionKeys": ["user"],
    },
    transformation_ctx="MachineLearningCurated_node1684408337801",
)

job.commit()
