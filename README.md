## STEDI Human Balance Analytics: Data Engineering Report
# 1. Introduction
The purpose of this report is to outline the data engineering work performed for the STEDI Human Balance Analytics project. The project involved building a data lakehouse solution on AWS to process and curate sensor data from STEDI Step Trainers and mobile apps, making it available for Data Scientists to train a machine learning model for detecting steps accurately in real-time.

This report provides an overview of the data sources, the data processing pipeline, the AWS infrastructure used, and the final outcome. The report aims to document the data engineering work performed, providing a reference for future projects.

# 2. Data Sources
Three JSON data sources were used in this project, coming from STEDI Step Trainers and mobile apps:

- Customer Records: Contains customer data from the STEDI website and fulfillment systems.
- Step Trainer Records: Contains motion sensor data from the STEDI Step Trainers.
- Accelerometer Records: Contains motion data from the mobile app's accelerometer.

Data GitHub Link:

https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter

These data sources were ingested into the AWS environment and stored in Amazon S3 buckets.

# 3. Data Processing Pipeline
The data processing pipeline consisted of several stages, which are described below:

- Landing Zones: Raw data from each data source was copied into separate S3 directories acting as landing zones. This allowed the raw data to be organized and available for processing.

- AWS Glue Crawlers: Glue Crawlers were used to infer the schema of the data in the landing zones and created tables in the Glue Data Catalog.

- Trusted Zones: AWS Glue jobs were created to sanitize the Customer and Accelerometer data, filtering out records that did not meet privacy requirements. The sanitized data was stored in separate trusted zone tables in S3.

- Curated Zones: Additional AWS Glue jobs and Glue Studio jobs were used to further process and curate the data. This included matching Customer and Accelerometer records and aggregating Step Trainer and Accelerometer data for the machine learning model. The curated data was stored in separate tables in S3.

# 4. AWS Infrastructure
The data processing pipeline leveraged several AWS services, including:

- Amazon S3: Used for storing raw, trusted, and curated data.
- AWS Glue: Used for data processing, schema inference, and table creation.
- Glue Crawlers: Used to create tables in the Glue Data Catalog.
- Glue Jobs: Used to sanitize, match, and aggregate data.
- Glue Studio: Used for creating and executing Glue Studio jobs.
- AWS Athena: Used for querying data stored in S3 via Glue Data Catalog tables.

# 5. Outcomes
Upon completion of the data processing pipeline, the following results were achieved:

Customer and Accelerometer data was sanitized according to privacy requirements, ensuring that only data from customers who agreed to share their data for research purposes was used in further processing.

Customer and Accelerometer data was matched and curated into a single table, associating each customer with their respective accelerometer data.

Step Trainer and Accelerometer data was aggregated to create a final curated table containing data for the machine learning model, ensuring that only data from customers who agreed to share their data was included.

In conclusion, the data engineering work performed for the STEDI Human Balance Analytics project successfully provided Data Scientists with sanitized, matched, and aggregated data for training a machine learning model. The data lakehouse solution built on AWS allowed for efficient and scalable data processing, making it suitable for future data growth and additional data sources.


