# 🧊 Airflow → S3 → Snowflake ETL Pipeline

This project demonstrates a daily ETL pipeline using Apache Airflow to load order data from Amazon S3 into Snowflake, validate the data, log load results, and perform SQL-based transformations for analytics and reporting.


---

## 📌 Project Overview

- Raw .csv order files are uploaded daily to an S3 bucket.
- An Airflow DAG orchestrates:
  - File name resolution based on the current date.
  - Data validation (schema, mandatory fields, unique order_id).
  - Data load into Snowflake using the COPY INTO command.
  - Post-load validation (row count check).
- After ingestion, SQL transformations in Snowflake generate business insights such as:
  - Average order value
  - Freight cost per state
  - Revenue breakdown by payment type
  - Monthly sales trends
  - Top-performing states by sales volume


---

## 🛠️ Tech Stack

| Component        | Role                                                                 |
|------------------|----------------------------------------------------------------------|
| Apache Airflow   | DAG orchestration and scheduling                                       |
| Amazon S3        | Raw file storage for daily order data (`orders_YYYYMMDD.csv`)         |
| Snowflake        | Cloud data warehouse for ingestion and analytics                      |
| Python           | Task logic, validation, and Airflow operators                         |
| AWS IAM          | Secure role and policy configuration to allow S3 → Snowflake access   |
| XCom (Airflow)   | Metadata exchange between tasks (e.g., row count after load)          |

---

## 📁 Project Structure
.
├── dags/<br>
│   └── dag_with_validation.py         # Main DAG with validation and load<br>
├── data/<br>                              # Example input files<br>
│   └── orders_20250725.csv <br>            
│   └── orders_20250726.csv <br>           
│   └── orders_20250727.csv <br>           
├── docker-compose.yml                 # Containerized Airflow <br> 
├── sql <br>                       
│   └── snoflake.sql                   # SQL scrips for snowflake configuration <br>
│   └── scrips.sql                     # SQL scrips for analytics and reporting <br>
└── README.md

---
## 🔧 AWS Configuration (S3 + IAM Role for Snowflake Integration)
1. S3 Bucket Setup
Create an S3 bucket: sales-data-2016-2018

Upload CSV files into the corresponding folders:
```
s3://sales-data-2016-2018/orders/orders_20250725.csv  
s3://sales-data-2016-2018/orders/order_items_20250725.csv
s3://sales-data-2016-2018/orders/customers_20250725.csv
... etc.
```
2. IAM Role Setup (for Snowflake to access S3)
2.1 Create IAM Policy

Name: sales-data-2016-2018

Description: Grants Snowflake access to the S3 bucket

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion"
      ],
      "Resource": "arn:aws:s3:::sales-data-2016-2018/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::sales-data-2016-2018"
    }
  ]
}
```
2.2 Create IAM Role

Type: AWS Account (for 3rd-party access)

Use External ID (recommended best practice)

Attach the above policy

Name: sales-data-2016-2018

Description: Role for Snowflake to access S3

Copy the generated Role ARN (you’ll need it for Snowflake storage integration):

```
arn:aws:iam::916450737010:role/sales-data-2016-2018
```
3. Update Trust Relationship for Snowflake
After creating a storage integration in Snowflake:

Use the values returned by Snowflake:

STORAGE_AWS_IAM_USER_ARN

STORAGE_AWS_EXTERNAL_ID

Go to Trust relationships → Edit trust policy, and update:

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```
## ❄️ Snowflake Configuration
 • A dedicated database (SALES_DATA) and schema (SALES_DATA_SCHEMA) were created to store and organize incoming data.<br>
 • A storage integration (aws_s3_integration) was set up to securely connect Snowflake with AWS S3 using an IAM role. This allows Snowflake to directly query and load data from the S3 bucket (s3://sales-data-2016-2018/).<br>
 • A custom file format was defined for CSV ingestion, handling delimiters, header rows, and timestamps.<br>
 • An external stage (aws_stage) was created to reference the S3 location, making it easy to browse, load, and remove files from Snowflake.<br>
 • Usage and access grants were applied to ensure the proper Snowflake roles could interact with the stage and integration.<br>
 • Tables such as customers, orders, and others were defined and populated using the COPY INTO command directly from the external stage.<br>
 • Snowflake’s analytics layer included:<br>
 • Creation of views for aggregated and cleaned data.<br>
 • Revenue breakdown by payment type.<br>
 • Performance by state, including metrics like average order value and freight costs.<br>
 • Monthly sales trends for time-series insights.

## 🔄 Airflow Configuration (Dockerized)
 • Airflow is containerized using docker-compose for local orchestration and scheduling.<br>
 • Airflow metadata and logs are persisted via Docker volumes for durability across restarts.<br>
 • Credentials and endpoints for Snowflake and AWS S3 are securely managed via Airflow Connections, created through the Airflow UI.<br>
 • Core services (e.g. webserver, scheduler, triggerer, and postgres) are defined in docker-compose.

## 🧪 Testing
Tested with manual uploads to S3 and triggering the DAG via Airflow UI.
Post-load validation ensures data integrity by comparing row counts before and after load.

## 📂 Logs and Monitoring
All task logs are viewable in Airflow UI (Logs tab per task).
Logs include printouts like loaded row counts, validation results, and file paths.
