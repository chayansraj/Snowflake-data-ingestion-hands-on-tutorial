-- Create the database and schema
CREATE DATABASE IF NOT EXISTS GCP_DATABASE;
CREATE OR REPLACE SCHEMA GCP;

USE GCP_DATABASE;
USE SCHEMA GCP;

CREATE OR REPLACE TABLE customer_csv (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10,2),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);

-- Create storage integration for gcs
CREATE STORAGE INTEGRATION gcp_snowflake
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('your_gcs_bucket');

DESC STORAGE INTEGRATION gcp_snowflake;

-- Create a custom csv file format with optional parameters
CREATE OR REPLACE FILE FORMAT customer_csv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    skip_header = 1
    record_delimiter='\n'
    FIELD_OPTIONALLY_ENCLOSED_BY ='\042';


-- Create the external stage for aws s3 bucket
CREATE OR REPLACE STAGE gcp_load
    url = 'your_gcs_bucket'
    STORAGE_INTEGRATION = gcp_snowflake
    FILE_FORMAT = customer_csv_ff;
    
DESC STAGE gcp_load;
LIST @gcp_load;

--Load data by using the COPY command
COPY INTO customer_csv FROM @gcp_load 
    FILE_FORMAT = customer_csv_ff
    ON_ERROR = 'CONTINUE';


SELECT COUNT(*) FROM customer_csv;
