USE ROLE DATA_LOADER;
USE WAREHOUSE DATA_LOAD_WH;

-- Creating a stage requires certain privileges
GRANT CREATE STAGE ON SCHEMA public TO ROLE DATA_LOADER;
GRANT USAGE ON INTEGRATION aws_snowflake TO ROLE DATA_LOADER;


-- Create the database and schema
CREATE DATABASE IF NOT EXISTS AWS_DATABASE;
CREATE OR REPLACE SCHEMA AWS;

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

    
-- Create storage integration for aws
CREATE STORAGE INTEGRATION aws_snowflake
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED= TRUE
    STORAGE_AWS_ROLE_ARN = '<your_iam_role_arn>'
    STORAGE_ALLOWED_LOCATIONS = ('<your_s3_bucket_uri>');



-- Create a custom csv file format with optional parameters
CREATE OR REPLACE FILE FORMAT customer_csv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    skip_header = 1
    record_delimiter='\n'
    FIELD_OPTIONALLY_ENCLOSED_BY ='\042';


-- Create the external stage for aws s3 bucket
CREATE OR REPLACE STAGE aws_load
    url = '<your_s3_bucket_uri>'
    STORAGE_INTEGRATION = aws_snowflake
    FILE_FORMAT = customer_csv_ff;
    
DESC STAGE AWS_LOAD;
LIST @AWS_LOAD;

    
--Load data by using the COPY command
COPY INTO customer_csv FROM @AWS_LOAD 
    FILE_FORMAT = customer_csv_ff
    ON_ERROR = 'CONTINUE';

SELECT COUNT(*) FROM customer_csv;

    

DESC INTEGRATION aws_snowflake;
