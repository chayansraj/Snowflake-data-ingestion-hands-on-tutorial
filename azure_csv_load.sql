-- Create the database and schema
CREATE DATABASE IF NOT EXISTS AZURE_DATABASE;
CREATE OR REPLACE SCHEMA AZURE;

USE AZURE_DATABASE;
USE SCHEMA AZURE;


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

-- Create storage integration for azure
CREATE STORAGE INTEGRATION azure_snowflake
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'your_tenant_id'
  STORAGE_ALLOWED_LOCATIONS = ('your_azure_blob_uri');

DESC STORAGE INTEGRATION AZURE_SNOWFLAKE;


-- Create a custom csv file format with optional parameters
CREATE OR REPLACE FILE FORMAT customer_csv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    skip_header = 1
    record_delimiter='\n'
    FIELD_OPTIONALLY_ENCLOSED_BY ='\042';


-- Create the external stage for azure blob storage
CREATE OR REPLACE STAGE azure_load
    url = 'your_azure_blob_uri'
    STORAGE_INTEGRATION = azure_snowflake
    FILE_FORMAT = customer_csv_ff;
    
DESC STAGE azure_load;
LIST @azure_load;
