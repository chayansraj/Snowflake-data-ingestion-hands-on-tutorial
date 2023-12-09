USE ROLE ACCOUNTADMIN;

-- Create a custome role to perform actions
CREATE OR REPLACE ROLE DATA_LOADER COMMENT='Role to load data from different sources';
GRANT ROLE DATA_LOADER TO ROLE SYSADMIN;

-- Grant specific privileges practicing security principal of least privileges
GRANT OWNERSHIP ON DATABASE CUSTOMER_DATA TO ROLE DATA_LOADER;
GRANT CREATE STAGE ON SCHEMA CUSTOMER_DATA.PUBLIC TO ROLE DATA_LOADER;
GRANT ALL PRIVILEGES ON FILE FORMAT CUSTOMER_DATA.PUBLIC.customer_csv_ff TO DATA_LOADER;

USE ROLE DATA_LOADER;

USE ROLE SYSADMIN;

--Create Warehouse for Data Loading work
CREATE OR REPLACE WAREHOUSE DATA_LOAD_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = TRUE;

-- Grant permissions to the role to perform actions on VW
GRANT ALL ON WAREHOUSE DATA_LOAD_WH TO ROLE DATA_LOADER;

-- Use the custom role
USE ROLE DATA_LOADER;
USE WAREHOUSE DATA_LOAD_WH;

-- Create the database and schema
CREATE DATABASE IF NOT EXISTS CUSTOMER_DATA;
CREATE OR REPLACE SCHEMA CUST;

-- Activate the database and schema
USE DATABASE CUSTOMER_DATA;
USE SCHEMA CUST;

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

SELECT COUNT(*) FROM customer_csv;
SELECT * FROM customer_csv;

-- Create a custom csv file format with optional parameters
CREATE OR REPLACE FILE FORMAT customer_csv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    skip_header = 1
    record_delimiter='\n'
    FIELD_OPTIONALLY_ENCLOSED_BY ='\042';


-- Create staging area through internal named stage
CREATE OR REPLACE STAGE csv_load
    FILE_FORMAT = customer_csv_ff;


-- Load the data into customer_csv table
COPY INTO customer_csv
    FROM @csv_load
    FILE_FORMAT = customer_csv_ff
    ON_ERROR='CONTINUE'
    VALIDATION_MODE= RETURN_ALL_ERRORS;

    
SELECT COUNT(*) FROM customer_csv;
