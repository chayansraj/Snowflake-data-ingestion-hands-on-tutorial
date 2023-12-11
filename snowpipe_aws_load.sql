USE ROLE ACCOUNTADMIN;


-- Creating a stage requires certain privileges
GRANT CREATE STAGE ON SCHEMA SNOWPIPE_AWS TO ROLE DATA_LOADER;
GRANT OWNERSHIP ON INTEGRATION aws_snowflake TO ROLE DATA_LOADER REVOKE CURRENT GRANTS;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DATA_LOADER;

-- Use this understand the privileges granted to role DATA_LOADER
SHOW GRANTS TO ROLE DATA_LOADER;


USE ROLE DATA_LOADER;
USE WAREHOUSE DATA_LOAD_WH;



-- Create the database and schema
CREATE DATABASE IF NOT EXISTS SNOWPIPE_AWS_DATABASE;
CREATE OR REPLACE SCHEMA SNOWPIPE_AWS;


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


USE DATABASE SNOWPIPE_AWS_DATABASE;
USE SCHEMA SNOWPIPE_AWS;



-- Create storage integration for aws s3 bucket and account
CREATE STORAGE INTEGRATION aws_snowpipe
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED= TRUE
    STORAGE_AWS_ROLE_ARN = '<your arn>'
    STORAGE_ALLOWED_LOCATIONS = ('<your bucket arn>');


    
-- Use Describe to check the details of the integration
DESC INTEGRATION aws_snowpipe;



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
    url = '<your bucket arn>'
    STORAGE_INTEGRATION = aws_snowpipe
    FILE_FORMAT = customer_csv_ff;

    
DESC STAGE AWS_LOAD;
LIST @AWS_LOAD;



-- Create a PIPE object with following parameters
CREATE OR REPLACE PIPE aws_pipe
    AUTO_INGEST = TRUE AS
    COPY INTO SNOWPIPE_AWS_DATABASE.SNOWPIPE_AWS.CUSTOMER_CSV
    FROM @AWS_LOAD
    FILE_FORMAT = customer_csv_ff
    ON_ERROR='CONTINUE';


SHOW PIPES;

-- Returns 1082 rows as loaded from snowpipe
SELECT COUNT(*) FROM customer_csv;


-- alter the timezone as per your requirement
SHOW PARAMETERS LIKE 'TIMEZONE';
SELECT CURRENT_TIMESTAMP();
ALTER SESSION SET TIMEZONE = 'UTC';

-- displays a comprehensive description of pipe object status
SELECT SYSTEM$PIPE_STATUS( 'SNOWPIPE_AWS_DATABASE.SNOWPIPE_AWS.AWS_PIPE' );


-- after the job, I paused the running pipe and removed the files in external stage
ALTER PIPE AWS_PIPE SET PIPE_EXECUTION_PAUSED = true;
REMOVE @AWS_LOAD;















    

