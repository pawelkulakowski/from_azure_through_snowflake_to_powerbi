-- Creating two seperate warehouses one for data loading, second one for BI operations

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE ELT_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = TRUE;

GRANT ALL ON WAREHOUSE ELT_WH TO ROLE SYSADMIN;

CREATE OR REPLACE WAREHOUSE POWERBI_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 120
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = TRUE;

GRANT ALL ON WAREHOUSE POWERBI_WH TO ROLE SYSADMIN;

-- Creating a new role 'POWERBI_ROLE' which will be used by POWERBI user to connect with SNOWFLAKE and granting privileges

CREATE OR REPLACE ROLE POWERBI_ROLE COMMENT='Power BI Role';
GRANT ALL ON WAREHOUSE POWERBI_WH TO ROLE POWERBI_ROLE;
GRANT ROLE POWERBI_ROLE TO ROLE SYSADMIN;

CREATE OR REPLACE USER POWERBI PASSWORD='PBISF123' 
    DEFAULT_ROLE=POWERBI_ROLE 
    DEFAULT_WAREHOUSE=POWERBI_WH
    DEFAULT_NAMESPACE=LAB_DW.PUBLIC
    COMMENT='Power BI User';

GRANT ROLE POWERBI_ROLE TO USER POWERBI;

-- Creating database and tables

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS LAB_DB;

GRANT USAGE ON DATABASE LAB_DB TO ROLE POWERBI_ROLE;

USE LAB_DB.PUBLIC;
USE WAREHOUSE ELT_WH;


CREATE OR REPLACE TABLE CATEGORY (
	CATEGORY_ID NUMBER(38,0),
	CATEGORY_NAME VARCHAR(50)
);

CREATE OR REPLACE TABLE CHANNELS (
	CHANNEL_ID NUMBER(38,0),
	CHANNEL_NAME VARCHAR(50)
);

CREATE OR REPLACE TABLE DEPARTMENT (
	DEPARTMENT_ID NUMBER(38,0),
	DEPARTMENT_NAME VARCHAR(50)
);

CREATE OR REPLACE TABLE ITEMS (
	ITEM_ID NUMBER(38,0),
	ITEM_NAME VARCHAR(250),
	ITEM_PRICE FLOAT,
	DEPARTMENT_ID NUMBER(38,0),
	CATEGORY_ID NUMBER(38,0),
	TMP_ITEM_ID NUMBER(38,0)
);

CREATE OR REPLACE TABLE SALES_ORDERS (
	SALES_ORDER_ID NUMBER(38,0),
	CHANNEL_CODE NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	PAYMENT_ID NUMBER(38,0),
	EMPLOYEE_ID NUMBER(38,0),
	LOCATION_ID NUMBER(38,0),
	SALES_DATE TIMESTAMP_NTZ(9),
	TMP_ORDER_ID FLOAT,
	TMP_ORDER_DOW NUMBER(38,0),
	TMP_USER_ID NUMBER(38,0)
);

CREATE OR REPLACE TABLE ITEMS_IN_SALES_ORDERS (
	SALES_ORDER_ID NUMBER(38,0),
	ITEM_ID NUMBER(38,0),
	ORDER_ID NUMBER(38,0),
	PROMOTION_ID NUMBER(38,0),
	QUANTITY FLOAT,
	REORDERED NUMBER(38,0),
	TMP_ORDER_ID FLOAT,
	TMP_PRODUCT_ID NUMBER(38,0)
);

CREATE OR REPLACE TABLE LOCATIONS (
	LOCATION_ID NUMBER(38,0),
	NAME VARCHAR(100),
	GEO2 VARCHAR(250),
	GEO GEOGRAPHY,
	LAT FLOAT,
	LONG FLOAT,
	COUNTRY VARCHAR(200),
	REGION VARCHAR(100),
	MUNICIPALITY VARCHAR(200),
	LONGITUDE FLOAT,
	LATITUDE FLOAT
);

CREATE OR REPLACE TABLE STATES (
	STATE_CODE NUMBER(38,0),
	STATE_NAME VARCHAR(250),
	REGION VARCHAR(250),
	STATE_GEO VARCHAR(16777216)
);

-- granting privileges to POWERBI_ROLE

GRANT USAGE ON SCHEMA LAB_DB.PUBLIC TO ROLE POWERBI_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA LAB_DB.PUBLIC TO ROLE POWERBI_ROLE

-- Creating external stage for data stored in Azure Blob Storage container

CREATE OR REPLACE STAGE LAB_DATA_STAGE 
url='xxx'
credentials=(azure_sas_token='xxx');

-- Creating a file format: no header, text 'NULL' to be treated as NULL

CREATE OR REPLACE FILE FORMAT CSVNOHEADER
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 0
    NULL_IF = ('NULL');


-- Checking files in external stage

LIST @LAB_DATA_STAGE;

-- Loading data

COPY INTO CATEGORY FROM @LAB_DATA_STAGE/category/ 
FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

COPY INTO CHANNELS FROM @LAB_DATA_STAGE/channels/ 
FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

COPY INTO DEPARTMENT from @LAB_DATA_STAGE/department/ 
FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

COPY INTO ITEMS from @LAB_DATA_STAGE/items/ 
FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

COPY INTO LOCATIONS from @LAB_DATA_STAGE/locations/ 
FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

COPY INTO STATES from @LAB_DATA_STAGE/states/ 
FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

LIST @LAB_DATA_STAGE/items_in_sales_orders/;

--COPY INTO ITEMS_IN_SALES_ORDERS from @LAB_DATA_STAGE/items_in_sales_orders/items_in_sales_orders_0_0_0.csv.gz FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

ALTER WAREHOUSE ELT_WH SET WAREHOUSE_SIZE = 'X-LARGE';

COPY INTO ITEMS_IN_SALES_ORDERS from   @LAB_DATA_STAGE/items_in_sales_orders/ 
FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);


COPY INTO SALES_ORDERS from @lab_data_stage/sales_orders/ FILE_FORMAT = (FORMAT_NAME = CSVNOHEADER);

ALTER WAREHOUSE ELT_WH SET WAREHOUSE_SIZE = 'X-SMALL';

-- Data modeling for PowerBi reports and dashboards
-- table locations, geo not used by powerbi, lat and long to be removed
describe table locations;
SELECT * FROM locations;
SELECT count(*) FROM locations;
SELECT count(lat) FROM locations;

-- table deparments; noticed duplication
SELECT department_id, COUNT(department_id) FROM department GROUP BY department_id ORDER BY COUNT(department_id) DESC;
SELECT * FROM department WHERE department_id = 39;

-- snowflake schema
SELECT region FROM states;
SELECT region FROM locations;

-- sales orders and items in sales order tables can be consolidated into a single table
SELECT * FROM sales_orders LIMIT 100;
SELECT * FROM items_in_sales_orders LIMIT 100;

-- Creating Views
CREATE OR REPLACE VIEW location_v AS
SELECT
    l.location_id,
    l.country,
    l.region,
    l.municipality,
    s.state_name,
    s.state_geo,
    l.longitude,
    l.latitude
FROM locations AS l
INNER JOIN states AS s
ON l.region = s.region;

SELECT * FROM location_v;

CREATE OR REPLACE VIEW items_V AS
SELECT
    i.item_id,
    i.item_name,
    i.item_price,
    c.category_name,
    d.department_name
FROM items as i
INNER JOIN department AS d
ON i.department_id = d.department_id
INNER JOIN category AS c
ON i.category_id = c.category_id
WHERE
    i.department_id !=39;

SELECT * FROM items_v;

CREATE OR REPLACE VIEW sales_orders_v AS
SELECT
    s.item_id,
    s.quantity,
    so.channel_code,
    so.location_id,
    so.sales_date,
    s.order_id
FROM items_in_sales_orders AS s
INNER JOIN sales_orders AS so
ON s.sales_order_id = so.sales_order_id;

SELECT * FROM sales_orders_v LIMIT 100;

-- creating aggregated view
CREATE OR REPLACE VIEW sales_orders_v_agg AS
SELECT
    item_id,
    channel_code as channel_id,
    location_id,
    SUM(quantity) AS total_quantity
FROM
    sales_orders_v
GROUP BY
    item_id,
    channel_code,
    location_id;

SELECT * FROM sales_orders_v_agg;
