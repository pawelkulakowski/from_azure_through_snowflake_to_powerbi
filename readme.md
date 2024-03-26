# Colecting data from blob storage account on Azure, loading data to Snowflake, preparing data models for PowerBI, creating composite models

## Azure 
Creating a blob storage account on Azure
- using new resource group
- creating a blob container
- generating SAS token for accesing the container
- loading files to a container

## Snowflake
- creating two seperate warehouses, first one for getting the data from stage, second one for powerbi purposes
- creating a new role for powerbi purposes
- creating database and tables
- granting privileges to a new role for powerbi purposes
- creating file format
- loading the data from stage
- temporary changing the size of the warehouse for getting larger files into snowflake
- data modelling for power bi, creating new views:
    - location_v by joining two tables
    - items_v by joining two tables and removing duplicated data
    - sales_order_v by joing sales orders data with items in sales orders
    - sales_order_v_agg creating aggregated view

## PowerBi
- setting the connection between snowflake and powerbi
- using performance analyzer to analyze the timings
- creating and configuring the composite model:
    - dual mode: channels, items_v, location_v
    - direct query: sales_orders_v
    - creating a relationship between tables and aggregated table

## Dax Studio
- analyzing queries and server timings