# Project Introduction

The goal of this project is to design a star schema using dbt for a dataset sourced from an AWS S3 bucket and loaded into Snowflake. The dataset, centered around New York City sales, will undergo transformations to create an analytical model. This process involves defining fact and dimension tables and implementing necessary transformations in the staging models to facilitate analytical queries.

## Table of Contents

1. [Data Introduction](#data-introduction)
2. [Data Loading to Snowflake](#data-loading-to-snowflake)
3. [Star Schema](#star-schema)
4. [dbt Models](#dbt-Models)
    1. [Staging Models](#staging-models)
        - [STG_ABANOUB_NYC_SALES_CLEANED](#stg_abanoub_nyc_sales_cleaned)
        - [STG_ABANOUB_DIM_LOCATION](#stg_abanoub_dim_location)
        - [STG_ABANOUB_DIM_PROPERTY_AT_PRESENT](#stg_abanoub_dim_property_at_present)
        - [STG_ABANOUB_DIM_PROPERTY_AT_SALE](#stg_abanoub_dim_property_at_sale)
        - [STG_ABANOUB_DIM_SALES_DATE](#stg_abanoub_dim_sales_date)
    2. [Marts](#marts)
        - [ABANOUB_FACT_SALES](#abanoub_fact_sales)
    3. [Queries](#query)
        - [Query 1](#query_1)
        - [Query 2](#query_2)
        - [Query 3](#query_3)
        - [Query 4](#query_4)
        - [Query 5](#query_5)
        - [Query 6](#query_6)
        - [Query 7](#query_7)
        - [Query 8](#query_8)
        - [Query 9](#query_9)
        - [Query 10](#query_10)
        - [Query 11](#query_11)

# 1- Data Introduction

### Source: AWS S3 Bucket - New York City Sales

The dataset comprises information related to property sales in New York City [**NYC_SALES**](https://aws.amazon.com/marketplace/pp/prodview-27ompcouk2o6i?sr=0-3&ref_=beagle&applicationId=AWSMPContessa#dataSets). The key table columns and their summarized descriptions are as follows:

1. **Borough:**
   - The name of the borough in which the property is located.

2. **Neighborhood:**
   - Determined by Department of Finance assessors during property valuation.
   - Common neighborhood names align with Finance designations.

3. **Building Class Category:**
   - Facilitates easy identification of similar properties by broad usage.
   - Files are sorted by Borough, Neighborhood, Building Class Category, Block, and Lot.

4. **Tax Class at Present:**
   - Assigns each property to one of four tax classes based on usage.
     - *Class 1:* Residential property of up to three units.
     - *Class 2:* Other primarily residential property (cooperatives, condominiums).
     - *Class 3:* Property with equipment owned by utility companies.
     - *Class 4:* All other properties (offices, factories, warehouses, etc.).

5. **Glossary of Terms for Property Sales Files:**
   - **Block:**
     - Sub-division of the borough where real properties are located.
   - **Lot:**
     - Subdivision of a Tax Block representing the property's unique location.
   - **Easement:**
     - A right, such as a right of way, allowing limited use of another's property.
   - **Building Class at Present:**
     - Describes a property’s constructive use using letters and numbers.
   - **Address:**
     - Street address of the property as listed on the Sales File.
   - **Zip Code:**
     - Property’s postal code.
   - **Residential Units:**
     - Number of residential units at the listed property.
   - **Commercial Units:**
     - Number of commercial units at the listed property.
   - **Total Units:**
     - Total number of units at the listed property.
   - **Land Square Feet:**
     - Land area of the property listed in square feet.
   - **Gross Square Feet:**
     - Total area of all building floors measured from exterior surfaces.
   - **Year Built:**
     - Year the structure on the property was built.
   - **Building Class at Time of Sale:**
     - Describes a property’s constructive use at the time of sale.
   - **Sales Price:**
     - Price paid for the property.
   - **Sale Date:**
     - Date the property was sold.

# 2- Data Loading to Snowflake

### Setting Up Storage Integration

To initiate the data loading process, a Storage Integration named `s3_integration` has been created in Snowflake. This integration facilitates a seamless connection to our AWS S3 bucket. It is configured as an external stage, specifically designed for S3 storage. The integration is configured with the necessary AWS credentials, including the Storage Integration's storage location ARN.

### Creating a Staging Area

A Snowflake Stage, denoted as `s3_stage`, has been established to serve as an intermediary for data transfer between Snowflake and our AWS S3 bucket. This stage is configured to leverage the previously defined Storage Integration (`s3_integration`). The storage location ARN ensures precise identification and retrieval of data from the S3 bucket.

![stage](https://github.com/abanoubsamir0004/dbt_test/assets/153556384/59b3bec0-140a-4bb4-8325-d69281ae6b04)

### Data Validation and Labeling

Before transferring data from the staging area to Snowflake tables, a crucial step involves validating and labeling the data based on the year. The dataset spans three years: 2016, 2017, and 2018. SQL queries are employed to inspect each file in the staging area and verify that its content corresponds to the expected year. This meticulous validation ensures the integrity of the data before proceeding with the data transfer.

#### Example Query:

```sql
-- Validate data for the year 2016
SELECT YEAR(TRY_CAST(t.$22 AS DATE)) AS DATE_YEAR
FROM @S3_STAGE/2016_NYC_Property_Sales__10212019.csv t
WHERE DATE_YEAR != 2016;
```

# 3- Star Schema

 This image  offers a snapshot of the designed star schema, showcasing the relationships between fact and dimension tables.

![Star Schema](https://github.com/abanoubsamir0004/dbt_test/assets/153556384/4a8bab4b-1479-4ad2-a1b2-e21e2c8d261d)

# 4- dbt Models

## 1- Staging Models Folder

### 1.1- STG_ABANOUB_NYC_SALES_CLEANED

In the Staging folder, the `STG_ABANOUB_NYC_SALES_CLEANED` model focuses on cleaning the source data. The Common Table Expressions (CTEs) involved in this process include:

1. **NYC_SALES_WITH_PK_NUMBER:**
    - Assigns a unique primary key (PK_NUMBER) to each row in the source data from STG_ABANOUB_NYC_SALES_CLEANED.
2. **SPLIT_CLEAN_TAX_CLASS:**
    - Splits and cleans the TAX_CLASS_AT_PRESENT column into TAX_CLASS_AT_PRESENT and TAX_SUBCLASS_AT_PRESENT.
3. **BUILDING_CLASS_AT_PRESENT_CLEANED:**
    - Cleans the BUILDING_CLASS_AT_PRESENT column, replacing null or empty values with 'UNKNOWN'.
4. **APARTMENT_NUMBER_CLEANED:**
    - Extracts apartment numbers from the address, handling cases where the address contains a comma.
5. **RESIDENTIAL_UNITS_CAST:**
    - Casts and cleans the RESIDENTIAL_UNITS column, converting values with '-' to NULL and removing commas.
6. **COMMERCIAL_UNITS_CAST:**
    - Casts and cleans the COMMERCIAL_UNITS column, converting values with '-' to NULL and removing commas.
7. **TOTAL_UNITS_CAST:**
    - Casts and cleans the TOTAL_UNITS column, converting values with '-' to NULL and removing commas.
8. **RESIDENTIAL_UNITS_CLEANED:**
    - Adjusts RESIDENTIAL_UNITS column by calculating the difference when it is 0, using TOTAL_UNITS and COMMERCIAL_UNITS.
9. **COMMERCIAL_UNITS_CLEANED:**
    - Adjusts COMMERCIAL_UNITS column by calculating the difference when it is 0, using TOTAL_UNITS and RESIDENTIAL_UNITS.
10. **TOTAL_UNITS_CLEANED:**
    - Adjusts TOTAL_UNITS column by summing RESIDENTIAL_UNITS_CLEANED and COMMERCIAL_UNITS_CLEANED.
11. **LAND_SQUARE_FEET_CLEANED:**
    - Cleans the LAND_SQUARE_FEET column, converting values with '-' to NULL and removing commas.
12. **GROSS_SQUARE_FEET_CLEANED:**
    - Cleans the GROSS_SQUARE_FEET column, converting values with '-' to NULL and removing commas.
13. **YEAR_BUILT_CLEANED:**
    - Cleans the YEAR_BUILT column, converting 0 values to NULL.
14. **SALE_PRICE_CLEANED:**
    - Cleans the SALE_PRICE column, converting values with '-' or '$0' to NULL and removing commas.
15. **SALE_DATE_CLEANED:**
    - Updates the year format for dates in 2017 and 2018 and creates a SALE_DATE column.
16. **FINAL:**
    - Joins all cleaned and processed columns from previous CTEs and generates the final dataset for analysis.

### 1.2- STG_ABANOUB_DIM_LOCATION

In the Staging folder, the `STG_ABANOUB_DIM_LOCATION` model creates a dimension table named DIM_LOCATION by extracting unique location-related information from the STG_ABANOUB_NYC_SALES_CLEANEND source.

**Description:**
The `DIM_LOCATION` dimension table is designed to capture location-related details extracted from the STG_ABANOUB_NYC_SALES_CLEANEND source. The table includes a unique identifier, `LOCATION_ID`, which combines borough, neighborhood, and ZIP code information for a distinct representation of each location.

### 1.3- STG_ABANOUB_DIM_PROPERTY_AT_PRESENT

In the Staging folder, the `STG_ABANOUB_DIM_PROPERTY_AT_PRESENT` model creates a dimension table named DIM_PROPERTY_AT_PRESENT by capturing distinct property-related attributes from the STG_ABANOUB_NYC_SALES_CLEANEND source.

**Description:**
The `DIM_PROPERTY_AT_PRESENT` dimension table focuses on extracting and organizing distinct property-related attributes from the STG_ABANOUB_NYC_SALES_CLEANEND source. It includes a unique identifier, `PROPERTY_AT_PRESENT_ID`, assigned through dense ranking based on building class category, building class at present, tax class at present, and tax subclass at present.

### 1.4- STG_ABANOUB_DIM_PROPERTY_AT_SALE

In the Staging folder, the `STG_ABANOUB_DIM_PROPERTY_AT_SALE` model creates a dimension table named DIM_PROPERTY_AT_SALE by extracting unique property-related details from the STG_ABANOUB_NYC_SALES_CLEANEND source.

**Description:**
The `DIM_PROPERTY_AT_SALE` dimension table is designed to capture unique property-related details from the STG_ABANOUB_NYC_SALES_CLEANEND source. It includes a unique identifier, `PROPERTY_AT_SALE_ID`, generated through dense ranking based on building class at the time of sale, tax class at the time of sale, and year built.

### 1.5- STG_ABANOUB_DIM_SALES_DATE

In the Staging folder, the `STG_ABANOUB_DIM_SALES_DATE` model creates a dimension table named DIM_SALE_DATE by extracting and formatting date-related details from the STG_ABANOUB_NYC_SALES_CLEANEND source.

**Description:**
The `DIM_SALE_DATE` dimension table focuses on extracting and formatting date-related details from the STG_ABANOUB_NYC_SALES_CLEANEND source. It includes a unique identifier, `SALES_DATE_ID`, representing the sale date in YYYYMMDD format.

## 2- Marts

### ABANOUB_FACT_SALES

In the Marts folder, the `ABANOUB_FACT_SALES` model creates a fact table that consolidates information by joining with several dimension tables. The dimensions include:

- `DIM_LOCATION`: Captures location-related details.
- `DIM_PROPERTY_AT_PRESENT`: Organizes distinct property-related attributes.
- `DIM_PROPERTY_AT_SALE`: Extracts unique property-related details.
- `DIM_SALE_DATE`: Manages and formats date-related details.

**Description:**
The `ABANOUB_FACT_SALES` fact table is constructed through a join with various dimension tables, enhancing analytical capabilities by incorporating context from different dimensions. The joined dimension tables include `DIM_LOCATION`, `DIM_PROPERTY_AT_PRESENT`, `DIM_PROPERTY_AT_SALE`, and `DIM_SALE_DATE`.


**Foreign Keys:**
The fact table includes essential foreign key derived from the various dimension tables , such as:
- `SALES_DATE_ID`
- `LOCATION_ID`
- `PROPERTY_AT_PRESENT_ID`
- `LAND_SQUARE_FEET`
- `PROPERTY_AT_SALE_ID`

**Measures:**
The fact table includes essential measures derived from the cleaned data in `STG_ABANOUB_NYC_SALES_CLEANED`, such as:
- `RESIDENTIAL_UNITS`
- `COMMERCIAL_UNITS`
- `TOTAL_UNITS`
- `LAND_SQUARE_FEET`
- `GROSS_SQUARE_FEET`
- `SALE_PRICE`

**Degenerated Dimensions:**
Additionally, the fact table includes degenerated dimensions like:
- `TAX_BLOCK`
- `TAX_LOT`
- `ADDRESS`
- `APARTMENT_NUMBER`

These degenerated dimensions provide specific details related to tax block, tax lot, property address, and apartment number.

**Final Query:**
The final query for this model performs a join with the mentioned dimension tables, extracts the necessary foreign key identifiers, and selects the basic measurements from the cleaned data in `STG_ABANOUB_NYC_SALES_CLEANED`.

## 3- Queries

### 3.1- Query 1

### 3.2- Query 2

### 3.3- Query 3

### 3.4- Query 4

### 3.5- Query 5

### 3.6- Query 6

### 3.7- Query 7

### 3.8- Query 8

### 3.9- Query 9

### 3.10- Query 10

### 3.11- Query 11



---

