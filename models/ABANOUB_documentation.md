# Project Introduction

The goal of this project is to design a star schema using dbt for a dataset sourced from an AWS S3 bucket and loaded into Snowflake. The dataset, centered around New York City sales, will undergo transformations to create an analytical model. This process involves defining fact and dimension tables and implementing necessary transformations in the staging models to facilitate analytical queries.

## Table of Contents

# Table of Contents

## Table of Contents

1. [Data Introduction](#1--data-introduction)
2. [Data Loading to Snowflake](#2--data-loading-to-snowflake)
3. [Star Schema](#3--star-schema)
4. [dbt Models](#4--dbt-models)
    1. [Staging Models](#41--staging-models)
        - [STG_ABANOUB_NYC_SALES_CLEANED](#411--stg_abanoub_nyc_sales_cleaned)
        - [STG_ABANOUB_DIM_LOCATION](#412--stg_abanoub_dim_location)
        - [STG_ABANOUB_DIM_PROPERTY_AT_PRESENT](#413--stg_abanoub_dim_property_at_present)
        - [STG_ABANOUB_DIM_PROPERTY_AT_SALE](#414--stg_abanoub_dim_property_at_sale)
        - [STG_ABANOUB_DIM_SALES_DATE](#415--stg_abanoub_dim_sales_date)
    2. [Marts](#42--marts)
        - [ABANOUB_FACT_SALES](#421--abanoub_fact_sales)
    3. [Queries](#43--queries)
        - [Query 1](#431--query-1)
        - [Query 2](#432--query-2)
        - [Query 3](#433--query-3)
        - [Query 4](#434--query-4)
        - [Query 5](#435--query-5)
        - [Query 6](#436--query-6)
        - [Query 7](#437--query-7)
        - [Query 8](#438--query-8)
        - [Query 9](#439--query-9)
        - [Query 10](#4310--query-10)
        - [Query 11](#4311--query-11)
5. [Most Common Data Issues](#5--most-common-data-issues)



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

In the Staging folder, the `STG_ABANOUB_NYC_SALES_CLEANED` model focuses on cleaning the source data. The (CTEs) involved in this process include:

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

### 3.1 - Query 1

### Calculate the Average Sale Price per Borough

This query aims to calculate the average sale price per borough by utilizing two (CTEs) for improved readability and organization.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. AVG_SALE_PRICE_PER_BOROUGH
   - Calculates the average sale price for each borough.
   - Joins the "FACT_SALES" CTE with the 'STG_ABANOUB_DIM_LOCATION' table on the common attribute 'LOCATION_ID'.
   - Groups the results by borough.
   - Orders the result set in ascending order based on borough names.

### Final Output:
   - The "AVG_SALE_PRICE_PER_BOROUGH" CTE extracts borough information and computes the average sale price, providing a clear and organized approach. The final output includes distinct boroughs and their corresponding average sale prices, ordered in ascending order based on borough names.

### 3.2- Query 2
### Find the Neighborhood with the Most Total Units

This query is designed to identify the neighborhood with the highest total number of units. The logic is organized using three (CTEs) to enhance clarity and structure the code.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_LOCATION
   - Extracts data from the 'STG_ABANOUB_DIM_LOCATION' table.

#### 3. TOTAL_UNITS_BY_NEIGHBORHOOD
   - Calculates the total number of units for each neighborhood by summing up the 'TOTAL_UNITS' column from the "FACT_SALES" CTE.
   - Uses an INNER JOIN with the "DIM_LOCATION" CTE on the common attribute 'LOCATION_ID' to associate sales data with neighborhood information.
   - Filters out records where the 'TOTAL_UNITS' value is NULL.
   - Groups the results by neighborhood.

### Final Output:
   - The final SELECT statement retrieves all columns from the "TOTAL_UNITS_BY_NEIGHBORHOOD" CTE, orders the result set in descending order based on the 'TOTAL_UNITS' column, and uses the LIMIT 1 clause to obtain only the top row representing the neighborhood with the highest total number of units.

### 3.3- Query 3
### Identify the Building Class Category with the Highest Average Land Square Feet

This query is designed to identify the building class category with the highest average land square feet. The logic is organized using three (CTEs) for improved clarity and structure.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_PROPERTY_AT_PRESENT
   - Extracts data from the 'STG_ABANOUB_DIM_PROPERTY_AT_PRESENT' table.

#### 3. AVG_LAND_SQUARE_FEET_BY_BUILDING_CLASS
   - Calculates the average land square feet for each building class category.
   - Joins the "FACT_SALES" CTE with the 'DIM_PROPERTY_AT_PRESENT' table on the common attribute 'PROPERTY_AT_PRESENT_ID.'
   - Filters out records where the 'LAND_SQUARE_FEET' value is NULL.
   - Groups the results by building class category.

### Final output:
   - The final SELECT statement retrieves all columns from the "AVG_LAND_SQUARE_FEET_BY_BUILDING_CLASS" CTE, orders the result set in descending order based on the 'AVERAGE_LAND_SQUARE_FEET' column, and uses the LIMIT 1 clause to obtain only the top row representing the building class category with the highest average land square feet.

### Final Output:
### Count the Number of Buildings by Different Dimensions

This query is designed to count the number of buildings based on various dimensions, considering each unique building by retrieving distinct rows using the combination of block and lot along with the location dimension. The logic is organized using multiple (CTEs) to capture building counts for different dimensions.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_LOCATION
   - Extracts data from the 'STG_ABANOUB_DIM_LOCATION' table.

#### 3. UNIQUE_BUILDINGS
   - Captures distinct combinations of borough, neighborhood, tax block, and tax lot to represent unique buildings.
   - Uses an INNER JOIN with the "DIM_LOCATION" CTE on the common attribute 'LOCATION_ID.'

#### 4. COUNT_DIFF_BUILDING_PER_BOROUGH_NAME
   - Counts the number of buildings based on the 'BOROUGH_NAME' dimension.
   - Groups by 'BOROUGH_NAME' and calculates the building count.

#### 5. COUNT_DIFF_BUILDING_PER_NEIGHBORHOOD
   - Counts the number of buildings based on the 'NEIGHBORHOOD' dimension.
   - Groups by 'BOROUGH_NAME' and 'NEIGHBORHOOD' and calculates the building count.

#### 6. COUNT_DIFF_BUILDING_PER_TAX_BLOCK
   - Counts the number of buildings based on the 'TAX_BLOCK' dimension.
   - Groups by 'BOROUGH_NAME,' 'NEIGHBORHOOD,' and 'TAX_BLOCK' and calculates the building count.

#### 7. FINAL
   - Combines the building counts from different dimensions using UNION.

### Final output:
  - The final SELECT statement retrieves all columns from the "FINAL" CTE, providing a comprehensive view of the building counts across different dimensions. The result set is ordered by dimension, borough name, neighborhood, and tax block for clarity.

### Final Output:
### Calculate the Total Sale Price Over Time by Different Date Parts

This query aims to calculate the total sale price over time, categorized by different date parts, namely 'YEAR,' 'MONTH,' and 'QUARTER.' The logic is organized using (CTEs) to capture the total sale price for each date part.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_SALES_DATE
   - Extracts data from the 'STG_ABANOUB_DIM_SALES_DATE' table.

#### 3. TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_YEAR
   - Calculates the total sale price grouped by 'YEAR.'
   - Groups by 'SALE_YEAR' and calculates the total sale price.

#### 4. TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_MONTH
   - Calculates the total sale price grouped by 'YEAR' and 'MONTH.'
   - Groups by 'SALE_YEAR' and 'SALE_MONTH' and calculates the total sale price.

#### 5. TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_QUARTER
   - Calculates the total sale price grouped by 'YEAR' and 'QUARTER.'
   - Groups by 'SALE_YEAR' and 'SALE_QUARTER' and calculates the total sale price.

#### 6. FINAL
   - Combines the total sale price results from different date parts using UNION ALL.
   - Converts numeric quarter values to human-readable quarter names.

### Final Output:
   - The final SELECT statement retrieves all columns from the "FINAL" CTE, providing a comprehensive view of the total sale price over time by different date parts. The result set is ordered by dimension, sale year, sale month, and sale quarter for clarity.

### 3.6- Query 6
### Group the Data by Tax Class at Present and Tax Class at Time of Sale and Compare the Average Sale Price for Each Combination

This query is designed to group the data by tax class at present and tax class at the time of sale, then compare the average sale price for each combination. The logic is structured using (CTEs) to calculate average sale prices for both tax class dimensions and perform a comparison.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_PROPERTY_AT_PRESENT
   - Extracts data from the 'STG_ABANOUB_DIM_PROPERTY_AT_PRESENT' table.

#### 3. DIM_PROPERTY_AT_SALE
   - Extracts data from the 'STG_ABANOUB_DIM_PROPERTY_AT_SALE' table.

#### 4. TAX_CLASS_AT_PRESENT_AVERAGE_SALE_PRICE
   - Calculates the average sale price for each tax class at present.
   - Filters out records where 'TAX_CLASS_AT_PRESENT' is 'UNKNOWN.'
   - Groups by 'TAX_CLASS_AT_PRESENT' and calculates the average sale price.

#### 5. TAX_CLASS_AT_SALE_AVERAGE_SALE_PRICE
   - Calculates the average sale price for each tax class at the time of sale.
   - Groups by 'TAX_CLASS_AT_TIME_OF_SALE' and calculates the average sale price.

#### 6. FINAL
   - Combines the results from both tax class dimensions using an INNER JOIN.
   - Adds a comparison column indicating which tax class has a higher average sale price.

### Final Output:
   - The final SELECT statement retrieves all columns from the "FINAL" CTE, providing a comprehensive view of the average sale prices for each tax class combination and their comparison. The result set is ordered by tax class at present for clarity.

### 3.7- Query 7
### Identify the Top 5 Most Expensive Buildings Based on Sale Price

This query is designed to identify the top 5 most expensive buildings based on the sale price. The logic is organized using (CTEs) to capture distinct combinations of borough, neighborhood, tax block, tax lot, and sale price, then rank and select the top 5 based on the maximum sale price.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_LOCATION
   - Extracts data from the 'STG_ABANOUB_DIM_LOCATION' table.

#### 3. UNIQUE_BUILDINGS
   - Captures distinct combinations of borough, neighborhood, tax block, tax lot, and sale price.
   - Uses an INNER JOIN with the "DIM_LOCATION" CTE on the common attribute 'LOCATION_ID.'

#### 4. TOP_5_EXPENSIVE_BUILDINGS
   - Selects the top 5 most expensive buildings based on the maximum sale price.
   - Filters out records where sale price is 0 or NULL.
   - Groups by borough, neighborhood, tax block, and tax lot, and orders the result set by sale price in descending order.
   - Limits the result set to the top 5 records.

### Final Output:
   - The final SELECT statement retrieves all columns from the "TOP_5_EXPENSIVE_BUILDINGS" CTE, providing details on the top 5 most expensive buildings based on sale price.

### 3.8- Query 8
### Use a Window Function to Calculate the Running Total of Sales Price

This query utilizes a window function to calculate the running total of sales price over time. The logic is structured using (CTEs) to capture distinct combinations of sale year and month, then apply the window function to calculate the running total of sales price.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_SALES_DATE
   - Extracts data from the 'STG_ABANOUB_DIM_SALES_DATE' table.

#### 3. RUNNING_SALES_PRICE_TOTAL
   - Captures distinct combinations of sale year and month.
   - Applies a window function using the SUM() function over the ordered sale year and month to calculate the running total of sales price.

### Final Output:
  - The final SELECT statement retrieves all columns from the "RUNNING_SALES_PRICE_TOTAL" CTE, providing a comprehensive view of the running total of sales price over time. The result set is ordered by sale year and month for clarity.

### 3.9- Query 9
### Create a New Column with the Difference in Years Between Sale Date and Year Built, and Analyze the Distribution of Sale Price

This query is designed to create a new column representing the difference in years between the sale date and the year a property was built. It then groups the data by this new column and analyzes the distribution of sale prices, including various statistical measures.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_PROPERTY_AT_SALE
   - Extracts data from the 'STG_ABANOUB_DIM_PROPERTY_AT_SALE' table.

#### 3. DIM_SALES_DATE
   - Extracts data from the 'STG_ABANOUB_DIM_SALES_DATE' table.

#### 4. SALES_WITH_AGE_DIFF
   - Calculates the difference in years between sale date and year built for each property.
   - Filters out records with NULL or specific outlier values in the 'YEAR_BUILT' column and records with a sale price of 0 or NULL.

#### 5. FINAL
   - Groups the data by the calculated age difference.
   - Calculates various statistical measures for the distribution of sale prices, including count, average, minimum, maximum, quartiles (Q1, Q3), and median.

### Final Output:
   - The final SELECT statement retrieves all columns from the "FINAL" CTE, providing a comprehensive view of the distribution of sale prices based on the difference in years between sale date and year built.

### 3.10- Query 10
### Identify Buildings Sold Multiple Times and Analyze Sale Price Changes Over Time

This query aims to identify buildings that have been sold multiple times and analyze the changes in sale prices over those transactions. The logic is structured using (CTEs) to identify unique buildings sold multiple times and then utilizes window functions to compare current and previous sale prices.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_LOCATION
   - Extracts data from the 'STG_ABANOUB_DIM_LOCATION' table.

#### 3. DIM_SALES_DATE
   - Extracts data from the 'STG_ABANOUB_DIM_SALES_DATE' table.

#### 4. UNIQUE_BUILDINGS_SOLDED_MULTIPLE_TIME
   - Counts buildings that have been sold multiple times.
   - Groups by borough, neighborhood, tax block, and tax lot.
   - Filters out records with sale prices equal to 0 or NULL.
   - Filters only buildings with more than one sale transaction.

#### 5. FINAL
   - Joins the identified buildings with multiple sales with the sales data.
   - Utilizes the LAG() window function to retrieve the previous sale price for each transaction.
   - Orders the result set for better analysis.

### Final Output:
   - The final SELECT statement retrieves all columns from the "FINAL" CTE, providing details on buildings sold multiple times and the corresponding sale prices over different transactions.

### 3.11- Query 11
### Determine Building Age Category Based on Year Built and Analyze the Relationship Between Building Age and Sale Price

This query is designed to determine the building age category based on the year built and then analyze the relationship between building age and sale price. The logic is organized using (CTEs) to calculate the building age category and then aggregate and analyze sales data based on this categorization.

### Common Table Expressions (CTEs):

#### 1. FACT_SALES
   - Retrieves data from the 'ABANOUB_FACT_SALES' table.

#### 2. DIM_PROPERTY_AT_SALE
   - Extracts data from the 'STG_ABANOUB_DIM_PROPERTY_AT_SALE' table.

#### 3. DIM_SALES_DATE
   - Extracts data from the 'STG_ABANOUB_DIM_SALES_DATE' table.

#### 4. BUILDING_AGE_CATEGORY
   - Calculates the building age based on the year built and sale year.
   - Excludes records with specific outlier values in the 'YEAR_BUILT' column.
   - Excludes records with NULL values in the 'YEAR_BUILT' column.

#### 5. FINAL
   - Groups the data by sale year, year built, and building age.
   - Calculates various statistical measures for the distribution of sale prices, including count, minimum, maximum, and average.

### Final Output:
   - The final SELECT statement retrieves all columns from the "FINAL" CTE, providing a comprehensive view of the relationship between building age and sale price over different years.

# 5- Most Common Data Issues

1. **Easement" Column:**
   - Contains all values as null or empty strings.

2. **Sale Date" Column:**
   - Year values appear as '0017' and '0018' instead of '2017' and '2018'.

3. **Sale Price" Column:**
   - Contains null values, zeros, and the '-' symbol.

4. **Tax Class at Present" Column:**
   - Some values have subclasses like 1A, 2B, 3C, 4B; it should be 1, 2, 3, or 4.

5. **Apartment Number" Column:**
   - Most data is null, and the address column has some apartment numbers after a comma in its string.

6. **Numeric Columns:**
   - Columns like "Residential Units," "Commercial Units," "Total Units," "Land Square Feet," "Gross Square Feet" have values of zeros, nulls, and the '-' symbol. These columns need to be cast to integer type.

7. **Year Built Column:**
   - Contains values like '1111' (not reliable) and '2019' (data sales according to 2016 to 2018 only). Ensure accurate data within the specified range.
---

