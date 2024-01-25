# SuperStore Project Introduction

The goal of this project is to design a star schema using dbt for a dataset sourced from an Kaggle and loaded into Snowflake. The dataset, centered Superstore sales, will undergo transformations to create an analytical model. This process involves defining fact and dimension tables and implementing necessary transformations in the staging models to facilitate analytical queries.

## Table of Contents

# Table of Contents

## Table of Contents

1. [Data Introduction](#data-introduction)
2. [Data Loading to Snowflake](#data-loading-to-snowflake)
3. [Star Schema](#star-schema)
4. [dbt Models](#dbt-models)
    1. [Staging](#staging)
        - [SUPERSTORE](#superstore)
    2. [DWH Models](#dwh-models)
        - [SUPERSTORE_DIMENSIONS](#superstore_dimensions)
            - [DIM_CUSTOMER](#dim_customer)
            - [DIM_DATE](#dim_date)
            - [DIM_LOCATION](#dim_location)
            - [DIM_ORDER](#dim_order)
            - [DIM_PRODUCT](#dim_product)
        - [SUPERSTORE_FACT](#superstore_fact)

    3. [Marts](#Marts)
        - [Query 1](#query-1)
 
5. [Most Common Data Issues](#most-common-data-issues)



# 1- Data Introduction <a name="data-introduction"></a>

### Source: Kaggle - SAMPLE Superstore

The dataset comprises information related to property sales about [**SUPERSTORE**](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final). The key table columns and their summarized descriptions are as follows:

1. **Row ID:**
   - Unique ID for each row.

2. **Order ID:**
   - Unique Order ID for each Customer.

3. **Order Date:**
   - Order Date of the product.

4. **Ship Date:**
   - Shipping Date of the Product.

5. **Ship Mode:**
   - Shipping Mode specified by the Customer.

6. **Customer ID:**
   - Unique ID to identify each Customer.

7. **Customer Name:**
   - Name of the Customer.

8. **Segment:**
   - The segment where the Customer belongs.

9. **Country:**
   - Country of residence of the Customer.

10. **City:**
    - City of residence of of the Customer.

11. **State:**
   - State of residence of the Customer.

12. **Postal Code:**
   - Postal Code of every Customer.

13. **Region:**
   - Region where the Customer belong.

14. **Product ID:**
   - Unique ID of the Product.

15. **Category:**
   - Category of the product ordered.

16. **Sub-Category:**
   - Sub-Category of the product ordered.

17. **Product Name:**
   - Name of the Product.

18. **Sales:**
   - Sales of the Product.

19. **Quantity:**
   - Quantity of the Product.

20. **Discount:**
   - Discount provided.

21. **Profit:**
   - Profit/Loss incurred.

# 2- Data Loading to Snowflake <a name="data-loading-to-snowflake"></a>

### Creating a Staging Area

A Snowflake Stage, denoted as SUPERSTORE_STAGE, has been established to serve as an intermediary for data transfer between Snowflake and my local machine. I use "Put" and "Copy Into" commands to get the data from the local machine and load it into Snowflake.
![stage](https://github.com/abanoubsamir0004/dbt_test/assets/153556384/952aca32-eee6-4f72-8eb3-bea07b7149a1)

# 3- Star Schema <a name="star-schema"></a>

 This image  offers a snapshot of the designed star schema, showcasing the relationships between fact and dimension tables.

![SUPERSTORE_STAR_SCHEMA](https://github.com/abanoubsamir0004/dbt_test/assets/153556384/1a2af10d-225a-42fb-adc5-34c3853e0a19)

# 4- dbt Models <a name="dbt-models"></a>

## 4.1- Staging Models Folder <a name="staging"></a>

### 4.1.1 - SUPERSTORE  <a name="superstore"></a>

The `STG_SUPERSTORE` model serves as the staging layer for the SUPERSTORE data, extracting and cleaning the raw data to prepare it for downstream analytics.

**Description:**
The `STG_SUPERSTORE` staging model extracts relevant columns from the raw data in the "SAMPLE_SUPERSTORE.SUPERSTORE" source. It includes data cleaning steps, such as converting the "POSTAL_CODE" column to an integer and trimming whitespace from the "PRODUCT_NAME" column.

**Source:**
The data is sourced from the "SAMPLE_SUPERSTORE.SUPERSTORE" table.

**Columns:**

- **ROW_ID**: Row identifier.
- **ORDER_ID**: Order identifier.
- **ORDER_DATE**: Date of the order.
- **SHIP_DATE**: Date of shipment.
- **SHIP_MODE**: Shipping mode.
- **CUSTOMER_ID**: Customer identifier.
- **CUSTOMER_NAME**: Name of the customer.
- **SEGMENT**: Customer segment.
- **COUNTRY**: Country of the order.
- **CITY**: City of the order.
- **STATE**: State of the order.
- **POSTAL_CODE**: Postal code of the order (converted to integer).
- **REGION**: Region of the order.
- **PRODUCT_ID**: Product identifier.
- **CATEGORY**: Product category.
- **SUB_CATEGORY**: Product subcategory.
- **PRODUCT_NAME**: Trimmed product name.
- **SALES**: Sales amount.
- **QUANTITY**: Quantity of products.
- **DISCOUNT**: Discount applied.
- **PROFIT**: Profit amount.

This staging model ensures that the data is cleansed and prepared for subsequent analysis in downstream models.

## 4.2- DWH Models Folder <a name="dwh-models"></a>

## 4.2.1- SUPERSTORE DIMENSIONS <a name="superstore_dimensions"></a>

### 4.2.1.1 - DIM_CUSTOMER  <a name="dim_customer"></a>

In the Staging folder, the `DIM_CUSTOMER` model creates a dimension table named DIM_CUSTOMER by extracting unique customer-related information from the SUPERSTORE source.

**Description:**
The `DIM_CUSTOMER` dimension table is designed to capture customer-related details extracted from the SUPERSTORE source. The table includes a unique identifier, `CUSTOMER_KEY`, derived from the original `CUSTOMER_ID` in the source table. Additional information such as `CUSTOMER_NAME`, `SEGMENT`, and `POSTAL_CODE` is included for a comprehensive representation of each customer.

**Table Structure:**

| Column         | Description                           |
| -------------- | ------------------------------------- |
| CUSTOMER_KEY   | Unique identifier for each customer.   |
| CUSTOMER_NAME  | Name of the customer.                  |
| SEGMENT        | Customer segmentation information.    |
| POSTAL_CODE    | Postal code associated with the customer. |

This dimension table facilitates a clearer understanding of customer-related data and serves as a valuable reference for analytical queries.

### 4.2.1.2 - DIM_DATE  <a name="dim_date"></a>

The `DIM_DATE` model creates a date dimension table named DIM_DATE using the dbt_date package to generate a comprehensive date range from January 1, 2010, to January 1, 2023.

**Description:**
The `DIM_DATE` dimension table is designed to capture date-related details for analytical purposes. The table includes a unique identifier, `DATE_KEY`, derived from the `DATE_DAY` column in the source. Additional date-related attributes such as `FULL_DATE`, `YEAR`, `MONTH`, `DAY`, `QUARTER`, and `QUARTER_NAME` are provided for a detailed representation of each date.

**Table Structure:**

| Column         | Description                                   |
| -------------- | --------------------------------------------- |
| DATE_KEY       | Unique identifier for each date.               |
| FULL_DATE      | Complete date representation.                  |
| YEAR           | Year extracted from the date.                  |
| MONTH          | Month extracted from the date (padded with 0). |
| DAY            | Day extracted from the date (padded with 0).   |
| QUARTER        | Quarter of the year.                           |
| QUARTER_NAME   | Quarter name (e.g., First Quarter).            |

This dimension table facilitates date-based analysis and serves as a valuable reference for time-related queries.

### 4.2.1.3 - DIM_LOCATION  <a name="dim_location"></a>

The `DIM_LOCATION` model creates a dimension table named DIM_LOCATION by extracting unique location-related information from the SUPERSTORE source.

**Description:**
The `DIM_LOCATION` dimension table is designed to capture unique location-related details extracted from the SUPERSTORE source. The table includes a surrogate key, `LOCATION_SK`, assigned through row numbering for a distinct representation of each location. Additional location attributes such as `COUNTRY`, `STATE`, `CITY`, and `REGION` are included.

**Table Structure:**

| Column         | Description                              |
| -------------- | ---------------------------------------- |
| LOCATION_SK    | Surrogate key for each unique location.  |
| COUNTRY        | Country of the location.                 |
| STATE          | State of the location.                   |
| CITY           | City of the location.                    |
| REGION         | Region of the location.                  |

This dimension table facilitates a clearer understanding of location-related data and serves as a valuable reference for analytical queries.

### 4.2.1.4 - DIM_ORDER  <a name="dim_order"></a>

The `DIM_ORDER` model creates a dimension table named DIM_ORDER by extracting unique order-related information from the SUPERSTORE source.

**Description:**
The `DIM_ORDER` dimension table is designed to capture unique order-related details extracted from the SUPERSTORE source. The table includes a unique identifier, `ORDER_KEY`, derived from the original `ORDER_ID` in the source table. Additional information such as `SHIP_MODE` is included for a comprehensive representation of each order.

**Table Structure:**

| Column      | Description                               |
| ----------- | ----------------------------------------- |
| ORDER_KEY   | Unique identifier for each order.          |
| SHIP_MODE   | Shipping mode for the order.               |

This dimension table facilitates a clearer understanding of order-related data and serves as a valuable reference for analytical queries.

### 4.2.1.5 - DIM_PRODUCT  <a name="dim_product"></a>

During exploration of the product data, it was observed that there are 32 product IDs, each associated with 2 different product names. To address this, the `DIM_PRODUCT` model creates a dimension table named DIM_PRODUCT by extracting distinct product-related information from the SUPERSTORE source.

**Description:**
The `DIM_PRODUCT` dimension table captures unique product-related details, ensuring that each product has a distinct representation. The table includes a surrogate key, `PRODUCT_KEY`, assigned through row numbering for uniqueness. The product's natural key consists of `PRODUCT_ID`, `PRODUCT_NAME`, `CATEGORY`, and `SUB_CATEGORY`.

**Table Structure:**

| Column        | Description                              |
| ------------- | ---------------------------------------- |
| PRODUCT_KEY   | Surrogate key for each unique product.   |
| PRODUCT_ID    | Unique identifier for each product.      |
| PRODUCT_NAME  | Name of the product.                     |
| CATEGORY      | Category to which the product belongs.   |
| SUB_CATEGORY  | Subcategory of the product.              |

This dimension table ensures that all products are represented uniquely, allowing for effective analysis and referencing in analytical queries.

## 4.2.2- SUPERSTORE FACT <a name="superstore_fact"></a>

### 4.2.2.1 - FACT_SALES  <a name="superstore_fact"></a>

The `FACT_SALES` model creates a fact table named FACT_SALES by combining data from various dimension tables and the SUPERSTORE source, facilitating comprehensive analysis of sales-related information.

**Description:**
The `FACT_SALES` fact table captures detailed sales-related information by integrating data from the following dimension tables:

- `DIM_DATE`: Date dimension.
- `DIM_LOCATION`: Location dimension.
- `DIM_PRODUCT`: Product dimension.
- `DIM_CUSTOMER`: Customer dimension.
- `DIM_ORDER`: Order dimension.

The table includes keys from each dimension, providing a link to detailed information about the order date, ship date, location, product, customer, and order. Sales-specific metrics such as sales amount, quantity, discount, and profit are also included.

**Table Structure:**

| Column          | Description                                        |
| --------------- | -------------------------------------------------- |
| ORDER_DATE_KEY  | Foreign key linking to the date dimension for order date.  |
| SHIP_DATE_KEY   | Foreign key linking to the date dimension for ship date.   |
| LOCATION_SK     | Foreign key linking to the location dimension.             |
| PRODUCT_KEY     | Foreign key linking to the product dimension.              |
| CUSTOMER_KEY    | Foreign key linking to the customer dimension.             |
| ORDER_KEY       | Foreign key linking to the order dimension.                |
| SALES           | Sales amount for the order.                              |
| QUANTITY        | Quantity of products in the order.                       |
| DISCOUNT        | Discount applied to the order.                           |
| PROFIT          | Profit amount for the order.                             |

This fact table provides a consolidated view of sales-related data, enabling in-depth analysis across multiple dimensions.

## 4.3- Marts  <a name="marts"></a>

### 4.3.1 - Query 1 <a name="query-1"></a>


# 5- Most Common Data Issues <a name="most-common-data-issues"></a>

1. **"Product ID" Column:**
   - Contains multiple entries with the same "PRODUCT_ID" but different "PRODUCT_NAME".
