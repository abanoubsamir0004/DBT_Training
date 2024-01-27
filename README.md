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

    3. [ANALYTICS](#analytics)
        - [Yearly Metrics Calculation](#query-1)
        - [Monthly Customer Total Sales](#query-2)
        - [Top 10 Customers by Total Sales](#query-3)
        - [Total Product Category and Subcategory Sales](#query-4)
        - [Total Sales for Each Segment](#query-5)
        - [Top 10 Selling Products](#query-6)
        - [Top 10 Most Profitable Products](#query-7)
        - [Profit Margin Analysis by Product Category](#query-8)
        - [Monthly Profit Trends](#query-9)
        - [Profitability by Location](#query-10)
        - [Ship Mode Performance Over Time](#query-11)
        - [Monthly Revenue Growth Rate](#query-12)

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
 
![SUPERSTORE_STAR_SCHEMA (1)](https://github.com/abanoubsamir0004/dbt_test/assets/153556384/a04515c0-6b97-4736-9000-2f2c1261e296)

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
The `DIM_CUSTOMER` dimension table is designed to capture customer-related details extracted from the SUPERSTORE source. The table includes a unique identifier, `CUSTOMER_KEY`, derived from the original `CUSTOMER_ID` in the source table. Additional information such as `CUSTOMER_NAME` and `SEGMENT` is included for a comprehensive representation of each customer.

**Table Structure:**

| Column         | Description                           |
| -------------- | ------------------------------------- |
| CUSTOMER_KEY   | Unique identifier for each customer.   |
| CUSTOMER_NAME  | Name of the customer.                  |
| SEGMENT        | Customer segmentation information.    |

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
The `DIM_LOCATION` dimension table is designed to capture unique location-related details extracted from the SUPERSTORE source. The table includes a surrogate key, `LOCATION_SK`, assigned through row numbering for a distinct representation of each location. Additional location attributes such as `COUNTRY`, `STATE`, `CITY`, `REGION` and `POSTAL_CODE` are included.

**Table Structure:**

| Column         | Description                              |
| -------------- | ---------------------------------------- |
| LOCATION_SK    | Surrogate key for each unique location.  |
| COUNTRY        | Country of the location.                 |
| STATE          | State of the location.                   |
| CITY           | City of the location.                    |
| REGION         | Region of the location.                  |
| POSTAL_CODE    | Postal code associated with the customer. |

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

# 4.3- ANALYTICS  <a name="analytics"></a>


## 4.3.1 Query 1: Yearly Metrics Calculation <a name="query-1"></a>

### Purpose:
Calculate key yearly metrics to provide an overview of business performance.

### Details:
- Calculates the count of unique customers, products sold, total sales, and total profits on a yearly basis.
- Utilizes a JOIN operation with the date dimension to aggregate sales metrics for each year.

## 4.3.2 Query 2: Monthly Customer Total Sales <a name="query-2"></a>

### Purpose:
Analyze monthly total sales for each customer to understand purchasing patterns over time.

### Details:
- Joins the sales fact table with customer and date dimensions.
- Aggregates monthly sales for each customer, providing insights into individual customer behaviors.

## 4.3.3 Query 3: Top 10 Customers by Total Sales <a name="query-3"></a>

### Purpose:
Identify and rank the top 10 customers based on their total sales contribution.

### Details:
- Computes the total sales for each customer and ranks them in descending order.
- Filters the result to display only the top 10 customers with the highest total sales.

## 4.3.4 Query 4: Total Product Category and Subcategory Sales <a name="query-4"></a>

### Purpose:
Analyze the total sales for each product category and subcategory to understand product performance.

### Details:
- Joins the sales fact table with the product dimension to aggregate sales metrics by category and subcategory.
- Presents the result in descending order of total sales, offering insights into popular product categories.

## 4.3.5 Query 5: Total Sales for Each Segment <a name="query-5"></a>

### Purpose:
Analyze the total sales for each customer segment to understand the contribution of different segments to overall sales.

### Details:
- Joins the sales fact table with the customer dimension to aggregate sales metrics by segment.
- Provides insights into the performance of different customer segments in terms of total sales.

## 4.3.6 Query 6: Top 10 Selling Products <a name="query-6"></a>

### Purpose:
Identify and rank the top 10 selling products based on total quantity sold and total product sales.

### Details:
- Aggregates sales metrics for each product, including total quantity sold and total product sales.
- Ranks the products in descending order of total sales and filters the result to display the top 10.

##  4.3.7 Query 7: Top 10 Most Profitable Products <a name="query-7"></a>

### Purpose:
Identify and rank the top 10 most profitable products based on total profit.

### Details:
- Aggregates total profits for each product.
- Ranks products in descending order of total profit and filters the result to display the top 10.

##  4.3.8 Query 8: Profit Margin Analysis by Product Category <a name="query-8"></a>

### Purpose:
Analyze the average profit margin for each product category.

### Details:
- Computes the average profit margin for each product category.
- Provides insights into the profitability of different product categories.

##  4.3.9 Query 9: Monthly Profit Trends <a name="query-9"></a>

### Purpose:
Analyze the trends in monthly profits over time.

### Details:
- Aggregates monthly profits based on the date dimension.
- Provides a chronological overview of monthly profits.

##  4.3.10 Query 10: Profitability by Location <a name="query-10"></a>

### Purpose:
Analyze profitability based on geographical location.

### Details:
- Aggregates total profits for each location (country, state, city, region, postal code).
- Provides insights into the most profitable locations.

##  4.3.11 Query 11: Ship Mode Performance Over Time <a name="query-11"></a>

### Purpose:
Analyze the performance of different ship modes over time.

### Details:
- Aggregates total sales for each ship mode over different time periods.
- Provides insights into the effectiveness of ship modes over time.

##  4.3.12 Query 12: Monthly Revenue Growth Rate <a name="query-12"></a>

### Purpose:
Calculate the monthly revenue growth rate to understand sales trends.

### Details:
- Computes the monthly growth rate by comparing total sales with the previous month.
- Provides insights into the month-over-month growth of total sales.


# 5- Most Common Data Issues <a name="most-common-data-issues"></a>

1. **`Product ID` Column:**
   - Contains multiple entries with the same `PRODUCT_ID` but different `PRODUCT_NAME`.
