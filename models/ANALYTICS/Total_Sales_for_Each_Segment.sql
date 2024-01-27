-- Total Sales for Each Segment

WITH 

    DIM_CUSTOMER AS (
        SELECT * FROM {{ ref('DIM_CUSTOMER') }}
    ),

    FACT_SALES AS (
        SELECT * FROM {{ ref('FACT_SALES') }}
    ),

    SEGMENT_SALES AS (
        SELECT
            DIM_CUSTOMER.SEGMENT,
            SUM(FACT_SALES.SALES) AS TOTAL_SALES
        FROM FACT_SALES 
        LEFT JOIN DIM_CUSTOMER 
            ON FACT_SALES.CUSTOMER_KEY = DIM_CUSTOMER.CUSTOMER_ID
        GROUP BY DIM_CUSTOMER.SEGMENT
    )

SELECT *
FROM SEGMENT_SALES
