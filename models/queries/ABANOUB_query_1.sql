{#Calculate the average sale price per borough.#}

WITH 

    FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    AVG_SALE_PRICE_PER_BOROUGH AS (
        SELECT
            L.BOROUGH,
            AVG(F.SALE_PRICE) AS AVERAGE_SALE_PRICE
        FROM
            FACT_SALES F
        INNER JOIN
            STG_ABANOUB_DIM_LOCATION L ON F.LOCATION_ID = L.LOCATION_ID
        GROUP BY
            L.BOROUGH
        ORDER BY BOROUGH ASC

    )

SELECT *
FROM AVG_SALE_PRICE_PER_BOROUGH 
