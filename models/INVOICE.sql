{{
    config(
        materialized='incremental',
        unique_key='ROW_ID',

    )
}}

with 

    invoices as (
        select * from {{ source('SAMPLE_SUPERSTORE', 'INCREMENTAL_SUPERSTORE') }}

        {% if is_incremental() %}

        where ORDER_DATE > (select max(ORDER_DATE) from {{ this }})

        {% endif %}

    ),

    final as (
        select 
            ROW_ID,  
            ORDER_ID, 
            ORDER_DATE,
            SHIP_DATE, 
            CUSTOMER_ID,
            PRODUCT_ID, 
            '{{ invocation_id }}' as batch_id
        from invoices
    )

select * from final
order by ROW_ID

