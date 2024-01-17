{% macro column_cast_transform(column_name, pk_column, table_name) %}
    SELECT 
        {{ pk_column }},
        CASE
            WHEN TRIM({{ column_name }}) LIKE '-'
                THEN NULL
            ELSE CAST(REPLACE({{ column_name }}, ',', '') AS INT)
        END AS {{ column_name }}
    FROM {{ table_name }}
{% endmacro %}