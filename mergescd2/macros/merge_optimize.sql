{% macro generic_coalesce2(schema_name, table_a, table_b, primary_key, date_column, table_a_columns, table_b_columns) %}
{% set table_a_columns = adapter.get_columns_in_table(schema_name, table_a) %}
{% set table_b_columns = adapter.get_columns_in_table(schema_name, table_b) %}

{% set table_a_column_names = table_a_columns | map(attribute='name') %}
{% set table_b_column_names = table_b_columns | map(attribute='name') %}

WITH AllKeysAndDates AS (
  SELECT {{ primary_key }} AS KeyValue, {{ date_column }} AS EffectiveDate
  FROM {{ schema_name }}.{{ table_a }}
  UNION ALL
  SELECT {{ primary_key }} AS KeyValue, {{ date_column }} AS EffectiveDate
  FROM {{ schema_name }}.{{ table_b }}
),
LaggedData AS (
  SELECT
    a.KeyValue,
    a.EffectiveDate,
    {% for column in table_a_columns %}
      c.{{ column.column }} AS {{ column.column }}_A{{ "," }}
    {% endfor %}
    {% for column in table_b_columns %}
      e.{{ column.column }} AS {{ column.column }}_B{{ "," }}
    {% endfor %}
    LAG(OBJECT_CONSTRUCT(c.*)) IGNORE NULLS OVER (PARTITION BY a.KeyValue ORDER BY a.EffectiveDate) AS LaggedRow_A,
    LAG(OBJECT_CONSTRUCT(e.*)) IGNORE NULLS OVER (PARTITION BY a.KeyValue ORDER BY a.EffectiveDate) AS LaggedRow_B
  FROM AllKeysAndDates a
  LEFT JOIN {{ schema_name }}.{{ table_a }} c ON a.KeyValue = c.{{ primary_key }} AND a.EffectiveDate = c.{{ date_column }}
  LEFT JOIN {{ schema_name }}.{{ table_b }} e ON a.KeyValue = e.{{ primary_key }} AND a.EffectiveDate = e.{{ date_column }}
)
SELECT
  ld.KeyValue,
  ld.EffectiveDate,
  {% for column in table_a_columns %}
    COALESCE(ld.{{ column.column }}_A, ld.LaggedRow_A['{{ column.column }}']) AS {{ column.column }}_A{{ "," }}
  {% endfor %}
  {% for column in table_b_columns %}
    COALESCE(ld.{{ column.column }}_B, ld.LaggedRow_B['{{ column.column }}']) AS {{ column.column }}_B{{ "," if not loop.last }}
  {% endfor %}
FROM LaggedData ld
{% endmacro %}
