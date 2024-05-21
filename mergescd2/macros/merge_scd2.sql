{% macro generic_coalesce(schema_name, table_a, table_b, primary_key, date_column) %}
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
)
SELECT 
  a.KeyValue, 
  a.EffectiveDate,
  {%- for column in table_a_column_names %}
    COALESCE(
      c.{{ column }},
      LEAD(c.{{ column }}) IGNORE NULLS OVER (PARTITION BY a.KeyValue ORDER BY a.EffectiveDate DESC)
    ) AS {{ column }}_A{{ "," }}
  {% endfor %}
  {%- for column in table_b_column_names %}
    COALESCE(
      e.{{ column }},
      LEAD(e.{{ column }}) IGNORE NULLS OVER (PARTITION BY a.KeyValue ORDER BY a.EffectiveDate DESC)
    ) AS {{ column }}_B{{ "," if not loop.last }}
  {% endfor %}
FROM AllKeysAndDates a
LEFT JOIN {{ schema_name }}.{{ table_a }} c ON a.KeyValue = c.{{ primary_key }} AND a.EffectiveDate = c.{{ date_column }}
LEFT JOIN {{ schema_name }}.{{ table_b }} e ON a.KeyValue = e.{{ primary_key }} AND a.EffectiveDate = e.{{ date_column }}
{% endmacro %}
