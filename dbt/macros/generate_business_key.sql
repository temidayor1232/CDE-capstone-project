{% macro generate_business_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}
