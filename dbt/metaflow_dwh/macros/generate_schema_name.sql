{# Use the configured schema as-is (STAGING, MARTS...) instead of <target>_<schema>. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim | upper }}
{%- endmacro %}
