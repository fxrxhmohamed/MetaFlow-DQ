{# Try every known trip timestamp format; NULL if none match (caught by DQ tests). #}
{% macro parse_trip_ts(column) -%}
    coalesce(
    {%- for fmt in var('trip_timestamp_formats') %}
        try_to_timestamp_ntz({{ column }}, '{{ fmt }}'){{ "," if not loop.last }}
    {%- endfor %}
    )
{%- endmacro %}
