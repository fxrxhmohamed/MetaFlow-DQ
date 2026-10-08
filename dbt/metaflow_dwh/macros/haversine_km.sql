{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
    haversine({{ lat1 }}, {{ lon1 }}, {{ lat2 }}, {{ lon2 }})
{%- endmacro %}
