{#
This macro gives sql for sum of fields given list of fields
#}

{% macro sum_of_fields(fields, prefix) -%}
    {% for field in fields %}
    sum({{field}}) as {{prefix}}_{{field}} {% if not loop.last %},{% endif %}
    {% endfor %}
{%- endmacro %}
