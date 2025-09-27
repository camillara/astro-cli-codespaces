{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}            {# sem custom: usa o do profile #}
  {%- else -%}
    {{ custom_schema_name | trim }} {# com custom: usa exatamente o que você passou #}
  {%- endif -%}
{%- endmacro %}
