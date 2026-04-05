{% macro generate_schema_name(custom_schema_name, node) -%}

    {# Nếu model không set schema → dùng mặc định #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}

    {# Nếu có schema (STG, DW, MART) → dùng đúng tên đó #}
    {%- else -%}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}