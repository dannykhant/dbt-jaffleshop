{%- macro grant_select(catalog_name=target.catalog, schema_name=target.schema, principal=target.role) %}
    {%- set sql0 %}
        grant use catalog on catalog {{ catalog_name }} to `{{ principal }}`;
    {% endset -%}
    {%- set sql1 %}
        grant use schema, select on schema {{ catalog_name }}.{{ schema_name }} to `{{ principal }}`;
    {% endset -%}
    {{ log("Granting select on " ~ schema_name ~ "to" ~ principal, info=True) }}
        {% do run_query(sql0) %}
        {% do run_query(sql1) %}
    {{ log("Priveleges granted", info=True) }}
{% endmacro -%}