{% macro clean_stale_models(catalog_name=target.catalog, schema_name=target.schema, days=7, dry_run=True) %}
    {% set get_drop_queries %}
        SELECT  
            case when table_type = 'VIEW' then table_type
                else 'TABLE' end as drop_type,
            'drop ' || drop_type || ' if exists ' || table_catalog || '.' || table_schema || '.' || table_name || ';' as query
        FROM system.information_schema.tables
        WHERE  datediff(now(), last_altered) > {{ days }} and 
        table_catalog = '{{ catalog_name }}' and 
        table_schema = '{{ schema_name }}';
    {% endset %}

    {{ log("Generating cleanup query", info=True) }}
    {% set drop_queries = run_query(get_drop_queries).columns[1].values() %}

    {% for drop_query in drop_queries %}
        {% if execute and not dry_run %}
            {{ log("Dropping table/view with: " ~ drop_query) }}
            {% do run_query(drop_query) %}
        {% else %}
            {{ log(drop_query) }}
        {% endif %}
    {% endfor %}
{% endmacro %}