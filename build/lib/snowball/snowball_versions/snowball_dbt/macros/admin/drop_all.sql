{%- macro drop_all() -%}
    {%- for node in graph.nodes.values() if node.resource_type == 'model' %}
        {% set relation = adapter.get_relation(
            database=node.database,
            schema=node.schema,
            identifier=node.alias
        ) %}
        {% if relation %}
            {% do log("🧹 Dropping: " ~ relation, info=True) %}
            {% do adapter.drop_relation(relation) %}
        {% endif %}
    {%- endfor %}
{%- endmacro -%}
