{% if ddl_command.upper() == 'CREATE' %}
CREATE
-- use add.<field> for new resource values
{%- if add.unique %} UNIQUE{% endif %} INDEX {{ name }}
ON {{ add.table }} (
{% for column in add.columns %}
    {{ column.name }}
    {%- if not loop.last %},{% endif %}
{% endfor %}
);

{% elif ddl_command.upper() == 'DROP' %}
DROP INDEX {{ name }};

{% endif -%}
