{% if ddl_command.upper() == 'CREATE' %}
CREATE TABLE {{ name }} (
-- use add.<field> for new resource values
{% for column in add.columns %}
    {{ column.name }} {{ column.type }}
    {%- if not column.nullable %} NOT NULL{% endif %}
    {%- if not loop.last %},{% endif %}
{% endfor %}
);

{% elif ddl_command.upper() == 'ALTER' %}
-- use change.<field> when altering an existing object
ALTER TABLE {{ name }}
{% for column in change.columns %}
  ALTER COLUMN {{ column.name }} TYPE {{ column.type }}
{% endfor %}
-- use remove.<field> when removing items
{% for column in remove.columns %}
  DROP COLUMN {{ column.name }}
{% endfor %}
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP TABLE {{ name }};

{% endif -%}
