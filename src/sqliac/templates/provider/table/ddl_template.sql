{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

-- use add.<field> for new resource values
-- use change.<field> when altering an existing object
-- use remove.<field> when removing items

{% if ddl_command.upper() == 'CREATE' %}
CREATE TABLE {{ name }} (
{% for column in add.columns %}
    {{ column.name }} {{ column.type }}{% if not column.nullable %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
)
{% if add.comment | default(None) %}
COMMENT = '{{ add.comment }}'
{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
{% for column in add.columns %}
ALTER TABLE {{ name }} ADD COLUMN {{ column.name }} {{ column.type }}{% if not column.nullable %} NOT NULL{% endif %};
{% endfor %}

{% for column in change.columns %}
ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} SET DATA TYPE {{ column.type }};
{% endfor %}

{% for column in remove.columns %}
ALTER TABLE {{ name }} DROP COLUMN {{ column.name }};
{% endfor %}

{% elif ddl_command.upper() == 'DROP' %}
DROP TABLE {{ name }};
{% endif -%}
