{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

-- use add.<field> for new resource values
-- use change.<field> when altering an existing object
-- use remove.<field> when removing items

{% if ddl_command.upper() == 'CREATE' %}
CREATE TABLE {{add.database}}.{{add.schema}}.{{ add.name }}
{% if add.columns | default(None) %} (
{% for column in add.columns %}
    {{ column.name }} {{ column.type }}{% if not column.nullable %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
)
{% endif %}
{% if add.comment | default(None) %}
COMMENT = '{{ add.comment }}'
{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
{% if add.columns | default(None) %}
{% for column in add.columns %}
ALTER TABLE {{add.database}}.{{add.schema}}.{{ add.name }} ADD COLUMN {{ column.name }} {{ column.type }}{% if not column.nullable %} NOT NULL{% endif %};
{% endfor %}

{% for column in change.columns %}
ALTER TABLE {{add.database}}.{{add.schema}}.{{ add.name }} ALTER COLUMN {{ column.name }} SET DATA TYPE {{ column.type }};
{% endfor %}

{% for column in remove.columns %}
ALTER TABLE {{remove.database}}.{{remove.schema}}.{{ remove.name }} DROP COLUMN {{ column.name }};
{% endfor %}
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP TABLE {{ name }};
{% endif -%}
