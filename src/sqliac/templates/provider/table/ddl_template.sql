{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

{# 1. Setup variables using our new 'pick' filter #}
{% set comment = 'comment' | pick(add, change) %}
{% set add_cols = add.get('columns', []) %}
{% set change_cols = change.get('columns', []) %}
{% set remove_cols = remove.get('columns', []) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE TABLE {{ name }}
{% if add_cols %} (
{% for column in add_cols %}
    {{ column.name }} {{ column.type }}{% if column.get('nullable') == 'NO' %} NOT NULL{% endif %}
    {% if column.get('comment') is not none %}COMMENT {{ column.comment | sql_escape}}{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
){% endif %};

{% elif ddl_command.upper() == 'ALTER' %}

{# Handle Adding Columns #}

{% for column in add_cols %}
ALTER TABLE {{ name }} ADD COLUMN {{ column.name }} {{ column.type }}{% if not column.get('nullable', True) %} NOT NULL{% endif %}
{% if column.get('comment') is not none %}COMMENT {{ column.comment | sql_escape}}{% endif %}{% if not loop.last %},{% endif %}
{% endfor %};

{# Handle Changing Columns #}
{% for column in change_cols %}
{% if column.get('type') %}
ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} SET DATA TYPE {{ column.type }};
{% endif %}

{% if column.get('comment') is not none %}
    ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} COMMENT {{ column.comment | sql_escape}};
{% endif %}

{% if column.get('nullable') == 'YES' %}
ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} DROP NOT NULL;
{% elif column.get('nullable') == 'NO' %}
ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} SET NOT NULL;
{% endif %}

{% if column.get('new_name') %}
ALTER TABLE {{ name }} RENAME COLUMN {{ column.name }} TO {{ column.new_name }};
{% endif %}
{% endfor %}

{# Handle Removing Columns #}
{% for column in remove_cols %}
ALTER TABLE {{ name }} DROP COLUMN {{ column.name }};
{% endfor %}

{# Handle Comment Change #}
{% if change.get('comment') %}
ALTER TABLE {{ name }} SET COMMENT = {{ change.get('comment') | sql_escape}};
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP TABLE {{ name }};
{% endif -%}
