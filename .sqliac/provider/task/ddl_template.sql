-- DDL template for Snowflake Task resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER TASK {{ name }}
  {% if add.get('warehouse') or change.get('warehouse') %}
  WAREHOUSE = {{ add.get('warehouse') or change.get('warehouse') }}
  {% endif %}
  {% if add.get('schedule') or change.get('schedule') %}
  SCHEDULE = '{{ add.get('schedule') or change.get('schedule') }}'
  {% endif %}
  {% if add.get('after') or change.get('after') %}
  AFTER {{ add.get('after') or change.get('after') }}
  {% endif %}
  {% if add.get('when') or change.get('when') %}
  WHEN {{ add.get('when') or change.get('when') }}
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %}
AS
  {{ add.get('sql_statement') or change.get('sql_statement') }};

{% elif ddl_command.upper() == 'DROP' %}
DROP TASK {{ name }};
{% endif -%}
