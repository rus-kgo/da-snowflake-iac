-- DDL template for Snowflake View resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER {% if add.get('secure') or change.get('secure') %}SECURE {% endif %}VIEW {{ name }}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %}
  AS
    {{ add.get('query') or change.get('query') }};

{% elif ddl_command.upper() == 'DROP' %}
DROP VIEW {{ name }};
{% endif -%}
