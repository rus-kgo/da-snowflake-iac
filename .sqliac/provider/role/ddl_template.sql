-- DDL template for Snowflake Role resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

-- Roles support CREATE OR ALTER since Snowflake 2023+
{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER ROLE {{ name }}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP ROLE {{ name }};
{% endif -%}
