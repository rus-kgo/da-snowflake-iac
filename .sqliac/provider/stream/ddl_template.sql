-- DDL template for Snowflake Stream resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER STREAM {{ name }}
  ON {{ add.get('source_type', 'TABLE') or change.get('source_type', 'TABLE') }} {{ add.get('source_name') or change.get('source_name') }}
  {% if add.get('append_only') or change.get('append_only') %}
  APPEND_ONLY = {{ add.get('append_only') or change.get('append_only') }}
  {% endif %}
  {% if add.get('show_initial_rows') or change.get('show_initial_rows') %}
  SHOW_INITIAL_ROWS = {{ add.get('show_initial_rows') or change.get('show_initial_rows') }}
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP STREAM {{ name }};
{% endif -%}
