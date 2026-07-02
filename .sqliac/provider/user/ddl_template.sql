-- DDL template for Snowflake User resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER USER {{ name }}
  {% if add.get('login_name') or change.get('login_name') %}
  LOGIN_NAME = '{{ add.get('login_name') or change.get('login_name') }}'
  {% endif %}
  {% if add.get('display_name') or change.get('display_name') %}
  DISPLAY_NAME = '{{ add.get('display_name') or change.get('display_name') }}'
  {% endif %}
  {% if add.get('email') or change.get('email') %}
  EMAIL = '{{ add.get('email') or change.get('email') }}'
  {% endif %}
  {% if add.get('default_role') or change.get('default_role') %}
  DEFAULT_ROLE = {{ add.get('default_role') or change.get('default_role') }}
  {% endif %}
  {% if add.get('default_warehouse') or change.get('default_warehouse') %}
  DEFAULT_WAREHOUSE = {{ add.get('default_warehouse') or change.get('default_warehouse') }}
  {% endif %}
  {% if add.get('default_namespace') or change.get('default_namespace') %}
  DEFAULT_NAMESPACE = {{ add.get('default_namespace') or change.get('default_namespace') }}
  {% endif %}
  {% if add.get('disabled') is not none or change.get('disabled') is not none %}
  DISABLED = {{ add.get('disabled', change.get('disabled', false)) }}
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP USER {{ name }};
{% endif -%}
