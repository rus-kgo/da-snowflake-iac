-- DDL template for Snowflake Stage resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER STAGE {{ name }}
  {% if add.get('url') or change.get('url') %}
  URL = '{{ add.get('url') or change.get('url') }}'
  {% endif %}
  {% if add.get('storage_integration') or change.get('storage_integration') %}
  STORAGE_INTEGRATION = {{ add.get('storage_integration') or change.get('storage_integration') }}
  {% endif %}
  {% if add.get('encryption_type') or change.get('encryption_type') %}
  ENCRYPTION = (TYPE = '{{ add.get('encryption_type') or change.get('encryption_type') }}')
  {% endif %}
  {% if add.get('directory_enabled') or change.get('directory_enabled') %}
  DIRECTORY = (ENABLE = {{ add.get('directory_enabled') or change.get('directory_enabled') }})
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP STAGE {{ name }};
{% endif -%}
