-- DDL template for Snowflake Storage Integration resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER STORAGE INTEGRATION {{ name }}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = '{{ add.get('storage_provider') or change.get('storage_provider') }}'
  ENABLED = {{ add.get('enabled', true) or change.get('enabled', true) }}
  STORAGE_ALLOWED_LOCATIONS = ({{ add.get('storage_allowed_locations') or change.get('storage_allowed_locations') }})
  {% if add.get('storage_blocked_locations') or change.get('storage_blocked_locations') %}
  STORAGE_BLOCKED_LOCATIONS = ({{ add.get('storage_blocked_locations') or change.get('storage_blocked_locations') }})
  {% endif %}
  {% if add.get('storage_aws_role_arn') or change.get('storage_aws_role_arn') %}
  STORAGE_AWS_ROLE_ARN = '{{ add.get('storage_aws_role_arn') or change.get('storage_aws_role_arn') }}'
  {% endif %}
  {% if add.get('azure_tenant_id') or change.get('azure_tenant_id') %}
  AZURE_TENANT_ID = '{{ add.get('azure_tenant_id') or change.get('azure_tenant_id') }}'
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP STORAGE INTEGRATION {{ name }};
{% endif -%}
