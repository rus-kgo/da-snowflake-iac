{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE STORAGE INTEGRATION {{ name }}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = '{{ add.storage_provider | upper }}'
  STORAGE_ALLOWED_LOCATIONS = (
    {%- for loc in add.storage_allowed_locations -%}
    '{{ loc }}'{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  )
  ENABLED = {{ add.enabled | string | upper }}
  {% if add.storage_aws_role_arn %}STORAGE_AWS_ROLE_ARN = '{{ add.storage_aws_role_arn }}'{% endif %}
  {% if add.storage_aws_external_id %}STORAGE_AWS_EXTERNAL_ID = '{{ add.storage_aws_external_id }}'{% endif %}
  {% if add.storage_aws_object_acl %}STORAGE_AWS_OBJECT_ACL = '{{ add.storage_aws_object_acl }}'{% endif %}
  {% if add.use_privatelink_endpoint %}USE_PRIVATELINK_ENDPOINT = {{ add.use_privatelink_endpoint | string | upper }}{% endif %}
  {% if add.comment %}COMMENT = '{{ add.comment }}'{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
ALTER STORAGE INTEGRATION {{ name }} SET
  {% if change.storage_allowed_locations %}
  STORAGE_ALLOWED_LOCATIONS = (
    {%- for loc in change.storage_allowed_locations -%}
    '{{ loc }}'{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  )
  {% endif %}
  {% if change.enabled is not none %}ENABLED = {{ change.enabled | string | upper }}{% endif %}
  {% if change.storage_aws_role_arn %}STORAGE_AWS_ROLE_ARN = '{{ change.storage_aws_role_arn }}'{% endif %}
  {% if change.storage_aws_object_acl %}STORAGE_AWS_OBJECT_ACL = '{{ change.storage_aws_object_acl }}'{% endif %}
  {% if change.comment %}COMMENT = '{{ change.comment }}'{% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP STORAGE INTEGRATION {{ name }};
{% endif -%}
