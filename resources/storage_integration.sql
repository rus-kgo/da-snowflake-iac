{% if iac_action.upper() == 'CREATE' %}
CREATE STORAGE INTEGRATION {{ name }}
TYPE = {{ type | upper }}
{% if storage_provider %}STORAGE_PROVIDER = '{{ storage_provider | upper }}'{% endif %}
STORAGE_ALLOWED_LOCATIONS = ('{{ storage_allowed_locations | join("', '") }}')
ENABLED = {{ enabled | string | upper }}
{% if storage_aws_role_arn %}STORAGE_AWS_ROLE_ARN = '{{ storage_aws_role_arn }}'{% endif %}
{% if storage_aws_external_id %}STORAGE_AWS_EXTERNAL_ID = '{{ storage_aws_external_id }}'{% endif %}
{% if storage_aws_object_acl %}STORAGE_AWS_OBJECT_ACL = '{{ storage_aws_object_acl }}'{% endif %}
{% if use_privatelink_endpoint %}USE_PRIVATELINK_ENDPOINT = {{ use_privatelink_endpoint | string | upper }} {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} STORAGE INTEGRATION {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER'%}
ALTER STORAGE_INTEGRATION {{ name }} SET
STORAGE_ALLOWED_LOCATIONS = ('{{ storage_allowed_locations | join("', '") }}')
ENABLED = {{ enabled | string | upper }}
{% if storage_aws_role_arn %}STORAGE_AWS_ROLE_ARN = '{{ storage_aws_role_arn }}'{% endif %}
{% if storage_aws_external_id %}STORAGE_AWS_EXTERNAL_ID = '{{ storage_aws_external_id }}'{% endif %}
{% if storage_aws_object_acl %}STORAGE_AWS_OBJECT_ACL = '{{ storage_aws_object_acl }}'{% endif %}
{% if use_privatelink_endpoint %}USE_PRIVATELINK_ENDPOINT = {{ use_privatelink_endpoint | string | upper }} {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';


{% elif iac_action.upper() == 'DROP' %}
DROP STORAGE INTEGRATION {{ name }};

{% endif -%}