-- DDL template for Snowflake Notification Integration resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER NOTIFICATION INTEGRATION {{ name }}
  ENABLED = {{ add.get('enabled', true) or change.get('enabled', true) }}
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = {{ add.get('notification_provider') or change.get('notification_provider') }}
  {% if add.get('direction') or change.get('direction') %}
  DIRECTION = {{ add.get('direction') or change.get('direction') }}
  {% endif %}
  {% if add.get('aws_sqs_arn') or change.get('aws_sqs_arn') %}
  AWS_SQS_ARN = '{{ add.get('aws_sqs_arn') or change.get('aws_sqs_arn') }}'
  AWS_SQS_ROLE_ARN = '{{ add.get('aws_sqs_role_arn') or change.get('aws_sqs_role_arn') }}'
  {% endif %}
  {% if add.get('azure_storage_queue_primary_uri') or change.get('azure_storage_queue_primary_uri') %}
  AZURE_STORAGE_QUEUE_PRIMARY_URI = '{{ add.get('azure_storage_queue_primary_uri') or change.get('azure_storage_queue_primary_uri') }}'
  AZURE_TENANT_ID = '{{ add.get('azure_tenant_id') or change.get('azure_tenant_id') }}'
  {% endif %}
  {% if add.get('gcp_pubsub_subscription_name') or change.get('gcp_pubsub_subscription_name') %}
  GCP_PUBSUB_SUBSCRIPTION_NAME = '{{ add.get('gcp_pubsub_subscription_name') or change.get('gcp_pubsub_subscription_name') }}'
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP NOTIFICATION INTEGRATION {{ name }};
{% endif -%}
