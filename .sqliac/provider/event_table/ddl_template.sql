-- DDL template for Snowflake Event Table resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

{% set data_retention = 'data_retention_time_in_days' | pick(add, change) %}
{% set change_tracking = 'change_tracking' | pick(add, change) %}
{% set comment = 'comment' | pick(add, change) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER EVENT TABLE {{ name }}
  {% if data_retention is not none %}
  DATA_RETENTION_TIME_IN_DAYS = {{ data_retention }}
  {% endif %}
  {% if change_tracking is not none %}
  CHANGE_TRACKING = {{ change_tracking }}
  {% endif %}
  {% if comment is not none %}
  COMMENT = '{{ comment }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP EVENT TABLE {{ name }};
{% endif -%}
