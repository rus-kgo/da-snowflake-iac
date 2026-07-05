{% set add = add | default({}) %}
{% set change = change | default({}) %}

{# Use .get() to safely check for keys that might not exist #}

{% set transient = 'transient' | pick(add, change) %}
{% set managed_access = 'managed_access' | pick(add, change) %}
{% set data_retention = 'data_retention_time_in_days' | pick(add, change) %}
{% set comment = 'comment' | pick(add, change) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER {% if transient %} TRANSIENT {% endif %} SCHEMA {{ name }}
  {% if managed_access %} WITH MANAGED ACCESS {% endif %}
  {% if data_retention is not none %} DATA_RETENTION_TIME_IN_DAYS = {{ data_retention }} {% endif %}
  {% if comment %} COMMENT = {{ comment | sql_escape}} {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP SCHEMA {{ name }};
{% endif -%}
