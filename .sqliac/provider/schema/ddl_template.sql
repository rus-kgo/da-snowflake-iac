{% set add = add | default({}) %}
{% set change = change | default({}) %}

{# Use .get() to safely check for keys that might not exist #}
{% set transient = add.get('transient') or change.get('transient') %}
{% set managed_access = add.get('managed_access') or change.get('managed_access') %}
{% set data_retention = add.get('data_retention_time_in_days') or change.get('data_retention_time_in_days') %}
{% set comment = add.get('comment') or change.get('comment') %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER {% if transient %} TRANSIENT {% endif %} SCHEMA {{ name }}
  {% if managed_access %} WITH MANAGED ACCESS {% endif %}
  {% if data_retention %} DATA_RETENTION_TIME_IN_DAYS = {{ data_retention }} {% endif %}
  {% if comment %} COMMENT = '{{ comment }}' {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP SCHEMA {{ name }};
{% endif -%}
