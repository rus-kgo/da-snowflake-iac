{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER DYNAMIC TABLE {{ database }}.{{ schema }}.{{ name }}
  {% if (add.warehouse | default(None)) or (change.warehouse | default(None)) %}
  WAREHOUSE = {{ add.warehouse | default(change.warehouse) }}
  {% endif %}
  TARGET_LAG = '{{ add.target_lag | default(change.target_lag) }}'
  {% if (add.refresh_mode | default(None)) or (change.refresh_mode | default(None)) %}
  REFRESH_MODE = {{ add.refresh_mode | default(change.refresh_mode) }}
  {% endif %}
  {% if (add.initialize | default(None)) or (change.initialize | default(None)) %}
  INITIALIZE = {{ add.initialize | default(change.initialize) }}
  {% endif %}
  {% if (add.data_retention_time_in_days | default(None)) is not none or (change.data_retention_time_in_days | default(None)) is not none %}
  DATA_RETENTION_TIME_IN_DAYS = {{ add.data_retention_time_in_days | default(change.data_retention_time_in_days) }}
  {% endif %}
  {% if (add.max_data_extension_time_in_days | default(None)) is not none or (change.max_data_extension_time_in_days | default(None)) is not none %}
  MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ add.max_data_extension_time_in_days | default(change.max_data_extension_time_in_days) }}
  {% endif %}
  {% if (add.comment | default(None)) or (change.comment | default(None)) %}
  COMMENT = '{{ add.comment | default(change.comment) }}'
  {% endif %}
AS
{{ add.as_ | default(change.as_) }};

{% elif ddl_command.upper() == 'DROP' %}
DROP DYNAMIC TABLE {{ database }}.{{ schema }}.{{ name }};
{% endif -%}
