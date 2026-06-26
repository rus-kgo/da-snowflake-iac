{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE EVENT TABLE {{ database }}.{{ schema }}.{{ name }}
  {% if add.data_retention_time_in_days is not none %}DATA_RETENTION_TIME_IN_DAYS = {{ add.data_retention_time_in_days }}{% endif %}
  {% if add.max_data_extension_time_in_days is not none %}MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ add.max_data_extension_time_in_days }}{% endif %}
  {% if add.change_tracking %}CHANGE_TRACKING = TRUE{% endif %}
  {% if add.default_ddl_collation %}DEFAULT_DDL_COLLATION = '{{ add.default_ddl_collation }}'{% endif %}
  {% if add.comment %}COMMENT = '{{ add.comment }}'{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
ALTER EVENT TABLE {{ database }}.{{ schema }}.{{ name }} SET
  {% if change.data_retention_time_in_days is not none %}DATA_RETENTION_TIME_IN_DAYS = {{ change.data_retention_time_in_days }}{% endif %}
  {% if change.max_data_extension_time_in_days is not none %}MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ change.max_data_extension_time_in_days }}{% endif %}
  {% if change.change_tracking is not none %}CHANGE_TRACKING = {{ change.change_tracking | string | upper }}{% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP EVENT TABLE {{ database }}.{{ schema }}.{{ name }};
{% endif -%}
