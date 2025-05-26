{% if iac_action.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER{% if transient %} TRANSIENT{% endif %} SCHEMA {{ name }}
{% if managed_access %}WITH MANAGED ACCESS {% endif %}
{% if data_retention_time_in_days %}DATA_RETENTION_TIME_IN_DAYS = {{ data_retention_time_in_days }} {% endif %}
{% if max_data_extension_time_in_days %}MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ max_data_extension_time_in_days }} {% endif %}
{% if replace_invalid_characters %}REPLACE_INVALID_CHARACTERS = {{ replace_invalid_characters|string|upper }} {% endif %}
{% if default_ddl_collation %}DEFAULT_DDL_COLLATION = '{{ default_ddl_collation }}' {% endif %}
{% if log_level %}LOG_LEVEL = '{{ log_level }}' {% endif %}
{% if trace_level %}TRACE_LEVEL = '{{ trace_level }}' {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} SCHEMA {{ name }};

{% endif -%}