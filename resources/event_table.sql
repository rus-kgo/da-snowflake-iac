{% if owner %} 
USE ROLE {{ owner }}; 
{%- endif -%}
{% if database %} 
USE DATABASE {{ database }}; 
{%- endif -%}
{% if schema %} 
USE SCHEMA {{ schema }}; 
{%- endif -%}

{% if iac_action.upper() == 'DROP' %}
DROP EVENT TABLE {{ name }};

{% elif iac_action.upper() == 'CREATE' %}
CREATE EVENT TABLE {{ name }}
{% if data_retention_time_in_days %} DATA_RETENTION_TIME_IN_DAYS = {{ data_retention_time_in_days }}{% endif %}
{% if max_data_extension_time_in_days %} MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ max_data_extension_time_in_days }}{% endif %}
{% if change_tracking|string|upper !="" %}CHANGE_TRACKING = {{ change_tracking|string|upper }}{% endif %}
{% if default_ddl_collation %} DEFAULT_DDL_COLLATION = '{{ default_ddl_collation }}'{% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and new_name %}
ALTER EVENT TABLE {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' %}
ALTER EVENT TABLE SET
{% if data_retention_time_in_days %} DATA_RETENTION_TIME_IN_DAYS = {{ data_retention_time_in_days }}{% endif %}
{% if max_data_extension_time_in_days %} MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ max_data_extension_time_in_days }}{% endif %}
{% if change_tracking|string|upper !="" %}CHANGE_TRACKING = {{ change_tracking|string|upper }}{% endif %}

{% endif -%}