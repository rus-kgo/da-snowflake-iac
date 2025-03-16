{% if owner %} 
USE ROLE {{owner}}; 
{%- endif -%}

{% if iac_action.upper() == 'DROP' %}
{{iac_action}} DATABASE {{name}}; 

{% elif iac_action.upper() == 'CREATE' %}
{{iac_action}} {% if transient %} TRANSIENT {% endif %} DATABASE {{name}} 
{% if data_retention_time_in_days %} DATA_RETENTION_TIME_IN_DAYS = {{data_retention_time_in_days}} {% endif %}
{% if max_data_extension_time_in_days %} MAX_DATA_EXTENSION_TIME_IN_DAYS = {{max_data_extension_time_in_days}} {% endif %}
{% if default_ddl_collation %} DEFAULT_DDL_COLLATION = {{default_ddl_collation}} {% endif %}
COMMENT = '{
        "comment":"{{ comment }}",
        "object_id_tag": "{{ object_id_tag }}",
        "max_data_extension_time_in_days": "{{ max_data_extension_time_in_days }}",
        "default_ddl_collation":"{{ default_ddl_collation }}",
        "log_level":"{{ log_level }}",
        "trace_level":"{{ trace_level }}"
    }';

{% elif iac_action.upper() == 'ALTER' %}
{% if new_name %}
{{iac_action}} DATABASE {{old_name}} RENAME TO {{new_name}} 
{% else %}
{{iac_action}} DATABASE {{name}} SET
{% if data_retention_time_in_days %} DATA_RETENTION_TIME_IN_DAYS = {{data_retention_time_in_days}} {% endif %}
{% if max_data_extension_time_in_days %} MAX_DATA_EXTENSION_TIME_IN_DAYS = {{max_data_extension_time_in_days}} {% endif %}
{% if default_ddl_collation %} DEFAULT_DDL_COLLATION = {{default_ddl_collation}} {% endif %}
{% if log_level %} LOG_LEVEL = {{log_level}} {% endif %}
{% if trace_level %} TRACE_LEVEL = {{trace_level}} {% endif %}
COMMENT = '{
        "comment":"{{ comment }}",
        "object_id_tag": "{{ object_id_tag }}",
        "max_data_extension_time_in_days": "{{ max_data_extension_time_in_days }}",
        "default_ddl_collation":"{{ default_ddl_collation }}",
        "log_level":"{{ log_level }}",
        "trace_level":"{{ trace_level }}",
    }';
{% endif %}
{% endif %}







