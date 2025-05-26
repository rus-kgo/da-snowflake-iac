{% if iac_action.upper() == 'DROP' %}
{{ iac_action }} WAREHOUSE {{ name }};
{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} WAREHOUSE {{ old_name }} RENAME TO {{ new_name }};
{% else %}
{{ iac_action }} WAREHOUSE {{ name }}
{% if iac_action.upper() == 'ALTER' -%} 
SET 
{% endif -%}
{% if warehouse_type %} WAREHOUSE_TYPE = '{{ warehouse_type }}' {% endif %}
{% if warehouse_size %} WAREHOUSE_SIZE = {{ warehouse_size }} {% endif %}
{% if max_cluster_count %} MAX_CLUSTER_COUNT = {{ max_cluster_count }} {% endif %}
{% if min_cluster_count %} MIN_CLUSTER_COUNT = {{ min_cluster_count }} {% endif %}
{% if scaling_policy %} SCALING_POLICY = {{ scaling_policy }} {% endif %}
{% if auto_suspend %} AUTO_SUSPEND = {{ auto_suspend }} {% endif %}
{% if auto_resume %} AUTO_RESUME = {{ auto_resume }} {% endif %}
{% if initially_suspended %} INITIALLY_SUSPENDED = {{ initially_suspended }} {% endif %}
{% if resource_monitor %} RESOURCE_MONITOR = {{ RESOURCE_MONITOR }} {% endif %}
{% if enable_query_acceleration %} ENABLE_QUERY_ACCELERATION = {{ enable_query_acceleration }} {% endif %}
{% if query_acceleration_max_scale_factor %} QUERY_ACCELERATION_MAX_SCALE_FACTOR = {{ query_acceleration_max_scale_factor }} {% endif %}
{% if max_concurrency_level %} MAX_CONCURRENCY_LEVEL = {{ max_concurrency_level }} {% endif %}
{% if statement_queued_timeout_in_seconds %} STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = {{ statement_queued_timeout_in_seconds }} {% endif %}
{% if statement_timeout_in_seconds %} STATEMENT_TIMEOUT_IN_SECONDS = {{ statement_timeout_in_seconds }} {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';
{% endif %}