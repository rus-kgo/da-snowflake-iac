{% if iac_action.upper() == 'DROP' %}
{{ iac_action }} DYNAMIC TABLE {{ name }};

{% elif iac_action.upper() == 'CREATE' %}
{{ iac_action }} DYNAMIC TABLE {{ name }} (
{% for column in columns %}
    {{ column.name }} {{ column.type }}{% if not loop.last %}, {% endif %}
{% endfor %}
)
WAREHOUSE = {{ warehouse }}
TARGET_LAG = '{{ target_lag }}'
{% if refresh_mode %}REFRESH_MODE = {{ refresh_mode }} {% endif %}
{% if initialize %}INITIALIZE = {{ initialize }} {% endif %}
{% if data_retention_time_in_days %}DATA_RETENTION_TIME_IN_DAYS = {{ data_retention_time_in_days }} {% endif %}
{% if max_data_extension_time_in_days %}MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ max_data_extension_time_in_days }} {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}'
AS {{ query }};

{% if not suspended %}
ALTER DYNAMIC TABLE {{ name }} RESUME;
{% endif %}

{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} DYNAMIC TABLE {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' %}

{% if suspended %}
{{ iac_action }} DYNAMIC TABLE {{ name }} SUSPEND;
{% endif %}

{% if not suspended %}
ALTER DYNAMIC TABLE {{ name }} RESUME;
{% endif %}

{% if target_lag or warehouse or data_retention_time_in_days or max_data_extension_time_in_days %}
ALERT DYNAMIC TABLE {{ name }} SET
{% if warehouse %}WAREHOUSE = {{ warehouse }}{% endif %}
{% if target_lag %}TARGET_LAG = '{{ target_lag }}'{% endif %}
{% if data_retention_time_in_days %}DATA_RETENTION_TIME_IN_DAYS = {{ data_retention_time_in_days }} {% endif %}
{% if max_data_extension_time_in_days %}MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ max_data_extension_time_in_days }} {% endif %}
{% endif %}

{% endif -%}