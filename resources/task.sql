{% if iac_action.upper() == 'CREATE' %}
{{ iac_action }} TASK {{ name }}
{% if schedule %} SCHEDULE = '{{ schedule }}' {% endif %}
{% if session_parameters %}
SESSION_PARAMETERS = (
{% for key, value in session_parameters.items() -%}
  {{ key }} = '{{ value }}'
  {% if not loop.last %},{% endif %}
{% endfor -%}
) {% endif %}
{% if user_task_timeout_ms %} USER_TASK_TIMEOUT_MS = {{ user_task_timeout_ms }} {% endif %}
{% if error_integration %} ERROR_INTEGRATION = '{{ error_integration }}' {% endif %}
AS {{ definition }}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} TASK {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' and schedule %}
{{ iac_action }} TASK {{ name }} SET SCHEDULE = '{{ schedule }}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} TASK {{ name }};

{% endif -%}