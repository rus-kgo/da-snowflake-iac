{% if iac_action.upper() == 'CREATE' %}
CREATE NOTIFICATION INTEGRATION {{ name }}
TYPE = EMAIL
{% if enabled %} ENABLED = {{ enabled | string | upper }} {% endif %}
{% if allowed_recipients %} ALLOWED_RECIPIENTS = (
{% for recipient in allowed_recipients %}
    '{{ recipient }}' {% if not loop.last %}, {% endif %}
{% endfor %}
)
{% endif %}
{% if default_recipients %} DEFAULT_RECIPIENTS = (
{% for d_recipient in default_recipients %}
    '{{ d_recipient }}' {% if not loop.last %}, {% endif %}
{% endfor %}
)
{% endif %}
{% if default_subject %} DEFAULT_SUBJECT = '{{ default_subject }}'{% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and new_name %}
ALTER NOTIFICATION INTEGRATION {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' %}
ALTER NOTIFICATION INTEGRATION {{ name }} SET
{% if enabled %} ENABLED = {{ enabled | string | upper }} {% endif %}
{% if allowed_recipients %} ALLOWED_RECIPIENTS = (
{% for recipient in allowed_recipients %}
    '{{ recipient }}' {% if not loop.last %}, {% endif %}
{% endfor %}
)
{% endif %}
{% if default_recipients %} DEFAULT_RECIPIENTS = (
{% for d_recipient in default_recipients %}
    '{{ d_recipient }}' {% if not loop.last %}, {% endif %}
{% endfor %}
)
{% endif %}
{% if default_subject %} DEFAULT_SUBJECT = '{{ default_subject }}'{% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} NOTIFICATION INTEGRATION {{ name }};

{% endif -%}