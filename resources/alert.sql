{% if iac_action.upper() == 'CREATE' %}
{{ iac_action }} ALERT {{ name }}
{% if schedule %} SCHEDULE = '{{ schedule }}' {% endif %}
IF (EXISTS({{ condition }}))
THEN {{ iac_action }}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% if not suspended %}
ALTER ALERT {{ name }} RESUME;
{% endif %}

{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} ALERT {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' %}
{% if schedule %}
{{ iac_action }} ALERT {{ name }} SET SCHEDULE = '{{ schedule }}';
{% endif %}

{% if suspended %}
{{ iac_action }} ALERT {{ name }} SUSPEND;
{% endif %}

{% if not suspended %}
{{ iac_action }} ALERT {{ name }} RESUME;
{% endif %}

{% if condition %}
{{ iac_action }} ALERT {{ name }} MODIFY CONDITION EXISTS ({{ condition }});
{% endif %}

{% if action %}
{{ iac_action }} ALERT {{ name }} MODIFY ACTION ({{ action }});
{% endif %}


{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} ALERT {{ name }};

{% endif -%}