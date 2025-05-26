{% if iac_action.upper() == 'DROP' %}
{{ iac_action }} VIEW {{ name }};

{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} VIEW {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' and (secure or change_tracking|string|upper !="" or comment) %}
{% if secure %}ALTER VIEW {{ name }} SET SECURE {% else %}ALTER VIEW {{ name }} UNSET SECURE;{% endif %}
{% if change_tracking|string|upper !="" %}ALTER VIEW {{ name }}
SET CHANGE_TRACKING = {{ change_tracking|string|upper }};{% endif %}
{% if comment %}ALTER VIEW {{ name }} SET
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';{% endif %}

{% elif iac_action.upper() == 'CREATE'%}
{{ iac_action }} 
{% if secure %}SECURE {% endif -%}
{% if temporary %}TEMPORARY {% endif -%}
{% if recursive %}RECURSIVE {% endif -%}
VIEW {{ name }}
{% if change_tracking %}CHANGE_TRACKING = TRUE {% endif %}
{% if copy_grants %}COPY_GRANTS {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}'
AS {{ as_ }};
{% endif -%}