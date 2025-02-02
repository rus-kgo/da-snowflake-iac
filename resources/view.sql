{% if owner %} 
USE ROLE {{owner}}; 
{%- endif -%}
{% if database %} 
USE DATABASE {{ database }}; 
{%- endif -%}
{% if schema %} 
USE SCHEMA {{ schema }}; 
{%- endif %}

{% if action.upper() == 'DROP' %}
{{ action }} VIEW {{ name }};

{% elif action.upper() == 'ALTER' and new_name %}
{{ action }} VIEW {{ old_name }} RENAME TO {{ new_name }};

{% elif action.upper() == 'ALTER' and (secure or change_tracking|string|upper !="" or comment) %}
{% if secure %}ALTER VIEW {{ name }} SET SECURE {% else %}ALTER VIEW {{ name }} UNSET SECURE;{% endif %}
{% if change_tracking|string|upper !="" %}ALTER VIEW {{ name }}
SET CHANGE_TRACKING = {{ change_tracking|string|upper }};{% endif %}
{% if comment %}ALTER VIEW {{ name }} SET
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';{% endif %}

{% elif action.upper() == 'CREATE'%}
{{ action }} 
{% if secure %}SECURE {% endif -%}
{% if temporary %}TEMPORARY {% endif -%}
{% if recursive %}RECURSIVE {% endif -%}
VIEW {{ name }}
{% if change_tracking %}CHANGE_TRACKING = TRUE {% endif %}
{% if copy_grants %}COPY_GRANTS {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}'
AS {{ as_ }};
{% endif -%}