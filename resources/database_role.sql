{% if owner %} 
USE ROLE {{owner}}; 
{%- endif -%}
{% if database %} 
USE DATABASE {{ database }}; 
{%- endif -%}
{% if iac_action.upper() == 'ALTER' %}
{{ iac_action }} DATABASE ROLE {% if old_name %} {{ old_name }} {% endif %}
{% if new_name %} RENAME TO {{ new_name }} {% endif %}
SET COMMENT TO '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'CREATE' %}
{{ iac_action }} DATABASE ROLE {{ name }} 
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} DATABASE ROLE {{ name }};

{% endif -%}



