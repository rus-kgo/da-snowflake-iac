{% if action.upper() == 'ALTER' %}
{{ action }} DATABASE ROLE {% if old_name %} {{ old_name }} {% endif %}
{% if new_name %} RENAME TO {{ new_name }} {% endif %}
SET COMMENT TO '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif action.upper() == 'CREATE' %}
{{ action }} DATABASE ROLE {{ name }} 
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif action.upper() == 'DROP' %}
{{ action }} DATABASE ROLE {{ name }};

{% endif -%}



