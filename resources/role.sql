{% if action == 'ALTER' %}
{{ action }} ROLE {% if old_name %} {{ old_name }} {% endif %}
{% if new_name %} RENAME TO {{ new_name }} {% endif %}
SET COMMENT TO '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif action == 'CREATE' %}
{{ action }} ROLE {{ name }} 
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif action == 'DROP' %}
{{ action }} ROLE {{ name }};

{% endif -%}



