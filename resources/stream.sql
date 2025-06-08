{% if iac_action.upper() == 'CREATE' %}
CREATE STREAM {{ name }} ON 
{% if object_type.upper() == 'DIRECTORY TABLE'%} STAGE {% else %} {{ object_type.upper() }} {% endif %} {{ object_name }}
{% if append_only %} APPEND_ONLY = {{ append_only | string | upper }} {% endif %}
{% if insert_only %} INSERT_ONLY = {{ insert_only | string | upper }} {% endif %}
{% if show_initial_rows %} SHOW_INITIAL_ROWS = {{ show_initial_rows | string | upper }} {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and comment %}
{{ iac_action }} STREAM {{ name }} SET
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} STREAM {{ name }};

{% endif -%}