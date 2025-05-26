-- TODO: this still needs work
{% if iac_action.upper() == 'CREATE' %}
{{ iac_action }} STREAM {{ name }} ON {{ object_type }} {{ object_name }}
{% if append_only %} APPEND_ONLY = {{ append_only }} {% endif %}
{% if insert_only %} INSERT_ONLY = {{ insert_only }} {% endif %}
{% if show_initial_rows %} SHOW_INITIAL_ROWS = {{ show_initial_rows }} {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} STREAM {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' and comment %}
{{ iac_action }} STREAM {{ name }} SET
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} STREAM {{ name }};

{% endif -%}