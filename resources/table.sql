{% if iac_action.upper() == 'CREATE' %}
{{ iac_action }} TABLE {{ name }} (
{% for column in columns %}
    {{ column.name }} {{ column.type }}
    {%- if not column.nullable %} NOT NULL {%- endif %}
    {%- if column.default is not none %} DEFAULT {{ column.default }} {%- endif %}
    {%- if column.comment %} COMMENT '{{ column.comment }}' {%- endif %}
    {%- if not loop.last %}, {% endif %}
{% endfor %}
)
{% if cluster_by %} CLUSTER BY ({{ cluster_by | join(', ') }}) {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and new_name %}
{{ iac_action }} TABLE {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' and comment %}
{{ iac_action }} TABLE {{ name }} SET
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} TABLE {{ name }};

{% endif -%}