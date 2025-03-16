{% if owner %} 
USE ROLE {{ owner }}; 
{%- endif -%}
{% if database %} 
USE DATABASE {{ database }}; 
{%- endif -%}
{% if schema %} 
USE SCHEMA {{ schema }}; 
{%- endif -%}

{% if iac_action.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER PROCEDURE {{ name }}(
{%if arguments[0].name %}
{% for a in arguments %}
    {{a.name}} {{a.type}}{% if not loop.last %}, {% endif %}
{% endfor %}
{% endif %}
)
RETURNS {{ return_type }}
LANGUAGE {{ language | upper }}
{% if runtime_version %}RUNTIME_VERSION = {{ runtime_version }}{% endif %}
{% if packages %} PACKAGES = (
{% for p in packages %}
    '{{ p }}' {% if not loop.last %}, {% endif %}
{% endfor %}
)
{% endif %}
{% if imports %} IMPORTS = (
{% for i in imports %}
    '{{ i }}' {% if not loop.last %}, {% endif %}
{% endfor %}
)
{% endif %}
{% if handler %}HANDLER = '{{ handler }}'{% endif %}
{% if body %}AS '''{{ body }}'''{% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'DROP' %}
{{ iac_action }} PROCEDURE {{ name }}({{ arguments | map(attribute='type') | join(', ') }});
{% endif -%}