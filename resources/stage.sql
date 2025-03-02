{% if owner %} 
USE ROLE {{ owner }}; 
{%- endif -%}
{% if database %} 
USE DATABASE {{ database }}; 
{%- endif -%}
{% if schema %} 
USE SCHEMA {{ schema }}; 
{%- endif -%}

{% if iac_action.upper() == 'CREATE' %}
CREATE STAGE {{ name }}
{% if url %} URL = '{{ url }}' {% endif %}
{% if file_format %} FILE_FORMAT = {{ file_format }} {% endif %}
{% if storage_integration %} STORAGE_INTEGRATION = {{ storage_integration }} {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' and new_name %}
ALTER STAGE {{ old_name }} RENAME TO {{ new_name }};

{% elif iac_action.upper() == 'ALTER' %}
ALTER STAGE {{ name }} SET
{% if url %} URL = '{{ url }}' {% endif %}
{% if file_format %} FILE_FORMAT = {{ file_format }} {% endif %}
{% if storage_integration %} STORAGE_INTEGRATION = {{ storage_integration }} {% endif %};


{% elif iac_action.upper() == 'DROP' %}
DROP STAGE {{ name }};

{% endif -%}