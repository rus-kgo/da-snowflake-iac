{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER STAGE {{ database }}.{{ schema }}.{{ name }}
  {% if (add.url | default(None)) or (change.url | default(None)) %}
  URL = '{{ add.url | default(change.url) }}'
  {% endif %}
  {% if (add.storage_integration | default(None)) or (change.storage_integration | default(None)) %}
  STORAGE_INTEGRATION = {{ add.storage_integration | default(change.storage_integration) }}
  {% endif %}
  {% if (add.file_format | default(None)) or (change.file_format | default(None)) %}
  FILE_FORMAT = {{ add.file_format | default(change.file_format) }}
  {% endif %}
  {% if (add.comment | default(None)) or (change.comment | default(None)) %}
  COMMENT = '{{ add.comment | default(change.comment) }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP STAGE {{ database }}.{{ schema }}.{{ name }};
{% endif -%}
