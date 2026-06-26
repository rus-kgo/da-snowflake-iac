{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE NOTIFICATION INTEGRATION {{ name }}
  TYPE = EMAIL
  ENABLED = {{ add.enabled | string | upper }}
  {% if add.allowed_recipients %}
  ALLOWED_RECIPIENTS = ({{ add.allowed_recipients | map("tojson") | join(", ") }})
  {% endif %}
  {% if add.default_recipients %}
  DEFAULT_RECIPIENTS = ({{ add.default_recipients | map("tojson") | join(", ") }})
  {% endif %}
  {% if add.default_subject %}DEFAULT_SUBJECT = '{{ add.default_subject }}'{% endif %}
  {% if add.comment %}COMMENT = '{{ add.comment }}'{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
ALTER NOTIFICATION INTEGRATION {{ name }} SET
  {% if change.enabled is not none %}ENABLED = {{ change.enabled | string | upper }}{% endif %}
  {% if change.allowed_recipients %}
  ALLOWED_RECIPIENTS = ({{ change.allowed_recipients | map("tojson") | join(", ") }})
  {% endif %}
  {% if change.default_recipients %}
  DEFAULT_RECIPIENTS = ({{ change.default_recipients | map("tojson") | join(", ") }})
  {% endif %}
  {% if change.default_subject %}DEFAULT_SUBJECT = '{{ change.default_subject }}'{% endif %}
  {% if change.comment %}COMMENT = '{{ change.comment }}'{% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP NOTIFICATION INTEGRATION {{ name }};
{% endif -%}
