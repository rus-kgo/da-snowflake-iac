{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER TASK {{ database }}.{{ schema }}.{{ name }}
  {% if (add.warehouse | default(None)) or (change.warehouse | default(None)) %}
  WAREHOUSE = {{ add.warehouse | default(change.warehouse) }}
  {% endif %}
  {% if (add.schedule | default(None)) or (change.schedule | default(None)) %}
  SCHEDULE = '{{ add.schedule | default(change.schedule) }}'
  {% endif %}
  {% if (add.user_task_timeout_ms | default(None)) is not none or (change.user_task_timeout_ms | default(None)) is not none %}
  USER_TASK_TIMEOUT_MS = {{ add.user_task_timeout_ms | default(change.user_task_timeout_ms) }}
  {% endif %}
  {% set sp = add.session_parameters | default(change.session_parameters | default({})) %}
  {% if sp %}
  SESSION_PARAMETERS = (
    {%- for key, value in sp.items() -%}
    {{ key }} = '{{ value }}'{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  )
  {% endif %}
  {% if (add.suspended | default(false)) or (change.suspended | default(false)) %}
  SUSPEND_TASK_AFTER_NUM_FAILURES = 0
  {% endif %}
  {% if (add.comment | default(None)) or (change.comment | default(None)) %}
  COMMENT = '{{ add.comment | default(change.comment) }}'
  {% endif %}
AS
{{ add.definition | default(change.definition) }};

{% elif ddl_command.upper() == 'DROP' %}
DROP TASK {{ database }}.{{ schema }}.{{ name }};
{% endif -%}
