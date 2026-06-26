{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE ALERT {{ database }}.{{ schema }}.{{ name }}
  WAREHOUSE = {{ add.warehouse }}
  {% if add.schedule %}SCHEDULE = '{{ add.schedule }}'{% endif %}
  IF (EXISTS({{ add.condition }}))
  THEN {{ add.action }}
  {% if add.comment %}COMMENT = '{{ add.comment }}'{% endif %};

{% if not add.suspended %}
ALTER ALERT {{ database }}.{{ schema }}.{{ name }} RESUME;
{% endif %}

{% elif ddl_command.upper() == 'ALTER' %}
{% if change.suspended is defined %}
ALTER ALERT {{ database }}.{{ schema }}.{{ name }} {{ 'SUSPEND' if change.suspended else 'RESUME' }};
{% endif %}
{% if change.schedule %}
ALTER ALERT {{ database }}.{{ schema }}.{{ name }}
  SET SCHEDULE = '{{ change.schedule }}';
{% endif %}
{% if change.condition %}
ALTER ALERT {{ database }}.{{ schema }}.{{ name }}
  MODIFY CONDITION EXISTS ({{ change.condition }});
{% endif %}
{% if change.action %}
ALTER ALERT {{ database }}.{{ schema }}.{{ name }}
  MODIFY ACTION {{ change.action }};
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP ALERT {{ database }}.{{ schema }}.{{ name }};
{% endif -%}
