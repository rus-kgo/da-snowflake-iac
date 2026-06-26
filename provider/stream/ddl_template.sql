{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE STREAM {{ database }}.{{ schema }}.{{ name }}
  ON {% if add.object_type | upper == 'DIRECTORY TABLE' %}STAGE{% else %}{{ add.object_type | upper }}{% endif %}
  {{ database }}.{{ schema }}.{{ add.object_name }}
  {% if add.append_only %}APPEND_ONLY = TRUE{% endif %}
  {% if add.insert_only %}INSERT_ONLY = TRUE{% endif %}
  {% if add.show_initial_rows %}SHOW_INITIAL_ROWS = TRUE{% endif %}
  {% if add.comment %}COMMENT = '{{ add.comment }}'{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
{% if change.comment %}
ALTER STREAM {{ database }}.{{ schema }}.{{ name }}
  SET COMMENT = '{{ change.comment }}';
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP STREAM {{ database }}.{{ schema }}.{{ name }};
{% endif -%}
