{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE USER {{ name }}
  {% if add.login_name %}LOGIN_NAME = '{{ add.login_name }}'{% endif %}
  {% if add.display_name %}DISPLAY_NAME = '{{ add.display_name }}'{% endif %}
  {% if add.email %}EMAIL = '{{ add.email }}'{% endif %}
  {% if add.disabled is not none %}DISABLED = {{ add.disabled | string | upper }}{% endif %}
  {% if add.default_warehouse %}DEFAULT_WAREHOUSE = {{ add.default_warehouse }}{% endif %}
  {% if add.default_namespace %}DEFAULT_NAMESPACE = {{ add.default_namespace }}{% endif %}
  {% if add.default_role %}DEFAULT_ROLE = {{ add.default_role }}{% endif %}
  {% if add.must_change_password is not none %}MUST_CHANGE_PASSWORD = {{ add.must_change_password | string | upper }}{% endif %}
  {% if add.comment %}COMMENT = '{{ add.comment }}'{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
ALTER USER {{ name }} SET
  {% if change.display_name %}DISPLAY_NAME = '{{ change.display_name }}'{% endif %}
  {% if change.email %}EMAIL = '{{ change.email }}'{% endif %}
  {% if change.disabled is not none %}DISABLED = {{ change.disabled | string | upper }}{% endif %}
  {% if change.default_warehouse %}DEFAULT_WAREHOUSE = {{ change.default_warehouse }}{% endif %}
  {% if change.default_namespace %}DEFAULT_NAMESPACE = {{ change.default_namespace }}{% endif %}
  {% if change.default_role %}DEFAULT_ROLE = {{ change.default_role }}{% endif %}
  {% if change.must_change_password is not none %}MUST_CHANGE_PASSWORD = {{ change.must_change_password | string | upper }}{% endif %}
  {% if change.comment %}COMMENT = '{{ change.comment }}'{% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP USER {{ name }};
{% endif -%}
