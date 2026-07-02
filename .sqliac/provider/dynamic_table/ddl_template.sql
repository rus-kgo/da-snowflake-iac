-- DDL template for Snowflake Dynamic Table resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE OR REPLACE DYNAMIC TABLE {{ name }}
  TARGET_LAG = '{{ add.get('target_lag', '1 hour') }}'
  WAREHOUSE = {{ add.get('warehouse') }}
  {% if add.get('comment') %}COMMENT = '{{ add.get('comment') }}'{% endif %}
  AS
    {{ add.get('query') }};

{% elif ddl_command.upper() == 'ALTER' %}
{% if change.get('target_lag') %}
ALTER DYNAMIC TABLE {{ name }} SET TARGET_LAG = '{{ change.get('target_lag') }}';
{% endif %}
{% if change.get('warehouse') %}
ALTER DYNAMIC TABLE {{ name }} SET WAREHOUSE = {{ change.get('warehouse') }};
{% endif %}
{% if change.get('comment') %}
ALTER DYNAMIC TABLE {{ name }} SET COMMENT = '{{ change.get('comment') }}';
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP DYNAMIC TABLE {{ name }};
{% endif -%}
