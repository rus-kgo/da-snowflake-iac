-- DDL template for Snowflake Alert resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER ALERT {{ name }}
  WAREHOUSE = {{ add.get('warehouse') or change.get('warehouse') }}
  SCHEDULE = '{{ add.get('schedule') or change.get('schedule') }}'
  IF (EXISTS ({{ add.get('condition') or change.get('condition') }}))
  THEN
    {{ add.get('action') or change.get('action') }};

{% if (add.get('comment') or change.get('comment')) %}
ALTER ALERT {{ name }} SET COMMENT = '{{ add.get('comment') or change.get('comment') }}';
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP ALERT {{ name }};
{% endif -%}
