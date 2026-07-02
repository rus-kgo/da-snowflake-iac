-- DDL template for Snowflake Dynamic Iceberg Table resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE OR REPLACE DYNAMIC ICEBERG TABLE {{ name }}
  TARGET_LAG = '{{ add.get('target_lag', '1 hour') }}'
  WAREHOUSE = {{ add.get('warehouse') }}
  EXTERNAL_VOLUME = '{{ add.get('external_volume') }}'
  CATALOG = '{{ add.get('catalog') }}'
  {% if add.get('base_location') %}
  BASE_LOCATION = '{{ add.get('base_location') }}'
  {% endif %}
  {% if add.get('comment') %}COMMENT = '{{ add.get('comment') }}'{% endif %}
  AS
    {{ add.get('query') }};

{% elif ddl_command.upper() == 'ALTER' %}
{% if change.get('target_lag') %}
ALTER DYNAMIC ICEBERG TABLE {{ name }} SET TARGET_LAG = '{{ change.get('target_lag') }}';
{% endif %}
{% if change.get('warehouse') %}
ALTER DYNAMIC ICEBERG TABLE {{ name }} SET WAREHOUSE = {{ change.get('warehouse') }};
{% endif %}
{% if change.get('comment') %}
ALTER DYNAMIC ICEBERG TABLE {{ name }} SET COMMENT = '{{ change.get('comment') }}';
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP DYNAMIC ICEBERG TABLE {{ name }};
{% endif -%}
