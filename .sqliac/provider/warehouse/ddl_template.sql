-- DDL template for Snowflake Warehouse resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER WAREHOUSE {{ name }}
  {% if add.get('warehouse_size') or change.get('warehouse_size') %}
  WAREHOUSE_SIZE = '{{ add.get('warehouse_size') or change.get('warehouse_size') }}'
  {% endif %}
  {% if add.get('auto_suspend') is not none or change.get('auto_suspend') is not none %}
  AUTO_SUSPEND = {{ add.get('auto_suspend', change.get('auto_suspend', 300)) }}
  {% endif %}
  {% if add.get('auto_resume') is not none or change.get('auto_resume') is not none %}
  AUTO_RESUME = {{ add.get('auto_resume', change.get('auto_resume', true)) }}
  {% endif %}
  {% if add.get('min_cluster_count') or change.get('min_cluster_count') %}
  MIN_CLUSTER_COUNT = {{ add.get('min_cluster_count') or change.get('min_cluster_count') }}
  {% endif %}
  {% if add.get('max_cluster_count') or change.get('max_cluster_count') %}
  MAX_CLUSTER_COUNT = {{ add.get('max_cluster_count') or change.get('max_cluster_count') }}
  {% endif %}
  {% if add.get('scaling_policy') or change.get('scaling_policy') %}
  SCALING_POLICY = '{{ add.get('scaling_policy') or change.get('scaling_policy') }}'
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP WAREHOUSE {{ name }};
{% endif -%}
