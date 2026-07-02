-- DDL template for Snowflake Grant resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

-- Grants are idempotent; re-granting the same privilege is a no-op
{% if ddl_command.upper() == 'GRANT' %}
GRANT {{ add.get('privilege') or change.get('privilege') }}
  ON {{ add.get('on_type') or change.get('on_type') }} {{ add.get('on_name') or change.get('on_name') }}
  TO ROLE {{ add.get('to_role') or change.get('to_role') }}
  {% if add.get('with_grant_option') or change.get('with_grant_option') %}WITH GRANT OPTION{% endif %};

{% elif ddl_command.upper() == 'REVOKE' %}
REVOKE {{ privilege }}
  ON {{ on_type }} {{ on_name }}
  FROM ROLE {{ to_role }};
{% endif -%}
