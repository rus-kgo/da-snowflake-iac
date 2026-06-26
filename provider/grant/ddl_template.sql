{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'GRANT' %}
GRANT {{ add.privilege }} ON {{ add.on_object_type }} {{ add.database }}.{{ add.schema }}.{{ add.on_object }}
  TO ROLE {{ add.to_role }}
  {% if add.with_grant_option %}WITH GRANT OPTION{% endif %};

{% elif ddl_command.upper() == 'REVOKE' %}
REVOKE {{ add.privilege }} ON {{ add.on_object_type }} {{ add.database }}.{{ add.schema }}.{{ add.on_object }}
  FROM ROLE {{ add.to_role }};
{% endif -%}
