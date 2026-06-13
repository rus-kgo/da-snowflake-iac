{% set add = add | default({}) %}
{% set change = change | default({}) %}

-- use add.<field> for new resource values
-- use change.<field> when altering an existing object

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER 
{%- if (add.transient | default(false)) or (change.transient | default(false)) %}
 TRANSIENT
{% endif %}
 DATABASE {{ name }}

{% if (add.data_retention_time_in_days | default(None)) 
   or (change.data_retention_time_in_days | default(None)) %}
DATA_RETENTION_TIME_IN_DAYS = {{ add.data_retention_time_in_days | default(change.data_retention_time_in_days) }}
{% endif %}

{% if (add.default_ddl_collation | default(None)) 
   or (change.default_ddl_collation | default(None)) %}
DEFAULT_DDL_COLLATION = '{{ add.default_ddl_collation | default(change.default_ddl_collation) }}'
{% endif %}

{% if (add.comment | default(None)) or (change.comment | default(None)) %}
COMMENT = '{{ add.comment | default(change.comment) }}'
{% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP DATABASE {{ name }};
{% endif -%}
