{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

{% set transient = 'transient' | pick(add, change) %}
{% set data_retention_time_in_days = 'data_retention_time_in_days' | pick(add, change) %}
{% set default_ddl_collation = 'default_ddl_collation' | pick(add, change) %}
{% set comment = 'comment' | pick(add, change) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER
{%- if transient %}
 TRANSIENT
{% endif %}
 DATABASE {{ name }}

{% if data_retention_time_in_days is not none %}
DATA_RETENTION_TIME_IN_DAYS = {{ data_retention_time_in_days }}
{% endif %}

{% if default_ddl_collation is not none %}
DEFAULT_DDL_COLLATION = '{{ default_ddl_collation }}'
{% endif %}

{% if comment is not none %}
COMMENT = '{{ comment }}'
{% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP DATABASE {{ name }};
{% endif -%}
