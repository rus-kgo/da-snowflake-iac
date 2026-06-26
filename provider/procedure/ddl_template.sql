{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER PROCEDURE {{ database }}.{{ schema }}.{{ name }}(
  {%- set args = add.arguments | default(change.arguments | default([])) %}
  {%- for arg in args %}
  {{ arg.name }} {{ arg.type }}{% if not loop.last %},{% endif %}
  {%- endfor %}
)
RETURNS {{ add.returns | default(change.returns) }}
LANGUAGE {{ (add.language | default(change.language)) | upper }}
{% if (add.runtime_version | default(None)) or (change.runtime_version | default(None)) %}
RUNTIME_VERSION = '{{ add.runtime_version | default(change.runtime_version) }}'
{% endif %}
{% set pkgs = add.packages | default(change.packages | default([])) %}
{% if pkgs %}
PACKAGES = ({{ pkgs | map("tojson") | join(", ") }})
{% endif %}
{% if (add.handler | default(None)) or (change.handler | default(None)) %}
HANDLER = '{{ add.handler | default(change.handler) }}'
{% endif %}
{% if (add.execute_as | default(None)) or (change.execute_as | default(None)) %}
EXECUTE AS {{ (add.execute_as | default(change.execute_as)) | upper }}
{% endif %}
{% if (add.comment | default(None)) or (change.comment | default(None)) %}
COMMENT = '{{ add.comment | default(change.comment) }}'
{% endif %}
AS
$$
{{ add.body | default(change.body) }}
$$;

{% elif ddl_command.upper() == 'DROP' %}
DROP PROCEDURE {{ database }}.{{ schema }}.{{ name }}(
  {%- set args = add.arguments | default(change.arguments | default([])) %}
  {%- for arg in args %}
  {{ arg.type }}{% if not loop.last %},{% endif %}
  {%- endfor %}
);
{% endif -%}
