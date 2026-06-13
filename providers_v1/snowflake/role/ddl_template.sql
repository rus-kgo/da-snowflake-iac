{% if iac_action.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER ROLE {{ name }}
{% if (add.comment | default(None)) or (change.comment | default(None)) %}
COMMENT = '{{ add.comment | default(change.comment) }}'
{% endif %};

{% elif iac_action.upper() == 'DROP' %}
DROP ROLE {{ name }};
{% endif -%}
