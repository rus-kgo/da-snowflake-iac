{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE SECURITY INTEGRATION {{ name }}
  TYPE = EXTERNAL_OAUTH
  ENABLED = {{ add.enabled | string | upper }}
  {% if add.external_oauth_type %}EXTERNAL_OAUTH_TYPE = {{ add.external_oauth_type | upper }}{% endif %}
  {% if add.external_oauth_issuer %}EXTERNAL_OAUTH_ISSUER = '{{ add.external_oauth_issuer }}'{% endif %}
  {% if add.external_oauth_token_user_mapping_claim %}
  EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM = ({{ add.external_oauth_token_user_mapping_claim | map("tojson") | join(", ") }})
  {% endif %}
  {% if add.external_oauth_snowflake_user_mapping_attribute %}
  EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE = '{{ add.external_oauth_snowflake_user_mapping_attribute }}'
  {% endif %}
  {% if add.external_oauth_jws_keys_url %}
  EXTERNAL_OAUTH_JWS_KEYS_URL = ({{ add.external_oauth_jws_keys_url | map("tojson") | join(", ") }})
  {% endif %}
  {% if add.external_oauth_audience_list %}
  EXTERNAL_OAUTH_AUDIENCE_LIST = ({{ add.external_oauth_audience_list | map("tojson") | join(", ") }})
  {% endif %}
  {% if add.external_oauth_any_role_mode %}EXTERNAL_OAUTH_ANY_ROLE_MODE = '{{ add.external_oauth_any_role_mode }}'{% endif %}
  {% if add.comment %}COMMENT = '{{ add.comment }}'{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
ALTER SECURITY INTEGRATION {{ name }} SET
  {% if change.enabled is not none %}ENABLED = {{ change.enabled | string | upper }}{% endif %}
  {% if change.external_oauth_token_user_mapping_claim %}
  EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM = ({{ change.external_oauth_token_user_mapping_claim | map("tojson") | join(", ") }})
  {% endif %}
  {% if change.external_oauth_snowflake_user_mapping_attribute %}
  EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE = '{{ change.external_oauth_snowflake_user_mapping_attribute }}'
  {% endif %}
  {% if change.external_oauth_jws_keys_url %}
  EXTERNAL_OAUTH_JWS_KEYS_URL = ({{ change.external_oauth_jws_keys_url | map("tojson") | join(", ") }})
  {% endif %}
  {% if change.external_oauth_audience_list %}
  EXTERNAL_OAUTH_AUDIENCE_LIST = ({{ change.external_oauth_audience_list | map("tojson") | join(", ") }})
  {% endif %}
  {% if change.external_oauth_any_role_mode %}EXTERNAL_OAUTH_ANY_ROLE_MODE = '{{ change.external_oauth_any_role_mode }}'{% endif %}
  {% if change.comment %}COMMENT = '{{ change.comment }}'{% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP SECURITY INTEGRATION {{ name }};
{% endif -%}
