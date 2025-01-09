{% if action == 'drop' %}
{{ action }} SECURITY INTEGRATION {{ name }}
{% else %}

{{ action }} SECURITY INTEGRATION {{ name }}

{% if action == 'alter' -%} 
SET 
{% endif -%}

    TYPE = EXTERNAL_OAUTH

    {% if enabled -%}
        ENABLED = {{ enabled }} 
    {% endif -%}

    {% if external_oauth_type -%}
        EXTERNAL_OAUTH_TYPE = '{{ external_oauth_type }}'
    {% endif -%}

    {% if external_oauth_issuer -%}
        EXTERNAL_OAUTH_ISSUER = ('{{ external_oauth_issuer | join("', '")}}')
    {% endif -%}

    {% if external_oauth_token_user_mapping_claim -%}
        EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM = ('{{ external_oauth_token_user_mapping_claim | join ("', '")}}')
    {% endif -%}

    {% if external_oauth_snowflake_user_mapping_attribute -%}
        EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE = '{{ external_oauth_snowflake_user_mapping_attribute }}'
    {% endif -%}

    {% if external_oauth_jws_keys_url -%}
        EXTERNAL_OAUTH_JWS_KEYS_URL = ('{{ external_oauth_jws_keys_url | join("', '") }}')
    {% endif -%}

    {% if external_oauth_blocked_roles_list -%}
        EXTERNAL_OAUTH_BLOCKED_ROLES_LIST = ('{{ external_oauth_blocked_roles_list | join("', '") }}')
    {% endif -%}

    {% if external_oauth_allowed_roles_list -%}
        EXTERNAL_OAUTH_ALLOWED_ROLES_LIST = ('{{ external_oauth_allowed_roles_list | join("', '") }}')
    {% endif -%}

    {% if external_oauth_rsa_public_key -%}
        EXTERNAL_OAUTH_RSA_PUBLIC_KEY = '{{ external_oauth_rsa_public_key }}'
    {% endif -%}

    {% if external_oauth_rsa_public_key_2 -%}
        EXTERNAL_OAUTH_RSA_PUBLIC_KEY_2 = '{{ external_oauth_rsa_public_key_2 }}'
    {% endif -%}

    {% if external_oauth_audience_list -%}
        EXTERNAL_OAUTH_AUDIENCE_LIST = ('{{ external_oauth_audience_list | join("', '") }}')
    {% endif -%}

    {% if external_oauth_any_role_mode -%}
        EXTERNAL_OAUTH_ANY_ROLE_MODE = '{{ external_oauth_any_role_mode }}'
    {% endif -%}

    {% if external_oauth_scope_delimiter -%}
        EXTERNAL_OAUTH_SCOPE_DELIMITER = '{{ external_oauth_scope_delimiter }}'
    {% endif -%}

    {% if external_oauth_scope_mapping_attribute -%}
        EXTERNAL_OAUTH_SCOPE_MAPPING_ATTRIBUTE = '{{ external_oauth_scope_mapping_attribute }}'
    {% endif -%}

    COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}'

    {% endif -%};