-- DDL template for Snowflake Security Integration resource
-- Co-authored with CoCo
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER SECURITY INTEGRATION {{ name }}
  TYPE = {{ add.get('type') or change.get('type') }}
  {% if add.get('saml2_issuer') or change.get('saml2_issuer') %}
  SAML2_ISSUER = '{{ add.get('saml2_issuer') or change.get('saml2_issuer') }}'
  SAML2_SSO_URL = '{{ add.get('saml2_sso_url') or change.get('saml2_sso_url') }}'
  SAML2_PROVIDER = '{{ add.get('saml2_provider') or change.get('saml2_provider') }}'
  SAML2_X509_CERT = '{{ add.get('saml2_x509_cert') or change.get('saml2_x509_cert') }}'
  {% endif %}
  {% if add.get('oauth_client') or change.get('oauth_client') %}
  OAUTH_CLIENT = {{ add.get('oauth_client') or change.get('oauth_client') }}
  {% endif %}
  {% if add.get('enabled') is not none or change.get('enabled') is not none %}
  ENABLED = {{ add.get('enabled', change.get('enabled', true)) }}
  {% endif %}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT = '{{ add.get('comment') or change.get('comment') }}'
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP SECURITY INTEGRATION {{ name }};
{% endif -%}
