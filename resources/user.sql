{% if iac_action.upper() == 'CREATE' %}
{{ iac_action }} USER {{ name }}
{% if password %} PASSWORD = '{{ password }}' {% endif %}
{% if login_name %} LOGIN_NAME = '{{ login_name }}' {% endif %}
{% if display_name %} DISPLAY_NAME = '{{ display_name }}' {% endif %}
{% if email %} EMAIL = '{{ email }}' {% endif %}
{% if disabled is not none %} DISABLED = {{ disabled | string | upper }} {% endif %}
{% if default_warehouse %} DEFAULT_WAREHOUSE = '{{ default_warehouse }}' {% endif %}
{% if default_namespace %} DEFAULT_NAMESPACE = '{{ default_namespace }}' {% endif %}
{% if default_role %} DEFAULT_ROLE = '{{ default_role }}' {% endif %}
{% if must_change_password is not none %} MUST_CHANGE_PASSWORD = {{ must_change_password | string | upper }} {% endif %}
{% if rsa_public_key %} RSA_PUBLIC_KEY = '{{ rsa_public_key }}' {% endif %}
{% if rsa_public_key_2 %} RSA_PUBLIC_KEY_2 = '{{ rsa_public_key_2 }}' {% endif %}
COMMENT = '{"comment":"{{ comment }}", "object_id_tag": "{{ object_id_tag }}"}';

{% elif iac_action.upper() == 'ALTER' %}
{{ iac_action }} USER {{ name }}
{% if password %} SET PASSWORD = '{{ password }}'; {% endif %}
{% if display_name %} SET DISPLAY_NAME = '{{ display_name }}'; {% endif %}
{% if email %} SET EMAIL = '{{ email }}'; {% endif %}
{% if disabled is not none %} SET DISABLED = {{ disabled | string | upper }}; {% endif %}
{% if default_warehouse %} SET DEFAULT_WAREHOUSE = '{{ default_warehouse }}'; {% endif %}
{% if default_namespace %} SET DEFAULT_NAMESPACE = '{{ default_namespace }}'; {% endif %}
{% if default_role %} SET DEFAULT_ROLE = '{{ default_role }}'; {% endif %}
{% if must_change_password is not none %} SET MUST_CHANGE_PASSWORD = {{ must_change_password | string | upper }}; {% endif %}
{% if rsa_public_key %} SET RSA_PUBLIC_KEY = '{{ rsa_public_key }}'; {% endif %}
{% if rsa_public_key_2 %} SET RSA_PUBLIC_KEY_2 = '{{ rsa_public_key_2 }}'; {% endif %}
{% if comment %} SET COMMENT = '{"commen
