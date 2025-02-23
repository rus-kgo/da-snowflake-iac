USE ROLE SECURITYADMIN; 

{% if iac_action.upper() == 'GRANT' %}
GRANT {{ privilege }} ON {{ on_object_type }} {{ on_object }}
TO ROLE {{ to_role }}
{% if with_grant_option %} WITH GRANT OPTION {% endif %};

{% elif iac_action.upper() == 'REVOKE' %}
REVOKE {{ privilege }} ON {{ on_object_type }} {{ on_object }}
FROM ROLE {{ to_role }};

{% endif -%}