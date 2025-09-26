{% extends 'base_alert_query.tmpl' %}
{% block criteria %}
    severity >= {{ severity }} AND
    investigation_ids IS NOT NULL AND
    status = 'OPEN'
{% endblock %}