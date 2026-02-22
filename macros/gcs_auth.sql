{% macro gcs_auth() %}

CREATE OR REPLACE SECRET gcs_secret (
    TYPE GCS,
    PROVIDER config,
    BEARER_TOKEN '{{ env_var("GCS_TOKEN") }}'
);

{% endmacro %}