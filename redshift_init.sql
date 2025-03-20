Copy-- macros/redshift_utils.sql
{% macro get_redshift_relation(model_name) %}
  {# This macro ensures proper initialization of the relation object for Redshift #}
  {% set rel = ref(model_name) %}
  {% set database = rel.database or target.database %}
  {% set schema = rel.schema or target.schema %}
  {% set identifier = rel.identifier or model_name %}
  
  {% do return(api.Relation.create(
    database=database,
    schema=schema,
    identifier=identifier,
    type='table'
  )) %}
{% endmacro %}
