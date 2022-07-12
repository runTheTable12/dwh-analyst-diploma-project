{% macro normalize_team_name(column_name) %}
	CASE WHEN {{ column_name }} = 'Brisbane Lions' THEN 'Brisbane'
		 WHEN {{ column_name }} = 'Greater Western Sydney' THEN 'GWS'
		 ELSE {{ column_name }} END
{% endmacro %}