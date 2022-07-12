{% macro coming_value(column_name, last_games_window, group_column) %}
	round(avg({{ column_name }}) OVER(PARTITION BY {{ group_column}}
	ORDER BY season, round_number 
	ROWS BETWEEN {{ last_games_window}} PRECEDING AND CURRENT ROW ), 2)
{% endmacro %}