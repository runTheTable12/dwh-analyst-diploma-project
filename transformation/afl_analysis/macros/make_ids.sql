{% macro make_ids(count_column) %}
	row_number() OVER (order by {{ count_column }})
{% endmacro %}