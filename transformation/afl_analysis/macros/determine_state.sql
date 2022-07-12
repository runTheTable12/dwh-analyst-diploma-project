{% macro determine_state(column_name) %}
	CASE WHEN {{ column_name }} in 
	('Carlton', 'Collingwood', 'Essendon', 'Geelong', 'Hawthorn', 'Melbourne',
	'North Melbourne', 'Richmond', 'St Kilda', 'Western Bulldogs') THEN 'Victoria'
	WHEN {{ column_name }} in ('Adelaide', 'Port Adelaide') THEN 'South Australia'
	WHEN {{ column_name }} in ('Brisbane', 'Gold Coast') THEN 'Queensland'
	WHEN {{ column_name }} in ('Fremantle', 'West Coast') THEN 'Western Australia'
	WHEN {{ column_name }} in ('GWS', 'Sydney') THEN 'New South Wales' END
{% endmacro %}