select 
	hmargin, 
	correct, 
	tip, 
	{{ normalize_team_name('hteam') }} as hteam, 
	source, 
	{{ normalize_team_name('ateam') }} as ateam, 
	hconfidence, 
	"year", 
	"round"  
from {{ source('afl', 'stg_total_tips') }}
where source in ('Squiggle', 'Matter of Stats', 'Aggregate', 'Graft')