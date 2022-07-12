select 
	{{ normalize_team_name('winner') }} as winner, 
	{{ normalize_team_name('hteam') }} as hteam, 
	hgoals, 
	hscore, 
	hbehinds, 
	{{ normalize_team_name('ateam') }} as ateam, 
	agoals, 
	ascore, 
	abehinds, 
	"localtime", 
	roundname, 
	"round", 
	"year", 
	is_grand_final, 
	is_final, 
	venue
from {{ source('afl', 'stg_total_results') }}