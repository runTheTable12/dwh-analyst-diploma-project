select "Team", "Round", "Season",
	SUM("GA") as goal_assists_total, 
	SUM("K") as kicks_total, 
	SUM("HB") as handballs_total, 
	SUM("D") as disposals_total, 
	SUM("M") as marks_total,  
	SUM("G") as goals_total, 
	SUM("B") as behinds_total, 
	SUM("T") as tackles_total, 
	SUM("HO") as hitouts_total, 
	SUM("I50") as inside_50_total, 
	SUM("CL") as clearances_total, 
	SUM("CG") as clangers_total, 
	SUM("R50") as rebound_50_total, 
	SUM("FF") as frees_for_total, 
	SUM("FA") as frees_against_total
from {{ source('afl', 'stg_player_stats') }} 
group by "Team", "Round" , "Season" 
order by "Season" desc, "Round", "Team"