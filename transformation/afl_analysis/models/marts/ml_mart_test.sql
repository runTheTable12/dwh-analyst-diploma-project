select 
	h.*,
	a_l5_goal_assists_avg,
	a_l5_kicks_avg,
	a_l5_handballs_avg,
	a_l5_behinds_avg,
	a_l5_tackles_avg,
	a_l5_hitouts_avg,
	a_l5_inside_50_avg,
	a_l5_clearances_avg,
	a_l5_clangers_avg,
	a_l5_rebound_50_avg,
	a_l5_frees_for_avg,
	a_l5_frees_against_avg,
	a_l5_opp_goal_assists_avg,
	a_l5_opp_kicks_avg,
	a_l5_opp_handballs_avg,
	a_l5_opp_behinds_avg,
	a_l5_opp_tackles_avg,
	a_l5_opp_hitouts_avg,
	a_l5_opp_inside_50_avg,
	a_l5_opp_clearances_avg,
	a_l5_opp_clangers_avg,
	a_l5_opp_rebound_50_avg,
	a_l5_opp_frees_for_avg,
	a_l5_opp_frees_against_avg
from {{ ref('aux_home_ml_stats_test') }} h
INNER JOIN
	 {{ ref('aux_away_ml_stats_test')}} a 
ON h.match_id = a.match_id
--where h.season = (select MAX(season) from {{ ref('aux_home_ml_stats_test') }}) AND
--h.round_number = (select MAX(round_number)-1 from {{ ref('aux_home_ml_stats_test') }})