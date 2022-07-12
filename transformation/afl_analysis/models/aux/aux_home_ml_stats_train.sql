select 
	match_id,
	winner,
	home_team,
	home_score,
	home_goals,
	home_behinds,
	away_team,
	away_score,
	away_goals,
	away_behinds,
	game_time,
	round_name,
	round_number,
	season,
	venue,
{{ avg_value('home_goal_assists_total', 5, 'home_team') }} as h_l5_goal_assists_avg,
{{ avg_value('home_kicks_total', 5, 'home_team') }} as h_l5_kicks_avg,
{{ avg_value('home_handballs_total', 5, 'home_team') }} as h_l5_handballs_avg,
{{ avg_value('home_behinds_total', 5, 'home_team') }} as h_l5_behinds_avg,
{{ avg_value('home_tackles_total', 5, 'home_team') }} as h_l5_tackles_avg,
{{ avg_value('home_hitouts_total', 5, 'home_team') }} as h_l5_hitouts_avg,
{{ avg_value('home_inside_50_total', 5, 'home_team') }} as h_l5_inside_50_avg,
{{ avg_value('home_clearances_total', 5, 'home_team') }} as h_l5_clearances_avg,
{{ avg_value('home_clangers_total', 5, 'home_team') }} as h_l5_clangers_avg,
{{ avg_value('home_rebound_50_total', 5, 'home_team') }} as h_l5_rebound_50_avg,
{{ avg_value('home_frees_for_total', 5, 'home_team') }} as h_l5_frees_for_avg,
{{ avg_value('home_frees_against_total', 5, 'home_team') }} as h_l5_frees_against_avg,
{{ avg_value('away_goal_assists_total', 5, 'home_team') }} as h_l5_opp_goal_assists_avg,
{{ avg_value('away_kicks_total', 5, 'home_team') }} as h_l5_opp_kicks_avg,
{{ avg_value('away_handballs_total', 5, 'home_team') }} as h_l5_opp_handballs_avg,
{{ avg_value('away_behinds_total', 5, 'home_team') }} as h_l5_opp_behinds_avg,
{{ avg_value('away_tackles_total', 5, 'home_team') }} as h_l5_opp_tackles_avg,
{{ avg_value('away_hitouts_total', 5, 'home_team') }} as h_l5_opp_hitouts_avg,
{{ avg_value('away_inside_50_total', 5, 'home_team') }} as h_l5_opp_inside_50_avg,
{{ avg_value('away_clearances_total', 5, 'home_team') }} as h_l5_opp_clearances_avg,
{{ avg_value('away_clangers_total', 5, 'home_team') }} as h_l5_opp_clangers_avg,
{{ avg_value('away_rebound_50_total', 5, 'home_team') }} as h_l5_opp_rebound_50_avg,
{{ avg_value('away_frees_for_total', 5, 'home_team') }} as h_l5_opp_frees_for_avg,
{{ avg_value('away_frees_against_total', 5, 'home_team') }} as h_l5_opp_frees_against_avg
from {{ ref('regular_season_match_stats')}}