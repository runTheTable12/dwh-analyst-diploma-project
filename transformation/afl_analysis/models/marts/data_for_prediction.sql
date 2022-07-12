with home_teams as(
	select home_team from {{ ref('comming_round_mart') }}
),
away_teams as(
	select away_team from {{ ref('comming_round_mart') }}
),
current_year as (
	select MAX(year) from {{ ref('aux_tips') }}
),

coming_round as (
	select MAX(round) from {{ ref('aux_tips') }} where year=(select * from current_year)
),
h_stats as (
select home_team, season, round_number, venue,
	   h_l5_goal_assists_avg,
	   h_l5_kicks_avg,
	   h_l5_handballs_avg,
	   h_l5_behinds_avg,
	   h_l5_tackles_avg,
	   h_l5_hitouts_avg,
	   h_l5_inside_50_avg,
	   h_l5_clearances_avg,
	   h_l5_clangers_avg,
	   h_l5_rebound_50_avg,
	   h_l5_frees_for_avg,
	   h_l5_frees_against_avg,
	   h_l5_opp_goal_assists_avg,
	   h_l5_opp_kicks_avg,
	   h_l5_opp_handballs_avg,
	   h_l5_opp_behinds_avg,
	   h_l5_opp_tackles_avg,
	   h_l5_opp_hitouts_avg,
	   h_l5_opp_inside_50_avg,
	   h_l5_opp_clearances_avg,
	   h_l5_opp_clangers_avg,
	   h_l5_opp_rebound_50_avg,
	   h_l5_opp_frees_for_avg,
	   h_l5_opp_frees_against_avg
from {{ ref('ml_mart_test') }}
where home_team in (select * from home_teams)
and season = (select * from current_year)),
a_stats as (

select away_team, season, round_number,
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
from {{ ref('ml_mart_test') }}
where away_team in (select * from away_teams)
and season = (select * from current_year)),
home_filtered as (
select h_stats.* from h_stats
inner join
(select home_team, MAX(round_number) as round_number
from h_stats
group by home_team) h_t
ON h_stats.home_team = h_t.home_team AND h_stats.round_number = h_t.round_number),

away_filtered as (
	select a_stats.* from a_stats
inner join
(select away_team, MAX(round_number) as round_number
from a_stats
group by away_team) a_t
ON a_stats.away_team = a_t.away_team AND a_stats.round_number = a_t.round_number
)

select c.home_team, c.away_team,
		h.venue,
		h_l5_goal_assists_avg,
	   h_l5_kicks_avg,
	   h_l5_handballs_avg,
	   h_l5_behinds_avg,
	   h_l5_tackles_avg,
	   h_l5_hitouts_avg,
	   h_l5_inside_50_avg,
	   h_l5_clearances_avg,
	   h_l5_clangers_avg,
	   h_l5_rebound_50_avg,
	   h_l5_frees_for_avg,
	   h_l5_frees_against_avg,
	   h_l5_opp_goal_assists_avg,
	   h_l5_opp_kicks_avg,
	   h_l5_opp_handballs_avg,
	   h_l5_opp_behinds_avg,
	   h_l5_opp_tackles_avg,
	   h_l5_opp_hitouts_avg,
	   h_l5_opp_inside_50_avg,
	   h_l5_opp_clearances_avg,
	   h_l5_opp_clangers_avg,
	   h_l5_opp_rebound_50_avg,
	   h_l5_opp_frees_for_avg,
	   h_l5_opp_frees_against_avg,
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

from {{ ref('comming_round_mart') }} c
inner join home_filtered h
on c.home_team = h.home_team
inner join away_filtered a
on c.away_team = a.away_team
