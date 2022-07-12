with home_team_stats as (select 
atr.winner as winner,
atr.hteam as home_team,
atr.hscore as home_score,
atr.hgoals as home_goals,
atr.hbehinds as home_behinds,
atr.ateam as away_team,
atr.ascore as away_score,
atr.agoals as away_goals,
atr.abehinds as away_behinds,
atr.localtime::timestamp as game_time,
atr.roundname as round_name,
atr.round as round_number,
atr.year as season,
atr.venue as venue,
aps.goal_assists_total as home_goal_assists_total,
aps.kicks_total as home_kicks_total,
aps.handballs_total as home_handballs_total,
aps.behinds_total as home_behinds_total,
aps.tackles_total as home_tackles_total,
aps.hitouts_total as home_hitouts_total,
aps.inside_50_total as home_inside_50_total,
aps.clearances_total as home_clearances_total,
aps.clangers_total as home_clangers_total,
aps.rebound_50_total as home_rebound_50_total,
aps.frees_for_total as home_frees_for_total,
aps.frees_against_total as home_frees_against_total 
from {{ ref('aux_total_results') }} atr
inner join {{ ref('aux_player_stats') }} aps
ON
atr.hteam = aps."Team" AND 
atr.roundname = aps."Round" AND
atr.year = aps."Season" AND
atr.is_final = 0),

away_team_stats as (
select 
atr.winner as winner,
atr.hteam as home_team,
atr.hscore as home_score,
atr.hgoals as home_goals,
atr.hbehinds as home_behinds,
atr.ateam as away_team,
atr.ascore as away_score,
atr.agoals as away_goals,
atr.abehinds as away_behinds,
atr.localtime as game_time,
atr.roundname as round_name,
atr.round as round_number,
atr.year as season,
atr.venue as venue,
aps.goal_assists_total as away_goal_assists_total,
aps.kicks_total as away_kicks_total,
aps.handballs_total as away_handballs_total,
aps.behinds_total as away_behinds_total,
aps.tackles_total as away_tackles_total,
aps.hitouts_total as away_hitouts_total,
aps.inside_50_total as away_inside_50_total,
aps.clearances_total as away_clearances_total,
aps.clangers_total as away_clangers_total,
aps.rebound_50_total as away_rebound_50_total,
aps.frees_for_total as away_frees_for_total,
aps.frees_against_total as away_frees_against_total 
from {{ ref('aux_total_results') }} atr
inner join {{ ref('aux_player_stats') }} aps
ON
atr.ateam = aps."Team" AND 
atr.roundname = aps."Round" AND
atr.year = aps."Season" AND
atr.is_final = 0)

select 
{{ make_ids('h.game_time') }} as match_id,
h.*,
case when h.winner=h.home_team then 1
else 0 end as is_home_win,
{{ determine_state('h.home_team') }} as home_team_state,
{{ determine_state('h.away_team') }} as away_team_state,
a.away_goal_assists_total,
a.away_kicks_total,
a.away_handballs_total,
a.away_behinds_total,
a.away_tackles_total,
a.away_hitouts_total,
a.away_inside_50_total,
a.away_clearances_total,
a.away_clangers_total,
a.away_rebound_50_total,
a.away_frees_for_total,
a.away_frees_against_total 

from home_team_stats h
inner join 
away_team_stats a
ON h.winner = a.winner and 
h.home_team = a.home_team and 
h.away_team = a.away_team and
h.season = a.season and 
h.round_number = a.round_number 
order by game_time desc