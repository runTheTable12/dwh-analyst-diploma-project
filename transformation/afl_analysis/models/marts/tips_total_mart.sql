with coming_tips as (
select hteam, ateam, hmargin, tip, hconfidence, source, year, round, correct
from {{ ref('aux_tips') }}
)

select distinct year as season, round, correct,
hteam as home_team,
ateam as away_team,
(select tip from coming_tips where source='Squiggle' and c.hteam = hteam and c.year=year and c.round = round) as squiggle_tip,
(select tip from coming_tips where source='Matter of Stats' and c.hteam = hteam and c.year=year and c.round = round) as mos_tip,
(select tip from coming_tips where source='Aggregate' and c.hteam = hteam and c.year=year and c.round = round) as aggregate_tip,
(select tip from coming_tips where source='Graft' and c.hteam = hteam and c.year=year and c.round = round) as graft_tip,
(select hmargin from coming_tips where source='Squiggle' and c.hteam = hteam and c.year=year and c.round = round) as squiggle_hmargin,
(select hmargin from coming_tips where source='Matter of Stats' and c.hteam = hteam and c.year=year and c.round = round) as mos_hmargin,
(select hmargin from coming_tips where source='Aggregate' and c.hteam = hteam and c.year=year and c.round = round) as aggregate_hmargin,
(select hmargin from coming_tips where source='Graft' and c.hteam = hteam and c.year=year and c.round = round) as graft_hmargin,
(select hconfidence from coming_tips where source='Squiggle' and c.hteam = hteam and c.year=year and c.round = round) as squiggle_hconfidence,
(select hconfidence from coming_tips where source='Matter of Stats' and c.hteam = hteam and c.year=year and c.round = round) as mos_hconfidence,
(select hconfidence from coming_tips where source='Aggregate' and c.hteam = hteam and c.year=year and c.round = round) as aggregate_hconfidence,
(select hconfidence from coming_tips where source='Graft' and c.hteam = hteam and c.year=year and c.round = round) as graft_hconfidence
from coming_tips c
order by season desc, round desc