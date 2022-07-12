with max_year as (
	select MAX(year) from {{ ref('aux_tips') }}
),

coming_round as (
	select MAX(round) from {{ ref('aux_tips') }} where year=(select * from max_year)
),

coming_tips as (
select hteam, ateam, hmargin, tip, hconfidence, source
from {{ ref('aux_tips') }}
where year=(select * from max_year)
and round=(select * from coming_round))

select DISTINCT
hteam as home_team,
ateam as away_team,
(select tip from coming_tips where source='Squiggle' and c.hteam = hteam) as squiggle_tip,
(select tip from coming_tips where source='Matter of Stats' and c.hteam = hteam) as mos_tip,
(select tip from coming_tips where source='Aggregate' and c.hteam = hteam) as aggregate_tip,
(select tip from coming_tips where source='Graft' and c.hteam = hteam) as graft_tip,
(select hmargin from coming_tips where source='Squiggle' and c.hteam = hteam) as squiggle_hmargin,
(select hmargin from coming_tips where source='Matter of Stats' and c.hteam = hteam) as mos_hmargin,
(select hmargin from coming_tips where source='Aggregate' and c.hteam = hteam) as aggregate_hmargin,
(select hmargin from coming_tips where source='Graft' and c.hteam = hteam) as graft_hmargin,
(select hconfidence from coming_tips where source='Squiggle' and c.hteam = hteam) as squiggle_hconfidence,
(select hconfidence from coming_tips where source='Matter of Stats' and c.hteam = hteam) as mos_hconfidence,
(select hconfidence from coming_tips where source='Aggregate' and c.hteam = hteam) as aggregate_hconfidence,
(select hconfidence from coming_tips where source='Graft' and c.hteam = hteam) as graft_hconfidence
from coming_tips c