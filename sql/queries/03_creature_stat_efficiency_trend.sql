/*
Tracks the trend of creature stat efficiency (power + toughness / mana cost) by color over time, 
to track changes in design philosophy of creature cards across different eras of Magic: The Gathering.
*/

select
    analytics_release_period(first_printing_year) as release_period,
    round(avg(stat_efficiency), 2) as avg_stat_efficiency_all_creatures, --printed-stat efficiency proxy
    round(avg(case when is_white then stat_efficiency end), 2) as avg_stat_efficiency_white, --printed-stat efficiency proxy
    round(avg(case when is_blue then stat_efficiency end), 2) as avg_stat_efficiency_blue, --printed-stat efficiency proxy
    round(avg(case when is_black then stat_efficiency end), 2) as avg_stat_efficiency_black, --printed-stat efficiency proxy
    round(avg(case when is_red then stat_efficiency end), 2) as avg_stat_efficiency_red, --printed-stat efficiency proxy
    round(avg(case when is_green then stat_efficiency end), 2) as avg_stat_efficiency_green, --printed-stat efficiency proxy
    round(avg(case when is_colorless then stat_efficiency end), 2) as avg_stat_efficiency_colorless --printed-stat efficiency proxy
from analytics.v_creature_design_features
where
    not has_variable_mana_cost -- Exclude creatures with variable mana costs to avoid skewing efficiency calculations
group by release_period
order by release_period asc;
