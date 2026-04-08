-- Tracks the trend of creature stat efficiency (power + toughness / mana cost) over time, 
--to see if there are any noticeable changes in design philosophy or power level of creature cards across different eras of Magic: The Gathering.

select
    case
        when first_printing_year < 1995 then '1993-1995'
        when first_printing_year < 2000 then '1996-2000'
        when first_printing_year < 2005 then '2001-2005'
        when first_printing_year < 2010 then '2006-2010'
        when first_printing_year < 2015 then '2011-2015'
        when first_printing_year < 2020 then '2016-2020'
        else '2021-Present'
    end as release_period,
    round(avg(stat_efficiency), 2) as avg_stat_efficiency_all_creatures,
    round(avg(case when is_white then stat_efficiency end), 2) as avg_stat_efficiency_white,
    round(avg(case when is_blue then stat_efficiency end), 2) as avg_stat_efficiency_blue,
    round(avg(case when is_black then stat_efficiency end), 2) as avg_stat_efficiency_black,
    round(avg(case when is_red then stat_efficiency end), 2) as avg_stat_efficiency_red,
    round(avg(case when is_green then stat_efficiency end), 2) as avg_stat_efficiency_green,
    round(avg(case when is_colorless then stat_efficiency end), 2) as avg_stat_efficiency_colorless
from analytics.v_creature_design_features
where
    not has_variable_mana_cost -- Exclude creatures with variable mana costs to avoid skewing efficiency calculations
group by release_period
order by release_period asc;
