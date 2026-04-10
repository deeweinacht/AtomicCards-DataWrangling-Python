/*
Evaluates whether cards have become more keyword dense over time by comparing average keyword counts across major card categories over time.

The permanent card category takes precedence over the non-permanent card category in the case of cards with multiple types.
*/
with card_mechanic_counts as (
    select
        card_id,
        count(distinct mechanic) as mechanic_count
    from analytics.v_color_mechanics
    group by card_id
)

select
    case
        when cdf.is_creature or cdf.is_artifact or cdf.is_enchantment or cdf.is_planeswalker or cdf.is_land then 'Permanent'
        when cdf.is_instant or cdf.is_sorcery then 'Non-Permanent'
        else null
    end as card_type_category, -- if any permament card category is true, then it's a permanent
    analytics_release_period(cdf.first_printing_year) as release_period,
    round(avg(coalesce(cmc.mechanic_count, 0)), 2) as avg_number_of_keyword_mechanics_in_category,
    coalesce(max(cmc.mechanic_count), 0) as max_number_of_keyword_mechanics_in_category,
    cast(avg(case when cmc.mechanic_count > 0 then 100.0 else 0 end) as integer) as percent_cards_with_keyword_mechanics
from
    analytics.v_card_design_features as cdf
left join card_mechanic_counts as cmc
    on cdf.card_id = cmc.card_id
group by
    analytics_release_period_order(first_printing_year),
    release_period,
    card_type_category
order by
    card_type_category desc, analytics_release_period_order(first_printing_year) asc;