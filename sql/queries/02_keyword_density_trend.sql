-- Evaluates whether cards have become more mechanically dense over time by comparing average keyword counts across major card categories over time.
with era_and_card_category as (
    select
        card_id,
        case
            when first_printing_year < 2000 then '1993-2000'
            when first_printing_year < 2010 then '2000-2010'
            when first_printing_year < 2020 then '2010-2020'
            else '2020-Present'
        end as release_decade,
        case
            when is_creature or is_artifact or is_enchantment or is_planeswalker or is_land then 'Permanent'
            when is_instant or is_sorcery then 'Non-Permanent'
            else null
        end as card_category
    from
        analytics.v_card_design_features
),
card_mechanic_counts as (
    select
        card_id,
        count(mechanic) as mechanic_count
    from analytics.v_color_mechanics
    group by card_id
)

select
    eacc.card_category as card_type_category,
    eacc.release_decade as release_decade,
    round(avg(coalesce(cmc.mechanic_count, 0)), 2) as avg_number_of_keyword_mechanics,
    coalesce(max(cmc.mechanic_count), 0) as max_number_of_keyword_mechanics,
    cast(avg(case when cmc.mechanic_count > 0 then 100.0 else 0 end) as integer) as percent_cards_with_keyword_mechanics
from
    era_and_card_category as eacc
left join card_mechanic_counts as cmc
    on eacc.card_id = cmc.card_id
group by
    release_decade, card_type_category
order by
    card_type_category, release_decade asc;