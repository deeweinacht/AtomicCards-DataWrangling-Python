/*
One row per card-keyword observation joined to card color structure, used for mechanic prevalence and color-concentration analysis.

color_category classifies cards into colorless, monocolored, and multicolored buckets to support mechanic distribution analysis at different color granularities.
*/
create or replace view analytics.v_color_mechanics as
    select
        keywords.card_id as card_id,
        keywords.keyword as mechanic,
        cards.first_printing_year as first_printing_year,
        cards.color_count as color_count,
        cards.is_white as is_white,
        cards.is_blue as is_blue,
        cards.is_black as is_black,
        cards.is_red as is_red,
        cards.is_green as is_green,
        case
            when cards.is_colorless then 'Colorless'
            when cards.is_mono then 'Monocolored'
            when cards.is_multi then 'Multicolored'
            else 'Unknown' 
        end as color_category
    from core.keywords as keywords
    join analytics.v_card_design_features as cards
        on keywords.card_id = cards.card_id
    order by
        card_id, mechanic;
    
