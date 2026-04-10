/*
One row per set summarizing the selected card pool mapped to that set’s printings. 
Used to compare sets by design composition and identify outlier release profiles.

Set-level shares are calculated over the project’s filtered paper constructed card pool,
not over the card counts in the original source files.
*/
create or replace view analytics.v_set_design_profile as
    with set_card_map as (
        select
            c.id as card_id,
            unnest(c.printings) as set_id
        from core.cards as c
    ),
    set_card_aggregates as (
        select
            scm.set_id,
            count(*) as total_cards_count,
            avg(cdf.mana_value) as avg_mana_value,
            avg(cdf.color_count) as avg_color_count,
            avg(cdf.text_tokens) as avg_text_tokens,
            avg(cdf.rulings_count) as avg_rulings_count,
            avg(cdf.design_complexity_score) as avg_complexity_score, --proxy metric for card design complexity, rulings are weighted higher than text density
            count(*) filter (where cdf.is_creature = true) as creature_card_count,
            count(*) filter (where cdf.is_artifact = true) as artifact_card_count,
            count(*) filter (where cdf.is_enchantment = true) as enchantment_card_count,
            count(*) filter (where cdf.is_instant = true) as instant_card_count,
            count(*) filter (where cdf.is_sorcery = true) as sorcery_card_count,
            count(*) filter (where cdf.is_land = true) as land_card_count,
            count(*) filter (where cdf.is_planeswalker = true) as planeswalker_card_count,
            count(*) filter (where cdf.is_colorless = true) as colorless_card_count,
            count(*) filter (where cdf.is_mono = true) as mono_card_count,
            count(*) filter (where cdf.is_multi = true) as multi_card_count,
            count(*) filter (where cdf.is_white = true) as white_card_count,
            count(*) filter (where cdf.is_blue = true) as blue_card_count,
            count(*) filter (where cdf.is_black = true) as black_card_count,            
            count(*) filter (where cdf.is_red = true) as red_card_count,
            count(*) filter (where cdf.is_green = true) as green_card_count
        from set_card_map as scm
        join analytics.v_card_design_features as cdf
            on scm.card_id = cdf.card_id
        group by set_id
    )
    select
        sets.id as set_id,
        sets.name as set_name,
        sets.release_date::date as release_date,
        EXTRACT('year' FROM sets.release_date) as release_year,
        sets.type as set_type,
        sets.total_cards as all_cards_in_set,
        agg.total_cards_count as paper_constructed_cards_in_set,
        agg.avg_mana_value as avg_mana_value,
        agg.avg_color_count as avg_color_count,
        agg.avg_text_tokens as avg_text_tokens,
        agg.avg_rulings_count as avg_rulings_count,
        agg.avg_complexity_score as avg_complexity_score,
        agg.creature_card_count*1.0/nullif(agg.total_cards_count, 0) as creature_type_share,
        agg.artifact_card_count*1.0/nullif(agg.total_cards_count, 0) as artifact_type_share,
        agg.enchantment_card_count*1.0/nullif(agg.total_cards_count, 0) as enchantment_type_share,
        agg.instant_card_count*1.0/nullif(agg.total_cards_count, 0) as instant_type_share,
        agg.sorcery_card_count*1.0/nullif(agg.total_cards_count, 0) as sorcery_type_share,
        agg.planeswalker_card_count*1.0/nullif(agg.total_cards_count, 0) as planeswalker_type_share,
        agg.land_card_count*1.0/nullif(agg.total_cards_count, 0) as land_type_share,
        agg.white_card_count*1.0/nullif(agg.total_cards_count, 0) as white_color_share,
        agg.blue_card_count*1.0/nullif(agg.total_cards_count, 0) as blue_color_share,
        agg.black_card_count*1.0/nullif(agg.total_cards_count, 0) as black_color_share,
        agg.red_card_count*1.0/nullif(agg.total_cards_count, 0) as red_color_share,
        agg.green_card_count*1.0/nullif(agg.total_cards_count, 0) as green_color_share,


        agg.colorless_card_count*1.0/nullif(agg.total_cards_count, 0) as colorless_proportion,
        agg.mono_card_count*1.0/nullif(agg.total_cards_count, 0) as mono_card_proportion,
        agg.multi_card_count*1.0/nullif(agg.total_cards_count, 0) as multi_card_proportion
    from core.sets as sets
    join set_card_aggregates as agg
        on sets.id = agg.set_id
    order by
        set_id;