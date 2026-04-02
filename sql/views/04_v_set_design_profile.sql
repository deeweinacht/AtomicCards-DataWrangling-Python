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
            avg(cdf.complexity_score) as avg_complexity_score,
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
        sets.total_cards as total_cards_in_set,
        agg.total_cards_count as selected_cards_in_set,
        agg.avg_mana_value as avg_mana_value,
        agg.avg_color_count as avg_color_count,
        agg.avg_text_tokens as avg_text_tokens,
        agg.avg_rulings_count as avg_rulings_count,
        agg.avg_complexity_score as avg_complexity_score,
        agg.creature_card_count*1.0/agg.total_cards_count as creature_type_share,
        agg.artifact_card_count*1.0/agg.total_cards_count as artifact_type_share,
        agg.enchantment_card_count*1.0/agg.total_cards_count as enchantment_type_share,
        agg.instant_card_count*1.0/agg.total_cards_count as instant_type_share,
        agg.sorcery_card_count*1.0/agg.total_cards_count as sorcery_type_share,
        agg.planeswalker_card_count*1.0/agg.total_cards_count as planeswalker_type_share,
        agg.land_card_count*1.0/agg.total_cards_count as land_type_share,
        agg.white_card_count*1.0/agg.total_cards_count as white_color_share,
        agg.blue_card_count*1.0/agg.total_cards_count as blue_color_share,
        agg.black_card_count*1.0/agg.total_cards_count as black_color_share,
        agg.red_card_count*1.0/agg.total_cards_count as red_color_share,
        agg.green_card_count*1.0/agg.total_cards_count as green_color_share,


        agg.colorless_card_count*1.0/agg.total_cards_count as colorless_proportion,
        agg.mono_card_count*1.0/agg.total_cards_count as mono_card_proportion,
        agg.multi_card_count*1.0/agg.total_cards_count as multi_card_proportion
    from core.sets as sets
    join set_card_aggregates as agg
        on sets.id = agg.set_id
    order by
        set_id;