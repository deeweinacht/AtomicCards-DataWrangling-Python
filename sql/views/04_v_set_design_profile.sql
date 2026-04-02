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
            avg(cdm.mana_value) as avg_mana_value,
            avg(cdm.color_count) as avg_color_count,
            avg(cdm.text_tokens) as avg_text_tokens,
            avg(cdm.rulings_count) as avg_rulings_count,
            avg(cdm.complexity_score) as avg_complexity_score,
            count(*) filter (where cdm.is_creature = true) as creature_card_count,
            count(*) filter (where cdm.is_artifact = true) as artifact_card_count,
            count(*) filter (where cdm.is_enchantment = true) as enchantment_card_count,
            count(*) filter (where cdm.is_instant = true) as instant_card_count,
            count(*) filter (where cdm.is_sorcery = true) as sorcery_card_count,
            count(*) filter (where cdm.is_land = true) as land_card_count,
            count(*) filter (where cdm.is_planeswalker = true) as planeswalker_card_count,
            count(*) filter (where cdm.is_colorless = true) as colorless_card_count,
            count(*) filter (where cdm.is_mono = true) as mono_card_count,
            count(*) filter (where cdm.is_multi = true) as multi_card_count,
            count(*) filter (where cdm.is_white = true) as white_card_count,
            count(*) filter (where cdm.is_blue = true) as blue_card_count,
            count(*) filter (where cdm.is_black = true) as black_card_count,            
            count(*) filter (where cdm.is_red = true) as red_card_count,
            count(*) filter (where cdm.is_green = true) as green_card_count
        from set_card_map as scm
        join analytics.v_card_design_features as cdm
            on scm.card_id = cdm.card_id
        group by set_id
    )
    select
        sets.id as set_id,
        sets.name as set_name,
        sets.release_date::date as release_date,
        EXTRACT('year' FROM sets.release_date) as release_year,
        sets.type as set_type,
        sets.total_cards as total_cards,
        agg.avg_mana_value as avg_mana_value,
        agg.avg_color_count as avg_color_count,
        agg.avg_text_tokens as avg_text_tokens,
        agg.avg_rulings_count as avg_rulings_count,
        agg.avg_complexity_score as avg_complexity_score,
        agg.total_cards_count as total_cards_count,
        agg.creature_card_count as creature_card_count,
        agg.artifact_card_count as artifact_card_count,
        agg.enchantment_card_count as enchantment_card_count,
        agg.instant_card_count as instant_card_count,
        agg.sorcery_card_count as sorcery_card_count,
        agg.land_card_count as land_card_count,
        agg.planeswalker_card_count as planeswalker_card_count,
        agg.colorless_card_count as colorless_card_count,
        agg.mono_card_count as mono_card_count,
        agg.multi_card_count as multi_card_count,
        agg.white_card_count as white_card_count,
        agg.blue_card_count as blue_card_count,
        agg.black_card_count as black_card_count,
        agg.red_card_count as red_card_count,
        agg.green_card_count as green_card_count
    from core.sets as sets
    join set_card_aggregates as agg
        on sets.id = agg.set_id
    order by
        set_id;