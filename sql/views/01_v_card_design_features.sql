create or replace view analytics.v_card_design_features as
    with type_flags as (
        select
            card_id,
            max(type_kind = 'type' and lower(type) = 'creature') as is_creature_card,
            max(type_kind = 'type' and lower(type) = 'artifact') as is_artifact_card,
            max(type_kind = 'type' and lower(type) = 'enchantment') as is_enchantment_card,
            max(type_kind = 'type' and lower(type) = 'instant') as is_instant_card,
            max(type_kind = 'type' and lower(type) = 'sorcery') as is_sorcery_card,
            max(type_kind = 'type' and lower(type) = 'land') as is_land_card,
            max(type_kind = 'type' and lower(type) = 'planeswalker') as is_planeswalker_card
        from core.types
        group by card_id
    )
    select
        cards.id as card_id,
        cards.name as card_name,
        cards.first_printing_date::date as first_printing_date,
        EXTRACT('year' FROM cards.first_printing_date) as first_printing_year,
        cards.mana_value as mana_value,
        cards.color_identity as color_identity,
        cards.color_count as color_count,
        cards.is_colorless as is_colorless,
        cards.is_mono as is_mono,
        cards.is_multi as is_multi,
        cards.is_white as is_white,
        cards.is_blue as is_blue,
        cards.is_black as is_black,
        cards.is_red as is_red,
        cards.is_green as is_green,
        cards.face_count as face_count,
        cards.text_length as text_length,
        cards.text_tokens as text_tokens,
        cards.rulings_count as rulings_count,
        cards.printings_count as printings_count,
        coalesce(cards.text_tokens, 0) + coalesce(cards.rulings_count, 0) as complexity_score,
        type_flags.is_creature_card as is_creature,
        type_flags.is_artifact_card as is_artifact,
        type_flags.is_enchantment_card as is_enchantment,
        type_flags.is_instant_card as is_instant,
        type_flags.is_sorcery_card as is_sorcery,
        type_flags.is_land_card as is_land,
        type_flags.is_planeswalker_card as is_planeswalker
        
    from core.cards as cards
    left join type_flags 
        on cards.id = type_flags.card_id
    order by
        card_id;