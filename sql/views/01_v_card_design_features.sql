/*
One row per card. 
Provides card-level design attributes, first-print timing, color structure, type flags, and a project-defined design complexity proxy used in downstream trend analysis.

design_complexity_score is a heuristic proxy combining printed text density with rulings volume
it is intended to approximate card-design complexity, not formal rules burden.
*/

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
        coalesce(cards.color_count, 0) as color_count,
        coalesce(cards.is_colorless, false) as is_colorless,
        coalesce(cards.is_mono, false) as is_mono,
        coalesce(cards.is_multi, false) as is_multi,
        coalesce(cards.is_white, false) as is_white,
        coalesce(cards.is_blue, false) as is_blue,
        coalesce(cards.is_black, false) as is_black,
        coalesce(cards.is_red, false) as is_red,
        coalesce(cards.is_green, false) as is_green,
        coalesce(cards.face_count, 0) as face_count,
        coalesce(cards.text_length, 0) as text_length,
        coalesce(cards.text_tokens, 0) as text_tokens,
        coalesce(cards.rulings_count, 0) as rulings_count,
        coalesce(cards.printings_count, 0) as printings_count,
        coalesce(cards.text_tokens, 0) + coalesce(cards.rulings_count*10, 0) as design_complexity_score, --proxy metric for card design complexity, rulings are weighted higher than text density
        coalesce(type_flags.is_creature_card, false) as is_creature,
        coalesce(type_flags.is_artifact_card, false) as is_artifact,
        coalesce(type_flags.is_enchantment_card, false) as is_enchantment,
        coalesce(type_flags.is_instant_card, false) as is_instant,
        coalesce(type_flags.is_sorcery_card, false) as is_sorcery,
        coalesce(type_flags.is_land_card, false) as is_land,
        coalesce(type_flags.is_planeswalker_card, false) as is_planeswalker
        
    from core.cards as cards
    left join type_flags 
        on cards.id = type_flags.card_id
    order by
        card_id;