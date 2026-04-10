/*
One row per creature face with numeric printed stats and derived efficiency-style proxies.
Intended for creature-design trend analysis, not gameplay balance evaluation.

stat_total and stat_efficiency are printed-stat proxy metrics used for design trend analysis.
*/
create or replace view analytics.v_creature_design_features as
    with creature_faces as (
        select distinct
            card_id, face_index
        from core.types
        where type_kind = 'type' and lower(type) = 'creature'
    )
    select
        faces.card_id as card_id,
        faces.face_index as face_index,
        faces.name as face_name,
        card_features.first_printing_year as first_printing_year,
        faces.mana_value as mana_value,
        faces.power_num as power,
        faces.toughness_num as toughness,
        faces.pt_ratio as pt_ratio,
        faces.power_num + faces.toughness_num as stat_total, --proxy metric for creature card strength
        (coalesce(faces.power_num, 0) + coalesce(faces.toughness_num, 0)) / nullif(faces.mana_value, 0) as stat_efficiency,  --proxy metric for creature strength efficiency
        faces.mana_cost_is_variable as has_variable_mana_cost,
        faces.is_colorless as is_colorless,
        faces.is_white as is_white,
        faces.is_blue as is_blue,
        faces.is_black as is_black,
        faces.is_red as is_red,
        faces.is_green as is_green
    from
        core.card_faces as faces
    inner join creature_faces
         on faces.card_id = creature_faces.card_id and faces.face_index = creature_faces.face_index
    left join analytics.v_card_design_features as card_features
        on faces.card_id = card_features.card_id
    where
        faces.power_num is not null
        and 
        faces.toughness_num is not null
        and
        faces.mana_value > 0
    order by
        card_id, face_index;