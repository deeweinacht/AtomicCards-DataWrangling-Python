-- Creates a design fingerprint for each expansion set by summarizing card complexity, mana value, color structure, and type prevalence within the set.
-- Expansion sets are primarily where new cards and mechanics are introduced, so this provides a way to track how design trends evolve at the set level over time. 

select 
    set_name,
    release_year,
    round(avg_mana_value, 2) as avg_mana_value,
    cast(avg_complexity_score as integer) as avg_complexity_score,
    round(creature_type_share, 2) as creature_type_share,
    round(artifact_type_share, 2) as artifact_type_share,
    round(enchantment_type_share, 2) as enchantment_type_share,
    round(instant_type_share, 2) as instant_type_share,
    round(sorcery_type_share, 2) as sorcery_type_share,
    round(land_type_share, 2) as land_type_share,
    cast(colorless_proportion*100.0 as integer) as percent_colorless_cards,
    cast(mono_card_proportion*100.0 as integer) as percent_monocolored_cards,
    cast(multi_card_proportion*100.0 as integer) as percent_multicolored_cards
from analytics.v_set_design_profile
where set_type = 'expansion'
order by release_year asc, set_name asc;

