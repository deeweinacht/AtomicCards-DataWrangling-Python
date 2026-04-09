-- Identifies the top 10 sets that deviate most from the average set design profile.
-- Ranks by calculating a set_outlier_score by totaling a set's z-scores for a number of key set metrics. 

with global_stats as (
    select
        avg(avg_mana_value) as g_mana,
        avg(avg_complexity_score) as g_complexity,
        avg(creature_type_share) as g_creatures,
        avg(artifact_type_share) as g_artifacts,
        avg(enchantment_type_share) as g_enchantments,
        avg(instant_type_share) as g_instants,
        avg(sorcery_type_share) as g_sorceries,
        avg(land_type_share) as g_lands,
        avg(colorless_proportion) as g_colorless,
        avg(mono_card_proportion) as g_mono,
        avg(multi_card_proportion) as g_multi,
        stddev(avg_mana_value) as g_mana_stdev,
        stddev(avg_complexity_score) as g_complexity_stdev,
        stddev(creature_type_share) as g_creatures_stdev,
        stddev(artifact_type_share) as g_artifacts_stdev,
        stddev(enchantment_type_share) as g_enchantments_stdev,
        stddev(instant_type_share) as g_instants_stdev,
        stddev(sorcery_type_share) as g_sorceries_stdev,
        stddev(land_type_share) as g_lands_stdev,
        stddev(colorless_proportion) as g_colorless_stdev,
        stddev(mono_card_proportion) as g_mono_stdev,
        stddev(multi_card_proportion) as g_multi_stdev
    from analytics.v_set_design_profile
),
z_scores as (
    select
        sdp.set_name,
        abs(sdp.avg_mana_value - g_mana) / g_mana_stdev as mana_z_score,
        abs(sdp.avg_complexity_score - g_complexity) / g_complexity_stdev as complexity_z_score,
        abs(sdp.creature_type_share - g_creatures) / g_creatures_stdev as creature_z_score,
        abs(sdp.artifact_type_share - g_artifacts) / g_artifacts_stdev as artifact_z_score,
        abs(sdp.enchantment_type_share - g_enchantments) / g_enchantments_stdev as enchantment_z_score,
        abs(sdp.instant_type_share - g_instants) / g_instants_stdev as instant_z_score,
        abs(sdp.sorcery_type_share - g_sorceries) / g_sorceries_stdev as sorcery_z_score,
        abs(sdp.land_type_share - g_lands) / g_lands_stdev as land_z_score,
        abs(sdp.colorless_proportion - g_colorless) / g_colorless_stdev as colorless_z_score,
        abs(sdp.mono_card_proportion - g_mono) / g_mono_stdev as mono_z_score,
        abs(sdp.multi_card_proportion - g_multi) / g_multi_stdev as multi_z_score
    from analytics.v_set_design_profile as sdp
    cross join global_stats
    where sdp.set_type in ('expansion', 'core')
)
select
    set_name,
    round((mana_z_score +
        complexity_z_score +
        (creature_z_score + artifact_z_score + enchantment_z_score + instant_z_score + sorcery_z_score + land_z_score)/6.0 +
        (colorless_z_score + mono_z_score + multi_z_score)/3.0
    ), 2) as set_outlier_score,
    round(mana_z_score, 3) as mana_cost_z_score,
    round(complexity_z_score, 3) as complexity_z_score,
    round((creature_z_score + artifact_z_score + enchantment_z_score + instant_z_score + sorcery_z_score + land_z_score)/6.0, 3) as card_types_z_score,
    round((colorless_z_score + mono_z_score + multi_z_score)/3.0, 3) as color_types_z_score
from z_scores
order by set_outlier_score desc
limit 10;