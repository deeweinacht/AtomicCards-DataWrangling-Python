/*
Measures how common the top 20 keyword mechanics are within each mono-color card population,
highlighting mechanics that function as recurring practical signals of color identity.
*/

WITH mono_color_totals AS (
    -- Calculate the total population of each color once
    SELECT
        count(distinct card_id) FILTER (WHERE is_white) as total_white,
        count(distinct card_id) FILTER (WHERE is_blue)  as total_blue,
        count(distinct card_id) FILTER (WHERE is_black) as total_black,
        count(distinct card_id) FILTER (WHERE is_red)   as total_red,
        count(distinct card_id) FILTER (WHERE is_green) as total_green
    FROM analytics.v_card_design_features
    WHERE color_count = 1
)

select
    mechanic,
    round(count(distinct case when is_white then card_id end) * 100.0 / nullif(max(mct.total_white), 0), 1) as percent_mono_white_cards,
    round(count(distinct case when is_blue  then card_id end) * 100.0 / nullif(max(mct.total_blue), 0), 1) as percent_mono_blue_cards,
    round(count(distinct case when is_black then card_id end) * 100.0 / nullif(max(mct.total_black), 0), 1) as percent_mono_black_cards,
    round(count(distinct case when is_red   then card_id end) * 100.0 / nullif(max(mct.total_red), 0), 1) as percent_mono_red_cards,
    round(count(distinct case when is_green then card_id end) * 100.0 / nullif(max(mct.total_green), 0), 1) as percent_mono_green_cards,
    count(distinct card_id) as mono_colored_cards_with_mechanic
from
    analytics.v_color_mechanics as cm
cross join 
    mono_color_totals as mct
where
    color_category = 'Monocolored'
group by mechanic
order by mono_colored_cards_with_mechanic desc, mechanic asc
limit 20;