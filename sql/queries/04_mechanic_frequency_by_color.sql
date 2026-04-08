-- Measures how strongly selected mechanics align with each of the colors (or colorless).
-- For the top 15 most common keyword mechanics, we report the percentage of mono-colored cards
-- that have that mechanic.

select
    mechanic,
    round(avg(case when is_white and color_category = 'Monocolored' then 100.0 else 0 end), 1) as white_percentage,
    round(avg(case when is_blue  and color_category = 'Monocolored' then 100.0 else 0 end), 1) as blue_percentage,
    round(avg(case when is_black and color_category = 'Monocolored' then 100.0 else 0 end), 1) as black_percentage,
    round(avg(case when is_red   and color_category = 'Monocolored' then 100.0 else 0 end), 1) as red_percentage,
    round(avg(case when is_green and color_category = 'Monocolored' then 100.0 else 0 end), 1) as green_percentage,
    count(case when color_category = 'Monocolored' then card_id end) as mono_colored_card_count,
    count(card_id) as all_card_count
from
    analytics.v_color_mechanics
group by mechanic
order by all_card_count desc
limit 15;