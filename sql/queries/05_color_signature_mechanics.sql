-- Determine what mechanic is most associated with each color to ensure design consistency.
-- For each color determine the mechanic has the highest appearance ratio for mono-colored cards of that color.

with mechanic_color_counts as (
    select
        mechanic,
        count(distinct card_id) as total_instances,
        count(distinct case when is_white and color_category = 'Monocolored' then card_id end) as white_c,
        count(distinct case when is_blue  and color_category = 'Monocolored' then card_id end) as blue_c,
        count(distinct case when is_black and color_category = 'Monocolored' then card_id end) as black_c,
        count(distinct case when is_red   and color_category = 'Monocolored' then card_id end) as red_c,
        count(distinct case when is_green and color_category = 'Monocolored' then card_id end) as green_c
    from analytics.v_color_mechanics
    group by 1
    having count(distinct card_id) >= 100 -- Focus on mechanics with at least 100 instances to ensure meaningful analysis
),
color_shares as (
    -- Calculate what % of the total each color owns
    select mechanic, total_instances, 'White' as color, (white_c * 1.0 / total_instances) as share, white_c as instances from mechanic_color_counts union all
    select mechanic, total_instances, 'Blue',  (blue_c  * 1.0 / total_instances), blue_c  from mechanic_color_counts union all
    select mechanic, total_instances, 'Black', (black_c * 1.0 / total_instances), black_c from mechanic_color_counts union all
    select mechanic, total_instances, 'Red',   (red_c   * 1.0 / total_instances), red_c   from mechanic_color_counts union all
    select mechanic, total_instances, 'Green', (green_c * 1.0 / total_instances), green_c from mechanic_color_counts
),
ranked_shares as (
    select 
        *,
        row_number() over (partition by color order by share desc) as rank
    from color_shares
)
select 
    color, 
    mechanic as signature_mechanic
from ranked_shares
where rank = 1
order by color;