-- Identifies keyword mechanics that are most strongly concentrated in a single mono-color population, 
-- highlighting which mechanics appear to be the clearest signals of stable color identity.

with mechanic_color_counts as (
    select
        mechanic,
        count(card_id) as mono_total_c,
        count(distinct case when is_white then card_id end) as white_c,
        count(distinct case when is_blue  then card_id end) as blue_c,
        count(distinct case when is_black then card_id end) as black_c,
        count(distinct case when is_red   then card_id end) as red_c,
        count(distinct case when is_green then card_id end) as green_c
    from analytics.v_color_mechanics
    where color_category = 'Monocolored'
    group by 1
    having count(distinct card_id) >= 50 -- Focus on mechanics with at least 50 instances to ensure meaningful analysis
),
color_shares as (
    select mechanic, mono_total_c, 'White' as color, (white_c * 1.0 / mono_total_c) as share, white_c as instances from mechanic_color_counts union all
    select mechanic, mono_total_c, 'Blue',  (blue_c  * 1.0 / mono_total_c), blue_c  from mechanic_color_counts union all
    select mechanic, mono_total_c, 'Black', (black_c * 1.0 / mono_total_c), black_c from mechanic_color_counts union all
    select mechanic, mono_total_c, 'Red',   (red_c   * 1.0 / mono_total_c), red_c   from mechanic_color_counts union all
    select mechanic, mono_total_c, 'Green', (green_c * 1.0 / mono_total_c), green_c from mechanic_color_counts
),
ranked_shares as (
    select 
        mechanic,
        color,
        mono_total_c,
        coalesce(share, 0) as share,
        coalesce(lead(color) over (partition by mechanic order by share desc, color asc), 'Unknown') as runner_up_color,
        coalesce(lead(share) over (partition by mechanic order by share desc, color asc), 0) as runner_up_share,
        row_number() over (partition by mechanic order by share desc, color asc) as row_num
    from color_shares
)
select 
    mechanic,
    color as dominant_color,
    round(share, 3) as dominant_concentration,
    runner_up_color,
    round(runner_up_share, 3) as runner_up_concentration,
    round((dominant_concentration - runner_up_concentration), 3) as concentration_gap,
    mono_total_c as mono_colored_card_instances,
    (select count(distinct card_id) from analytics.v_color_mechanics where mechanic = ranked_shares.mechanic) as total_card_instances
from ranked_shares
where row_num = 1
order by concentration_gap desc;