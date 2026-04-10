/*
Identifies mechanics that are most concentrated in a single mono-color population by 
comparing each mechanic’s dominant mono-color share against its runner-up share.
*/

with mechanic_color_counts as (
    select
        mechanic,
        count(card_id) as mono_total_count,
        count(distinct case when is_white then card_id end) as white_count,
        count(distinct case when is_blue  then card_id end) as blue_count,
        count(distinct case when is_black then card_id end) as black_count,
        count(distinct case when is_red   then card_id end) as red_count,
        count(distinct case when is_green then card_id end) as green_count
    from analytics.v_color_mechanics
    where color_category = 'Monocolored'
    group by 1
    having count(distinct card_id) >= 50 -- Focus on mechanics with at least 50 instances to ensure meaningful analysis
),
color_shares as (
    select mechanic, mono_total_count, 'White' as color, (white_count * 1.0 / mono_total_count) as share, white_count as instances from mechanic_color_counts union all
    select mechanic, mono_total_count, 'Blue',  (blue_count  * 1.0 / mono_total_count), blue_count  from mechanic_color_counts union all
    select mechanic, mono_total_count, 'Black', (black_count * 1.0 / mono_total_count), black_count from mechanic_color_counts union all
    select mechanic, mono_total_count, 'Red',   (red_count   * 1.0 / mono_total_count), red_count   from mechanic_color_counts union all
    select mechanic, mono_total_count, 'Green', (green_count * 1.0 / mono_total_count), green_count from mechanic_color_counts
),
ranked_shares as (
    select 
        mechanic,
        color,
        mono_total_count,
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
    mono_total_count as mono_colored_cards_with_mechanic,
    (select count(distinct card_id) from analytics.v_color_mechanics where mechanic = ranked_shares.mechanic) as total_cards_with_mechanic
from ranked_shares
where row_num = 1
order by concentration_gap desc, total_cards_with_mechanic desc;