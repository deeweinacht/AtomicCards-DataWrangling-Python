-- Measures how strongly selected mechanics align with colors by calculating how often each mechanic appears in each color.

with top_mechanics as (
    select
        mechanic,
        count(mechanic) as mechanic_card_count
    from analytics.v_color_mechanics
    group by mechanic
    order by mechanic_card_count desc
    limit 15
)
select
    tm.mechanic as mechanic,
    count(case when mech.is_white then card_id end) as white_count,
    count(case when mech.is_blue then card_id end) as blue_count,
    count(case when mech.is_black then card_id end) as black_count,
    count(case when mech.is_red then card_id end) as red_count,
    count(case when mech.is_green then card_id end) as green_count,
    count(distinct case when mech.color_category = 'Colorless' then card_id end) as colorless_count,
    count(card_id) as all_card_count
from
    top_mechanics as tm
join analytics.v_color_mechanics as mech
    on mech.mechanic = tm.mechanic
group by tm.mechanic, tm.mechanic_card_count
order by tm.mechanic_card_count desc;