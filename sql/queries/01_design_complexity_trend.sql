/*
Measures how card complexity has changed over time by tracking yearly first-print design features of cards. 
*/

select
    analytics_release_period(first_printing_year) as release_period,
    cast(avg(text_tokens) as integer) as avg_text_tokens_per_card,
    round(avg(rulings_count), 2) as avg_rulings_count_per_card,
    cast(avg(design_complexity_score) as integer) as avg_design_complexity_score, --heuristic design-complexity proxy that weights rulings more heavily than printed text density
    cast(avg(cast(is_multi as integer))*100.0 as integer) as percent_multicolored_cards,
    cast(avg(case when face_count > 1 then 100.0 else 0 end) as integer) as percent_multiface_cards
from
    analytics.v_card_design_features
group by
    analytics_release_period_order(first_printing_year),
    release_period
order by
    release_period asc;