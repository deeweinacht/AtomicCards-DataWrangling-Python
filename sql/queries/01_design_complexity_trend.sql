-- Measures how card complexity has changed over time by tracking yearly first-print design features of cards. 

select
    case
        when first_printing_year < 1995 then '1993-1995'
        when first_printing_year < 2000 then '1996-2000'
        when first_printing_year < 2005 then '2001-2005'
        when first_printing_year < 2010 then '2006-2010'
        when first_printing_year < 2015 then '2011-2015'
        when first_printing_year < 2020 then '2016-2020'
        else '2021-Present'
    end as release_period,
    cast(avg(text_tokens) as integer) as avg_text_tokens,
    round(avg(rulings_count), 2) as avg_rulings_count,
    cast(avg(complexity_score) as integer) as avg_complexity_score,
    cast(avg(cast(is_multi as integer))*100.0 as integer) as percent_multicolored_cards,
    cast(avg(case when face_count > 1 then 100.0 else 0 end) as integer) as percent_multiface_cards
from
    analytics.v_card_design_features
group by
    release_period
order by
    release_period asc;