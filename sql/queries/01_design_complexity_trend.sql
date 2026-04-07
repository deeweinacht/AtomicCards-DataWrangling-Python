select
    first_printing_year as release_year,
    avg(complexity_score) as avg_complexity_score,
    avg(color_count) as avg_color_count,
    avg(face_count) as avg_face_count
from
    analytics.v_card_design_features
group by
    release_year
order by
    release_year asc;