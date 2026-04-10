/*
Macro for creating time buckets for analysis
*/

create or replace macro analytics_release_period(year_value) as
case
    when year_value < 1996 then '1993-1995'
    when year_value < 2001 then '1996-2000'
    when year_value < 2006 then '2001-2005'
    when year_value < 2011 then '2006-2010'
    when year_value < 2016 then '2011-2015'
    when year_value < 2021 then '2016-2020'
    else '2021-Present'
end;

create or replace macro analytics_release_period_order(year_value) as
case
    when year_value < 1996 then 1
    when year_value < 2001 then 2
    when year_value < 2006 then 3
    when year_value < 2011 then 4
    when year_value < 2016 then 5
    when year_value < 2021 then 6
    else 7
end;