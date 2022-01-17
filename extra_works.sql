SELECT * FROM awesome.hist_er_elite;
#trend accurate percent
select aa.ticker, sum(aa.same_direction)/count(*) as direction_correct
from (
select ticker, 
case
when epssurprisepct < 0 and price_change >0 then 0
when epssurprisepct > 0 and price_change <0 then 0
else 1 end as same_direction
from awesome.hist_er_elite
) aa
group by aa.ticker
;


#big table option recommendation
select elite.*, trend.accurate_pct, avg_change.avg_change, temp.er_date, temp.zack_rank
from awesome.hist_er_elite elite
left join (
select aa.ticker, sum(aa.same_direction)/count(*) as accurate_pct
from (
select ticker, 
case
when epssurprisepct < 0 and price_change >0 then 0
when epssurprisepct > 0 and price_change <0 then 0
else 1 end as same_direction
from awesome.hist_er_elite
) aa
group by aa.ticker
) trend on trend.ticker = elite.ticker
left join (
select ticker, avg(abs(price_change)) as avg_change
from awesome.hist_er_elite
group by ticker) avg_change on avg_change.ticker = elite.ticker
left join awesome.next_er_date temp on temp.ticker = elite.ticker
where temp.er_date is not null and avg_change.avg_change >= 0.1
and temp.zack_rank in (1, 5)
order by temp.er_date, elite.ticker, elite.date;


#lite version
select distinct ticker, er_date, zack_rank, accurate_pct, avg_change, sector from (
select elite.ticker, trend.accurate_pct, avg_change.avg_change, temp.er_date, temp.zack_rank, com.sector
from awesome.hist_er_elite elite
left join (
select aa.ticker, sum(aa.same_direction)/count(*) as accurate_pct
from (
select ticker, 
case
when epssurprisepct < 0 and price_change >0 then 0
when epssurprisepct > 0 and price_change <0 then 0
else 1 end as same_direction
from awesome.hist_er_elite
) aa
group by aa.ticker
) trend on trend.ticker = elite.ticker
left join (
select ticker, avg(abs(price_change)) as avg_change
from awesome.hist_er_elite
group by ticker) avg_change on avg_change.ticker = elite.ticker
left join awesome.next_er_date temp on temp.ticker = elite.ticker
left join awesome.company_info com on com.Symbol = elite.ticker
where temp.er_date is not null and avg_change.avg_change >= 0.1
and temp.zack_rank in (1, 5)
order by temp.er_date, elite.ticker, elite.date
) as a order by er_date;


#zack rank er return tracking
select sector, avg(increase_pct), min(increase_pct), max(increase_pct), count(*)from (
select aa.*, (aa.nextday_close_price-aa.current_close_price)/aa.current_close_price as increase_pct, com.sector,
case when  (aa.nextday_close_price-aa.current_close_price)/aa.current_close_price> 0.05 then 1
	 when  (aa.nextday_close_price-aa.current_close_price)/aa.current_close_price> 0 then 0.5
     else -1 end as current_rank from
(
select *,  rank() over (partition by ticker order by date desc) as date_order 
from awesome.hist_er_elite
) aa
left join awesome.company_info com on com.Symbol = aa.ticker
where aa.date_order = 1 and aa.zacks_rank like 'Strong%') bb group by sector
;



#portfolio analysis
select p.*, hist.date as er_date from awesome.option_portfolio p
inner join awesome.hist_er_elite hist on p.ticker = hist.ticker and hist.date > '2020-2-1'
UNION
select p.*, n.er_date from awesome.option_portfolio p
inner join awesome.next_er_date n on p.ticker = n.ticker and n.er_date < '2020-3-20'
union
select *, curdate() er_date
from awesome.option_portfolio where ticker not in (
select distinct ticker
from awesome.hist_er_elite where date>'2020-2-1'
union
select distinct ticker
from awesome.next_er_date where er_date < '2020-3-20');


select bb.er_date, avg(aa.nextday_volume)
from awesome.hist_er_elite aa
left join awesome.next_er_date bb on aa.ticker= bb.ticker
where bb.er_date not in (select * from (
select b.er_date
from awesome.hist_er_elite a
left join awesome.next_er_date b on a.ticker = b.ticker
where b.er_date is not null
group by b.er_date
order by sum(a.nextday_volume) desc
limit 3) t )
group by bb.er_date;

