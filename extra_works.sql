select ne.ticker, ne.er_date, ne.zack_rank, hist.*
from next_er_date  ne
inner join hist_er hist on hist.ticker = ne.ticker
where ne.zack_rank like 'Strong%';



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
and temp.zack_rank like 'Strong%'
order by temp.er_date, elite.ticker, elite.date;


#lite version
select distinct ticker, er_date, zack_rank, accurate_pct, avg_change from (
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
and temp.zack_rank like 'Strong%'
order by temp.er_date, elite.ticker, elite.date
) as a;




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

