select * from (select FORACID, NET_REVENUE, AS_OF_DATE from psu_revenue
)
pivot 
(
   Min(NET_REVENUE) for as_of_date in ('31/jan/2021','28/feb/2021', '31/mar/2021', '30/apr/2021',
   '31/may/2021', '30/jun/2021', '31/jul/2021', '31/aug/2021', '30/sep/2021' )
)
order by foracid



