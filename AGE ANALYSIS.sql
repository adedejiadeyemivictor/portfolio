create table Customer_info_tmp as
select distinct cust_id,cust_sex,date_of_birth,CUST_MARITAL_STATUS   
from report.d_cust_details_tbl@exadata_lnk

create table Customer_info_v2 as
SELECT cust_id,cust_sex,date_of_birth,CUST_MARITAL_STATUS Marital_status,
floor((SYSDATE - date_of_birth)/365.25) AGE
FROM Customer_info_tmp


create table Customer_info_v3 as
select cust_id,cust_sex,date_of_birth,age, Marital_status,
case
when age between '0' and '17' then '0_17 Yrs'
when age between '18' and '30' then '18_30 Yrs'
when age between '31' and '45' then '31_45 Yrs'
when age between '46' and '60' then '46_60 Yrs'
when age >'60' then '60 and above'
else 'Unknown' end age_band,
case
when age <= 24 then 'GenZ'
when age <= 40 then 'Millennials'
when age <= 56 then 'GenX'
when age <= 73 then 'Baby Boomers'
when age >= 74 then 'Silent Generation'
else 'Unknown' end Generation
 from Customer_info_v2
 
 
create table Aging_analysis_details as
select distinct  a.cust_id,foracid, acct_name, cust_sex,
ACCT_CRNCY_CODE Curency_type, schm_type, bu_code, group_code, Team_code, Desk_code,
acct_cls_date,segment_desc, acct_status,
date_of_birth,age, Marital_status, age_band, generation
 from Customer_info_v3 a left outer join report.d_acct_details_tbl@exadata_lnk b
 on a.cust_id = b.cust_id
 

create table Aging_analysis_product as
select cust_id, foracid, DATE_OF_BIRTH acct_status, age_band, generation, 
segment_desc,age,  fn_has_first_mobile(foracid) First_Mobile,
fn_has_ussd (foracid) USSD, fn_has_card (foracid) Active_Card,
fn_has_first_online (foracid) Online_Banking  from Aging_analysis_details
where schm_type in ('SBA', 'ODA', 'TDA')


create table Aging_analysis_turnover as
select age_band, generation, segment_desc, cust_sex, a.schm_type,
SUM(Dr_turnover) Total_Dr_turnover,
avg(Dr_turnover) avr_Dr_turnover,
SUM(cr_turnover) Total_cr_turnover,
avg(cr_turnover) avr_cr_turnover,
avg(avg_crbal) avr_crbal
from Aging_analysis_details a left outer join REPORT.MIS_BAL_TBL_ONE_MNTH@exadata_lnk b
on a.cust_id=b.cust_id
where return_date >= ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-12)
and A.schm_type in ('SBA', 'ODA', 'TDA')
group by age_band, generation, segment_desc, cust_sex, a.schm_type;



create table Aging_analysis_revenue as
select age_band, generation, segment_desc, bu_code, cust_sex, schm_type, SUM(NRFF) NRFF, avg (nrff) Avg_nrff, SUM(Revenue) revenue, avg(Revenue) avr_revenue from
(select distinct as_of_date, a.foracid, age_band, generation, segment_desc, bu_code, cust_sex, a.schm_type,
 NRFF, net_revenue revenue from Aging_analysis_details a left outer join report.d_ofsaa8_account_view@exadata_lnk b
on a.cust_id=b.customer_id
where as_of_date >= ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-12)
and A.schm_type in ('SBA', 'ODA', 'TDA'))
group by age_band, generation, segment_desc, bu_code, cust_sex, schm_type;






update Aging_analysis_product
set USSD = 1
where USSD = 'Y';

update Aging_analysis_product
set USSD = 0
where USSD = 'N';

update Aging_analysis_product
set ACTIVE_CARD = 1
where ACTIVE_CARD = 'Y';

update Aging_analysis_product
set ACTIVE_CARD = 0
where ACTIVE_CARD = 'N';

update Aging_analysis_product
set ONLINE_BANKING = 1
where ONLINE_BANKING = 'Y';

update Aging_analysis_product
set ONLINE_BANKING = 0
where ONLINE_BANKING = 'N';
 
 
 
 

 
