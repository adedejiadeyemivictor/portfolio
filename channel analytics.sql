
CREATE TABLE PRODUCT_ANALYTICS AS
select cust_id, case when mobile_banking >=1 and internetbanking >=1 and ussd >= 1 then '3 PRODUCT'
WHEN (mobile_banking >=1 and internetbanking >=1) THEN 'MB AND IB' WHEN
(internetbanking >=1 and ussd >= 1) THEN 'IB AND USSD' 
WHEN (mobile_banking >=1 and USSD >=1) THEN 'MB AND USSD' WHEN mobile_banking >=1 THEN 'MB'
WHEN internetbanking >=1 THEN 'IB' WHEN USSD >=1 THEN 'USSD' ELSE 'NO DIGITAL' END CHANNEL, 
PRODUCT_CNT, DIGITAL_CHANNEL from product_table;


CREATE TABLE PRODUCT_ANALYTICS_TMP1 AS
SELECT distinct a.cust_id, c.bu_desc, c.group_desc, a.CHANNEL, 
a.PRODUCT_CNT, a.DIGITAL_CHANNEL  FROM PRODUCT_ANALYTICS a, report.d_acct_details_tbl@exadata_lnk b,
report.d_business_units_dim@exadata_lnk c
where A.CUST_ID = b.cust_id
and b.desk_code = c.desk_code
and c.bu_code in ('4', '5', '6');


truncate table PRODUCT_ANALYTICS;

drop table PRODUCT_ANALYTICS;


select count (distinct cust_id) from PRODUCT_ANALYTICS_TMP

select channel, bu_desc, count (distinct cust_id) from PRODUCT_ANALYTICS_TMP1
group by channel, bu_desc


select count(distinct cust_id), avg(revenue) AVG_REV, bu_desc, GROUP_DESC,channel,
case when digital_channel = 0 then 'NO CHANNEL' ELSE 'CHANNEL' END NO_or_YES,
CASE when digital_channel = 0 THEN 'NO CHANNEL'
when digital_channel = 1 THEN 'SINGLE CHANNEL' ELSE 'MULTIPLE CHANNELS' END CHANNEL_BREAKDOWN
from PRODUCT_ANALYTICS_TMP_NR
GROUP BY bu_desc, 
case when digital_channel = 0 then 'NO CHANNEL' ELSE 'CHANNEL' END,
CASE when digital_channel = 0 THEN 'NO CHANNEL'
when digital_channel = 1 THEN 'SINGLE CHANNEL' ELSE 'MULTIPLE CHANNELS' END, GROUP_DESC, channel


select A.CUST_ID, CUST_NAME, BU_DESC, GROUP_DESC, PHONE1, EMAIL_ID1, PRIMARY_SOL_ID
from PRODUCT_ANALYTICS_TMP_NR A, 
report.d_CUST_details_tbl@exadata_lnk B, report.d_acct_details_tbl@exadata_lnk C
where channel = 'NO DIGITAL'
AND DIGITAL_CHANNEL = 0
AND A.CUST_ID = B.CUST_ID
AND A.CUST_ID = C.CUST_ID
AND ACCT_STATUS = 'A'




SELECT * FROM report.d_CUST_details_tbl@exadata_lnk





select * from PRODUCT_ANALYTICS_TMP_NR