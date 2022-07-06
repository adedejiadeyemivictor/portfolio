CREATE OR REPLACE PROCEDURE CEVM_ETL.Segment_report_dashboard 
as
   BEGIN
   
   
        BEGIN
            EXECUTE IMMEDIATE 'truncate table CEVM_ETL.Segment_revenue';

            INSERT INTO Segment_revenue


         SELECT AS_OF_DATE, GROUP_DESC, 
         --case when bu_code in ('4','5','6') then ( 
         CASE WHEN SEGMENT_DESC LIKE '%AFFLUENT%' THEN 'AFFLUENT'
WHEN SEGMENT_DESC LIKE '%SME%' THEN 'SME' WHEN SEGMENT_DESC LIKE '%MASS%' THEN 'MASS' 
WHEN SEGMENT_DESC LIKE '%COMMER%' THEN 'COMMERCIAL' ELSE 'OTHERS' 
--END) when bu_code = '7' then 'COMMERCIAL' else 'OTHERS'
 end  SEGMENT_DESC, 
CASE WHEN GROUP_DESC LIKE '%NRG%' THEN 'NORTH' WHEN GROUP_DESC LIKE '%SRG%' THEN 'SOUTH' WHEN GROUP_DESC LIKE '%LWR%' THEN 'LAGOS AND WEST' 
WHEN GROUP_DESC LIKE '%CMB%' OR SEGMENT_DESC LIKE'%COMMERCIAL%' OR BU_CODE = '7' THEN 'COMMERCIAL BANKING' ELSE 'OTHERS' END BUDESC,  NRFF, NET_REVENUE, NII, bu_code FROM (
         select 
         AS_OF_DATE, GROUP_DESC,  case when ak.cust_id=am.cust_id
        then affluent_segment else           segm_desc
             end segment_desc, BUDESC,bu_code, sum( NRFF) NRFF, 
        SUM(NET_REVENUE) NET_REVENUE, SUM(NII) NII FROM (
        select kk.*, case when LL.cust_id is null then KK.bu_desc else  'PSU' END BUDESC from (
        select as_of_date, cust_id,  nrff, net_revenue, net_revenue-nrff nii, b.bu_desc, b.bu_code,
         group_desc,team_desk,desk_desc,
            case  WHEN b.bu_code = '7' then 'COMMERCIAL' WHEN segment_desc LIKE '%AFFLUENT%'
          THEN
             'AFFLUENT'
          WHEN segment_desc LIKE '%MASS%' OR segment_desc IS NULL
          THEN
             'MASS'
          WHEN segment_desc LIKE '%SME%' then 'SME' 
          WHEN SEGMENT_DESC LIKE '%COMMER%' THEN 'COMMERCIAL' else 'Others'
             END
          segm_desc  
            from report.d_ofsaa8_account_view@exadata_lnk a, report.d_business_units_dim@exadata_lnk b, 
            report.d_acct_details_tbl@exadata_lnk c
            where as_of_date between to_date(to_char(sysdate,'YYYY')-1||'01'||'01', 'YYYYMMDD') and-- 
 ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-1)
and as_of_date not between ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-12) and to_date(to_char(sysdate,'YYYY')-1||'12'||'31', 'YYYYMMDD')
--an
            and a.DESK_CODE = b.DESK_CODE
            and a.account_number=c.foracid
            and replace(bu_desc, '&','') in ('LAGOS  WEST REGION','NORTH REGION','SOUTH REGION','COMMERCIAL BANKING')) kk 
            left outer join rsa_psu_accounts ll on kk.cust_id=ll.cust_id) ak
                                          left outer join FBN_2021_Acct_v2 am on ak.cust_id=am.cust_id
            GROUP BY AS_OF_DATE, GROUP_DESC,bu_code,  case when ak.cust_id=am.cust_id
        then affluent_segment else           segm_desc
             end, BUDESC)
             WHERE bu_code IN ('4', '5', '6', '7');
             
             
             commit;

          END;
          
          
          BEGIN
           EXECUTE IMMEDIATE 'truncate table CEVM_ETL.Segment_deposit';

            INSERT INTO Segment_deposit
            SELECT end_DATE, 
            
          --  case when bu_code in ('4','5','6') then ( 
            CASE WHEN segm_desc LIKE '%AFFLUENT%' THEN 'AFFLUENT'
WHEN segm_desc LIKE '%SME%' THEN 'SME' WHEN segm_desc LIKE '%MASS%' THEN 'MASS' 
WHEN segm_desc LIKE '%COMMER%' THEN 'COMMERCIAL' ELSE 'OTHERS' 
--END) when bu_code = '7' then 'COMMERCIAL' else 'OTHERS'
 end  SEGMENT_DESC, 
CASE WHEN GROUP_DESC LIKE '%NRG%' THEN 'NORTH' WHEN GROUP_DESC LIKE '%SRG%' THEN 'SOUTH' WHEN GROUP_DESC LIKE '%LWR%' THEN 'LAGOS AND WEST' 
WHEN GROUP_DESC LIKE '%CMB%' OR SEGM_DESC LIKE '%COMMERCIAL%' THEN 'COMMERCIAL BANKING' ELSE 'OTHERS' END BUDESC, GROUP_DESC, SCHM_CODE, DEPOSIT FROM (
             select end_DATE, case when ak.cust_id=am.cust_id
            then affluent_segment else           segm_desc
             end segm_desc, budesc,bu_code,group_desc,schm_code ,sum(DEPOSIT) DEPOSIT from( 
            select kk.*, case when LL.foracid is null then KK.bu_desc else  'PSU' END BUDESC from (
            select aa.CUST_ID,aa.foracid, aa.end_DATE, bb.bu_desc,group_desc,aa.schm_code,team_desk,bb.bu_code,desk_desc, case  WHEN cc.bu_code = '7' then 'COMMERCIAL' WHEN segment_desc LIKE '%AFFLUENT%' 
          THEN
             'AFFLUENT'
          WHEN segment_desc LIKE '%MASS%' OR segment_desc IS NULL
          THEN
             'MASS'
          WHEN segment_desc LIKE '%SME%' then 'SME'
          WHEN SEGMENT_DESC LIKE '%COMMER%' THEN 'COMMERCIAL' else 'Others'
             END
          segm_desc,
             cls_bal DEPOSIT
            from report.d_acct_balances_tbl@exadata_lnk aa, report.d_business_units_dim@exadata_lnk bb, 
            report.d_acct_details_tbl@exadata_lnk cc
            --where aa.acid = bb.acid
            where aa.DESK_CODE = bb.DESK_CODE
            and aa.foracid=cc.foracid
            and aa.SCHM_TYPE = 'LAA'
            --and aa.END_DATE =(SELECT MAX(END_DATE) FROM report.d_acct_balances_tbl@exadata_lnk)
            /*and trim(PRODUCT) in ('100511Foreign Loans','100561Overdraft -General','100533Motor Vehicle Lease',
            '100521Wholesale Finance Facility','100526Tenored Loans','100524Invoice Finance Facility','100512Cleanline Assets','10052LCY Loans','100525Retail Finance','100522Consumer Finance','10056Overdraft - LCY'
            )*/
            AND  aa.end_DATE  between to_date(to_char(sysdate,'YYYY')-1||'01'||'01', 'YYYYMMDD') and 
             ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-1)
           and aa.end_DATE not between ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-12) and to_date(to_char(sysdate,'YYYY')-1||'12'||'31', 'YYYYMMDD')
            and replace(bu_desc, '&','') in ('LAGOS  WEST REGION','NORTH REGION','SOUTH REGION','COMMERCIAL BANKING')) kk 
            left outer join rsa_psu_accounts ll on kk.foracid=ll.foracid) ak
                                          left outer join FBN_2021_Acct_v2 am on ak.cust_id=am.cust_id
            group by end_DATE, case when ak.cust_id=am.cust_id
          then affluent_segment else           segm_desc
             end, budesc,group_desc,bu_code,schm_code);
             
              commit;
--group by aa.CUST_ID,aa.foracid,aa.END_DATE, bb.bu_desc,group_desc,team_desk,desk_desc
          
          
          END;
          
          
        BEGIN
            EXECUTE IMMEDIATE 'truncate table CEVM_ETL.Segment_customer_aquisition';

             INSERT INTO Segment_customer_aquisition         
             
              SELECT CUST_COUNT, last_day(to_date(yr_month||'-01', 'yy-mm-dd')) yr_month, CASE WHEN GROUP_DESC LIKE '%NRG%' THEN 'NORTH' WHEN GROUP_DESC LIKE '%SRG%' THEN 'SOUTH' WHEN GROUP_DESC LIKE '%LWR%' THEN 'LAGOS AND WEST' 
WHEN (GROUP_DESC LIKE '%CMB%' OR segment_desc LIKE '%COMMERCIAL%') THEN 'COMMERCIAL BANKING' ELSE 'OTHERS' END BUDESC, GROUP_DESC, SCHM_CODE, 
--case when bu_code in ('4','5','6') then ( 
CASE WHEN segment_desc LIKE '%AFFLUENT%' THEN 'AFFLUENT'
WHEN segment_desc LIKE '%SME%' THEN 'SME' 
WHEN segment_desc LIKE '%MASS%' THEN 'MASS' 
WHEN segment_desc LIKE '%COMMER%' THEN 'COMMERCIAL' ELSE 'OTHERS' 
--END) when bu_code = '7' then 'COMMERCIAL' else 'OTHERS' 
end  SEGMENT_DESC FROM (
         select count(distinct ak.cust_id) cust_count, to_char(ACCT_OPN_DATE, 'yy-mm') yr_month,BUDESC,bu_code,
          group_desc,
         schm_code,
        case when ak.cust_id=am.cust_id
        then affluent_segment else segm_desc end segment_desc
         from (
           select kk.*, case when LL.cust_id is null then KK.bu_desc else  'PSU' END BUDESC from (
                SELECT cust_id,
                 ACCT_OPN_DATE,
                bb.bu_desc,bb.bu_code,
                group_desc,
                desk_desc,
                aa.schm_code,
                team_desk,
                segment_desc,
                 cASE
             WHEN aa.bu_code = '7' then 'COMMERCIAL' WHEN segment_desc LIKE '%AFFLUENT%'
          THEN
             'AFFLUENT'
          WHEN segment_desc LIKE '%MASS%' OR segment_desc IS NULL
          THEN
             'MASS' WHEN segment_desc LIKE '%SME%' then 'SME'
          WHEN SEGMENT_DESC LIKE '%COMMER%' THEN 'COMMERCIAL' else 'Others'          
          END
          segm_desc
             FROM report.d_acct_details_tbl@exadata_lnk aa,
              report.d_business_units_dim@exadata_lnk bb
             WHERE     aa.DESK_CODE = bb.DESK_CODE
          and ACCT_OPN_DATE --> '1-JAN-2020'
           between to_date(to_char(sysdate,'YYYY')-1||'01'||'01', 'YYYYMMDD') and 
           ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-1)
          and ACCT_OPN_DATE not between ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-12) and to_date(to_char(sysdate,'YYYY')-1||'12'||'31', 'YYYYMMDD')
              AND REPLACE (bu_desc, '&', '') IN ('LAGOS  WEST REGION',
                                          'NORTH REGION',
                                          'SOUTH REGION',
                                          'COMMERCIAL BANKING')) kk left outer join rsa_psu_accounts ll on kk.cust_id=ll.cust_id) ak
                                          left outer join FBN_2021_Acct_v2 am on ak.cust_id=am.cust_id
                                          group by to_char(ACCT_OPN_DATE, 'yy-mm'),BUDESC,bu_code,
               group_desc,
        case when ak.cust_id=am.cust_id
        then affluent_segment else segm_desc end,schm_code);
         commit;

      END;
        
       BEGIN
        EXECUTE IMMEDIATE 'truncate table CEVM_ETL.Segment_loan';

             INSERT INTO Segment_loan
 select as_of_date, --case when ak.cust_id=am.cust_id
       -- then (CASE WHEN affluent_segment LIKE '%AFFLUENT%' THEN 'AFFLUENT'
--WHEN affluent_segment LIKE '%SME%' THEN 'SME' WHEN affluent_segment LIKE '%MASS%' THEN 'MASS' 
--WHEN affluent_segment LIKE '%COMMER%' THEN 'COMMERCIAL' ELSE 'OTHERS' END)  else           segm_desc end 
segm_desc,  CASE WHEN group_desc LIKE '%NRG%' THEN 'NORTH' WHEN GROUP_DESC LIKE '%SRG%' THEN 'SOUTH' WHEN GROUP_DESC LIKE '%LWR%' THEN 'LAGOS AND WEST' 
WHEN GROUP_DESC LIKE '%CMB%' THEN 'COMMERCIAL BANKING' ELSE 'OTHERS' END BUDESC, 
replace(GROUP_DESC,',','_') GROUP_DESC,sol_id, schm_code, count(distinct account_number) loan_acct_cnt, sum( loan_cls_bal) loan from (
select kk.*, case when LL.foracid is null then KK.bu_desc else  'PSU' END BUDESC from (
select cc.CUST_ID,aa.account_number,cc.sol_id, schm_code,segment_desc,cASE
          WHEN BB.bu_code = '7' then 'COMMERCIAL' WHEN segment_desc LIKE '%AFFL%' OR segment_desc LIKE 'HIGH %'
          THEN
             'AFFLUENT'
          WHEN segment_desc LIKE '%MASS%' OR segment_desc IS NULL
          THEN
             'MASS'
          WHEN segment_desc LIKE '%SME%' then 'SME' else 'Others'
              END segm_desc,
aa.as_of_date, bb.bu_desc, group_desc,desk_desc,team_desk, cur_book_bal loan_cls_bal
from report.d_ofsaa8_account_view@exadata_lnk aa, report.d_business_units_dim@exadata_lnk bb, report.d_acct_details_tbl@exadata_lnk cc
--where aa.acid = bb.acid
where aa.DESK_CODE = bb.DESK_CODE
and aa.account_number=cc.foracid
and aa.as_of_date between to_date(to_char(sysdate,'YYYY')-1||'01'||'01', 'YYYYMMDD') and-- 
 ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-1)
and aa.as_of_date not between ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-12) and to_date(to_char(sysdate,'YYYY')-1||'12'||'31', 'YYYYMMDD')
--and aa.SCHM_TYPE = 'LAA'
and trim(PRODUCT) in ('100511Foreign Loans','100561Overdraft -General','100533Motor Vehicle Lease',
'100521Wholesale Finance Facility','100526Tenored Loans','100524Invoice Finance Facility','100512Cleanline Assets','10052LCY Loans','100525Retail Finance','100522Consumer Finance','10056Overdraft - LCY'
)
--and MATURITY_DATE > sysdate-117
and replace(bu_desc, '&','') in ('LAGOS  WEST REGION','NORTH REGION','SOUTH REGION','COMMERCIAL BANKING')) kk left outer join 
rsa_psu_accounts ll on kk.account_number=ll.foracid) ak
                                          left outer join FBN_2021_Acct_v2 am on ak.cust_id=am.cust_id
group by as_of_date,segm_desc, --case when ak.cust_id=am.cust_id then affluent_segment else    segm_desc end,  
BUDESC, sol_id, schm_code,
GROUP_DESC;

 commit;
--group by aa.CUST_ID,aa.foracid,aa.END_DATE, bb.bu_desc,group_desc,team_desk,desk_desc;
        
        
        END;
     
    COMMIT;
   
   
    
   END;
/
