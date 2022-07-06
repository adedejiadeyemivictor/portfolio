CREATE OR REPLACE PACKAGE BODY CEVM_ETL.RSAC_
AS
   PROCEDURE LOAD_FMOB
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table CEVM_ETL.STG_CHANNEL_FMOB';

      INSERT INTO CEVM_ETL.STG_CHANNEL_FMOB
         SELECT a.cust_id, 'Y' has_first_mobile, b.foracid
           FROM REPORT.FBNMOB_USERS@exadata_lnk a,
                report.d_acct_details_tbl@exadata_lnk b
          WHERE a.cust_id = b.cust_id;

      COMMIT;
   END;

   PROCEDURE LOAD_ONLINE
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table CEVM_ETL.STG_CHANNEL_FONLINE';

      INSERT INTO CEVM_ETL.STG_CHANNEL_FONLINE
         SELECT A.CUST_ID, 'Y' HAS_FIRST_ONLINE, FORACID
           FROM REPORT.EBIZ_ONLINEBANKING_REG@EXADATA_LNK A,
                REPORT.D_ACCT_DETAILS_TBL@EXADATA_LNK B
          WHERE A.CUST_ID = B.CUST_ID;

      COMMIT;
   END;


   PROCEDURE LOAD_USSD
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table CEVM_ETL.STG_CHANNEL_USSD';

      INSERT INTO CEVM_ETL.STG_CHANNEL_USSD
         SELECT CUST_ID, 'Y' USSD_REG_DATE, FORACID
           FROM report.d_acct_details_tbl@EXADATA_LNK
          WHERE USSD_REG_DATE IS NOT NULL;

      COMMIT;
   END;

   PROCEDURE LOAD_LOAN
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table CEVM_ETL.STG_CHANNEL_LOAN';

      INSERT INTO CEVM_ETL.STG_CHANNEL_LOAN
         SELECT CUST_ID,
                'Y' has_loan,
                FORACID,
                operating_acct,
                schm_type
           FROM report.d_acct_balances_tbl@EXADATA_LNK
          WHERE     end_date =
                       (SELECT MAX (end_date)
                          FROM report.d_acct_balances_tbl@EXADATA_LNK)
                AND is_credit = '1';

      COMMIT;
   END;

   PROCEDURE LOAD_CARDS
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table CEVM_ETL.STG_CHANNEL_CARDS';

      INSERT INTO CEVM_ETL.STG_CHANNEL_CARDS
         SELECT X.*, 'Y' HAS_CARD
           FROM (  SELECT CUST_ID, FORACID, COUNT (*) NUMBER_OF_CARDS
                     FROM (SELECT PC_CUSTOMER_ID CUST_ID,
                                  TO_CHAR (PC_ACCOUNT_ID) FORACID
                             FROM REPORT.EBIZ_POSTCARD_DATA@exadata_lnk
                            WHERE     pc_expiry_date >=
                                         TO_CHAR (SYSDATE, 'YYMM')
                                  AND pc_card_status = '1'
                           UNION ALL
                           SELECT CUST_ID, TO_CHAR (VC_ACCOUNTNUMBER) FORACID
                             FROM REPORT.EBIZ_VIACARD_DATA@exadata_lnk A,
                                  REPORT.D_ACCT_DETAILS_TBL@EXADATA_LNK B
                            WHERE     VC_ACCOUNTNUMBER = FORACID
                                  AND A.vc_expirydate >=
                                         TO_CHAR (SYSDATE, 'YYMM')
                                  AND UPPER (vc_hold_rsp_code) = 'LINKED')
                 GROUP BY CUST_ID, FORACID) X;

      COMMIT;
   END;

   PROCEDURE load_turnover
   AS
   BEGIN
     --- EXECUTE IMMEDIATE 'truncate table stg_rsac_turnover';

      INSERT INTO stg_rsac_turnover
           SELECT cust_id,
                  foracid,
                  SUM (cr_turnover) trunover,
                  return_date
             FROM REPORT.MIS_BAL_TBL_ONE_MNTH@exadata_lnk,
                  (SELECT account_number FROM RSAC_DIGITAL_ACCTS_TBL
                   UNION
                   SELECT foracid account_number FROM rsac_rsa_accounts)
            WHERE foracid = account_number AND return_date =ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-1)
         GROUP BY cust_id, foracid, return_date;

      COMMIT;
   END;

   PROCEDURE load_netrev
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table stg_rsac_NET_REV';

      INSERT INTO stg_rsac_NET_REV
           SELECT A.ACCOUNT_NUMBER,
                  SUM (net_revenue) NET_REVENUE,
                  SUM (fee_income),
                  SUM (comm_income),
                  as_of_date
             FROM REPORT.D_OFSAA8_ACCOUNT_VIEW@exadata_lnk A,
                  (SELECT account_number FROM RSAC_DIGITAL_ACCTS_TBL
                   UNION
                   SELECT foracid account_number FROM rsac_rsa_accounts) B
            WHERE     A.ACCOUNT_NUMBER = B.account_number
                  AND AS_OF_date > ADD_MONTHS(LAST_DAY(TRUNC(SYSDATE)),-6)
         GROUP BY A.ACCOUNT_NUMBER, as_of_date;

      COMMIT;
   END;

   PROCEDURE load_balances
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table stg_rsac_balances';

      INSERT INTO stg_rsac_balances
           SELECT A.foracid, CEVM_ETL.GET_EAB_BAL (acid, SYSDATE) balance
             FROM REPORT.d_acct_details_tbl@exadata_lnk A,
                  (SELECT account_number FROM RSAC_DIGITAL_ACCTS_TBL
                   UNION
                   SELECT foracid account_number FROM rsac_rsa_accounts) B
            WHERE A.foracid = B.account_number
         GROUP BY A.foracid, acid;

      COMMIT;
   END;

   FUNCTION get_balance (v_foracid IN VARCHAR2)
      RETURN NUMBER
   AS
      v_balance   NUMBER;
   BEGIN
      SELECT SUM (balance)
        INTO v_balance
        FROM stg_rsac_balances
       WHERE foracid = v_foracid;

      RETURN v_balance;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN 0;
   END;

   FUNCTION get_netrev (v_foracid IN VARCHAR2)
      RETURN NUMBER
   AS
      v_netrev   NUMBER;
   BEGIN
      SELECT SUM (net_revenue)
        INTO v_netrev
        FROM STG_RSAC_NET_REV
       WHERE account_number = v_foracid;

      RETURN v_netrev;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN 0;
   END;

   FUNCTION get_feeincome (v_foracid IN VARCHAR2)
      RETURN NUMBER
   AS
      v_fee   NUMBER;
   BEGIN
      SELECT SUM (fee_income)
        INTO v_fee
        FROM STG_RSAC_NET_REV
       WHERE account_number = v_foracid;

      RETURN v_fee;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN 0;
   END;

   FUNCTION get_turnover (v_foracid IN VARCHAR2)
      RETURN NUMBER
   AS
      v_turnover   NUMBER;
   BEGIN
      SELECT SUM (trunover)
        INTO v_turnover
        FROM STG_RSAC_TURNOVER
       WHERE foracid = v_foracid;

      RETURN v_turnover;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN 0;
   END;

   PROCEDURE refresh_conversion_tbl
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table rsac_conversion_tbl';

      INSERT INTO rsac_conversion_tbl
         SELECT foracid,
                clr_bal_amt,
                frez_code,
                schm_code,
                dsa_staff_no
           FROM tbaadm.gam@ffbnffrdb, CEVM_ETL.RSAC_DIGITAL_ACCTS_TBL
          WHERE account_number = foracid AND clr_bal_amt > 0;

      COMMIT;
   END;

   PROCEDURE refesh_rsac_dim
   AS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table tmp_channels_acct';

      INSERT INTO tmp_channels_acct
         SELECT DISTINCT foracid,
                         b.cust_id,
                         acct_name,
                         a.sol_id,
                         channel_code,
                         b.acct_opn_date,
                         clr_bal_amt,
                         frez_code,
                         frez_reason_code,
                         b.last_tran_date
           FROM custom.FI_ACCT_CHANNEL@ffbnffrdb a
                JOIN tbaadm.gam@ffbnffrdb b USING (foracid)
          WHERE     TRIM (frez_code) IS NOT NULL
                AND b.del_flg = 'N'
                AND b.entity_cre_flg = 'Y'
                and acct_opn_date>=sysdate -180
                and acct_crncy_code = 'NGN'
                and schm_type = 'SBA';


      COMMIT;

      EXECUTE IMMEDIATE 'truncate table tmp_channels_acct2';

      INSERT INTO tmp_channels_acct2
         SELECT x.*,floor(dbms_random.value(4,7)) BU_CODE -- Y.BU_CODE
           FROM (SELECT p.*, Q.MOBILENO, p.sol_id || '75' desk_code
                   FROM tmp_channels_acct p
                        LEFT OUTER JOIN REPORT.EBIZ_ALERT_SETUP@exadata_lnk q
                           ON (foracid = accountno)) x;
                           --x,
                --report.d_business_units_dim@exadata_lnk y
          --WHERE x.desk_code = y.desk_code;

      COMMIT;
   END;
END;
/
