/* Formatted on 7/5/2022 12:49:07 PM (QP5 v5.115.810.9015) */
CREATE OR REPLACE PROCEDURE CEVM_ETL.retail_loan_assignment (
   --  d_START                IN     DATE DEFAULT NULL,
   --d_end                  IN     DATE DEFAULT NULL,
   n_NUMBER_OF_ACCOUNTS   IN NUMBER DEFAULT NULL ,
   v_STAFFS               IN VARCHAR2 DEFAULT NULL ,
   v_LOAN_TYPE            IN VARCHAR2 DEFAULT NULL
--  v_STATUS                  OUT VARCHAR2
)
AS
   v_job   INTEGER;
   x       NUMBER;
BEGIN
   INSERT INTO RETAIL_CR_UNIT_STAFF_ACCOUNT_ASSIGNED (SOL_ID,
                                                      LOAN_REF,
                                                      CUST_AC_NO,
                                                      CUST_ID,
                                                      PHONE,
                                                      DPD,
                                                      LIMIT_EXPIRY,
                                                      SANCT_LIM,
                                                      STATUS,
                                                      CREATED_ON,
                                                      CURRENCY,
                                                      LOAN_BAL,
                                                      LIEN_AMOUNT,
                                                      SCHM_TYPE,
                                                      BALANCE,
                                                      RM_CODE,
                                                      ACCT_NAME,
                                                      date_assigned,
                                                      ASSIGNED_STAFF, account_type
                                                      )
      SELECT   SOL_ID,
               LOAN_REF,
               CUST_AC_NO,
               CUST_ID,
               PHONE,
               DPD,
               LIMIT_EXPIRY,
               SANCT_LIM,
               STATUS,
               CREATED_ON,
               CURRENCY,
               LOAN_BAL,
               LIEN_AMOUNT,
               SCHM_TYPE,
               BALANCE,
               RM_CODE,
               ACCT_NAME,
               trunc(SYSDATE),
               v_STAFFS, 
               account_type
        FROM   (SELECT   b.*, DENSE_RANK () OVER (ORDER BY cust_id) rnk
                  FROM   RETAIL_CR_UNIT_ACCOUNT b) a
       WHERE   rnk <= n_NUMBER_OF_ACCOUNTS AND ACCOUNT_TYPE = v_LOAN_TYPE
               AND CUST_AC_NO NOT IN
                        (SELECT   DISTINCT LOAN_REF
                           FROM   RETAIL_CR_UNIT_STAFF_ACCOUNT_ASSIGNED);

   COMMIT;
END;
/