CREATE OR REPLACE PROCEDURE CEVM_ETL.assign_rsa_staff (
   v_bu_code      IN VARCHAR2,
   v_assignment   IN NUMBER,
   start_date     IN DATE,
   end_date       IN DATE)
AS
   max_count   NUMBER := 0;
--   v_bu_code   VARCHAR2 (2) := '4';
BEGIN
   SELECT COUNT (*)
     INTO max_count
     FROM tem_ass1
    WHERE bu_code = v_bu_code;

   EXECUTE IMMEDIATE 'truncate table test_assig';

   COMMIT;

   INSERT INTO test_assig (account_number,
                           staff,
                           retail_bu,
                           acct_opn_date,
                           day_assigned,
                           mobileno)
      SELECT foracid account_number,
             CAST ('' AS VARCHAR2 (10)) staff,
             bu_code retail_bu,
             acct_opn_date,
             CAST (NULL AS DATE) day_assigned,
             mobileno
        FROM tmp_channels_acct2
       WHERE     foracid NOT IN (SELECT a.foracid
                                   FROM (SELECT account_number
                                           FROM RSAC_DIGITAL_ACCTS_TBL
                                          WHERE retail_bu = v_bu_code) b,
                                        tbaadm.gam@ffbnffrdb a
                                  WHERE     a.foracid = b.account_number
                                        AND acct_opn_date >= '1-apr-2019')
             /**--AND foracid IN (SELECT foracid
               --                FROM report.d_acct_details_tbl@exadata_lnk
                 --             WHERE bvn IS NOT NULL)**/
             AND bu_code = v_bu_code;

   --   INSERT INTO test_assig (account_number,
   --                           staff,
   --                           retail_bu,
   --                           acct_opn_date,
   --                           day_assigned,
   --                           mobileno,
   --                           note)
   --      SELECT account,
   --             CAST ('' AS VARCHAR2 (10)) staff,
   --             bu_code retail_bu,
   --             CAST (NULL AS DATE) acct_opn_date,
   --             CAST (NULL AS DATE) day_assigned,
   --             phonenumber,
   --             notes
   --        FROM (SELECT a.*, b.bu_code
   --                FROM (SELECT account,
   --                             current_status NOTES,
   --                             phonenumber,
   --                             sol_id,
   --                             sol_id || '75' desk_code
   --                        FROM tmp_rsac_expired) a
   --                     JOIN report.d_business_units_dim@exadata_lnk b
   --                        ON (a.desk_code = b.desk_code)
   --               WHERE account NOT IN (SELECT a.foracid
   --                                       FROM (SELECT account_number
   --                                               FROM RSAC_DIGITAL_ACCTS_TBL -- WHERE retail_bu = v_bu_code
   --                                                                          ) b,
   --                                            tbaadm.gam@finacle_lnk a
   --                                      WHERE a.foracid = b.account_number));
   --
   --  /** --WHERE bu_code = v_bu_code;**/

   COMMIT;

   FOR K
      IN (SELECT calendar_date
            FROM report.d_time_dim@exadata_lnk
           WHERE     calendar_date BETWEEN start_date AND end_date
                 AND DAY_WEEKNAME NOT IN ('SATURDAY', 'SUNDAY'))
   LOOP
      FOR i IN (SELECT staff_number
                  FROM RSAC_STAFF_TBL
                 WHERE bu_code = v_bu_code AND active = 'Y'--AND staff_number IN ('TN080112')
               )
      LOOP
         EXECUTE IMMEDIATE 'truncate table temp_xxx';

         COMMIT;

         /**       INSERT INTO temp_xxx
                  (SELECT account_number
                     FROM test_assig
                    WHERE     TO_CHAR (acct_opn_date, 'yyyy') = 2018
                          AND ROWNUM <= 5                           --max_count * 0.
                          AND retail_bu = v_bu_code
                          AND day_assigned IS NULL
                  UNION
                   SELECT account_number
                     FROM test_assig
                    WHERE     TO_CHAR (acct_opn_date, 'yyyy') = 2019
                          AND ROWNUM <= 45                           --max_count * 6
                          AND retail_bu = v_bu_code
                          AND day_assigned IS NULL);
      **/

         INSERT INTO temp_xxx
            (SELECT account_number, note
               FROM test_assig
              WHERE     TO_CHAR (acct_opn_date, 'yyyy') = 2019
                    --AND TO_CHAR (acct_opn_date, 'MON') = 'JUN'
                    AND ROWNUM <= v_assignment -- and  ROWNUM <= 50                          --max_count * 0.
                    AND retail_bu = v_bu_code
                    AND day_assigned IS NULL);

         COMMIT;

         FOR j IN (SELECT account_number FROM temp_xxx)
         LOOP
            UPDATE test_assig
               SET staff = i.staff_number, day_assigned = k.calendar_date
             WHERE     retail_bu = v_bu_code
                   AND account_number = j.account_number;

            COMMIT;
         END LOOP;
      END LOOP;
   END LOOP;

   INSERT INTO RSAC_DIGITAL_ACCTS_TBL (account_number,
                                       phone_number,
                                       day_assigned,
                                       dsa_staff_no,
                                       retail_bu,
                                       notes,
                                       source)
      SELECT account_number,
             mobileno,
             day_assigned,
             staff,
             retail_bu,
             note,
             'DIGITAL ACCOUNTS'
        FROM test_assig
       WHERE     day_assigned IS NOT NULL
             AND staff IS NOT NULL
             AND retail_bu = v_bu_code;

   COMMIT;
END;
/
