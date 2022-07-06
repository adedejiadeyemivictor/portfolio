CREATE OR REPLACE FUNCTION CEVM_ETL.fn_get_acct_status (v_foracid IN VARCHAR2)
   RETURN varchar2
AS
   acct_status   varchar2(50);
BEGIN
   SELECT acct_status
     INTO acct_status
     FROM report.d_acct_details_tbl@exadata_lnk
    WHERE foracid = v_foracid;
   

   RETURN nvl(acct_status,'nill');
EXCEPTION
   WHEN NO_DATA_FOUND
   THEN
      RETURN 'nill';
END;
/
