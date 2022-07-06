DROP TRIGGER CEVM_ETL.RETAIL_CR_UNIT;

CREATE OR REPLACE TRIGGER CEVM_ETL."RETAIL_CR_UNIT"   
  before insert or update ON CEVM_ETL.RETAIL_CR_UNIT_STAFF               
  for each row
begin
if inserting then
  if :NEW."TB_ID" is null then 
    select RETAIL_CR_UNIT_SEQ.nextval into :NEW."TB_ID"  from sys.dual; 
  end if; 
  end if;
end;
/

