
ALTER TABLE RSAC_DIGITAL_ACCTS_TBL_2 RENAME TO RSAC_DIGITAL_ACCTS_TBL


UPDATE RSAC_DIGITAL_ACCTS_TBL_2
SET DSA_STAFF_NO = 'TS000000'
WHERE RETAIL_BU = '4'
and DSA_STAFF_NO = 'TN000000'



delete from RSAC_STAFF_TBL 
where tb_id in ('161','2', '3', '4', '5', '8', '21', '143' )


ALTER TABLE RSAC_DIGITAL_ACCTS_TBL ENABLE ROW MOVEMENT;


FLASHBACK TABLE RSAC_DIGITAL_ACCTS_TBL TO TIMESTAMP TO_TIMESTAMP
('2021/sep/6 09:00:00', 'YYYY-MM-DD HH24:MI:SS');

