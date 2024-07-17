
ALTER TABLE RSAC_DIGITAL_ACCTS_TBL_2 RENAME TO RSAC_DIGITAL_ACCTS_TBL


UPDATE RSAC_DIGITAL_ACCTS_TBL_2
SET DSA_STAFF_NO = 'TS000000'
WHERE RETAIL_BU = '4'
and DSA_STAFF_NO = 'TN000000'



delete from RSAC_STAFF_TBL 
where tb_id in ('161','2', '3', '4', '5', '8', '21', '143' )


DELETE E FROM ACCTS_TBL E
  INNER JOIN
  (SELECT *, RANK() OVER(PARTITION BY firstname, lastname, country ORDER BY id) rank FROM ACCTS_TBL) T 
  ON E.ID = t.ID
  WHERE rank > 1;


CREATE TABLE Employee
    (ID INT identity(1,1), FirstName Varchar2(100), LastName Varchar2(100), Country Varchar(100));
    
Insert into Employee (FirstName,LastName,Country) values('Raj','Gupta','India'),
                                ('Raj','Gupta','India'),
                                ('Mohan','Kumar','USA'),
                                ('James','Barry','UK'),
                                ('James','Barry','UK'),
                                ('James','Barry','UK')

  
ALTER TABLE RSAC_DIGITAL_ACCTS_TBL ENABLE ROW MOVEMENT;


FLASHBACK TABLE RSAC_DIGITAL_ACCTS_TBL TO TIMESTAMP TO_TIMESTAMP
('2021/sep/6 09:00:00', 'YYYY-MM-DD HH24:MI:SS');

