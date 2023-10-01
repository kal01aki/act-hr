select * from employee;
select * from department;
select * from jobs;

insert into employee VALUES(1, 'Helen Belay', 'Jemo', 251900202020,45000.75,25,101);
insert into department values(45, 'Purchase',1,2);
insert into department values(25, 'General Office',2,2);
insert into jobs values(101, 'Assisstant',2000,15000);
insert into jobs values(111, 'Manager',25000,55000);
insert into jobs values(121, 'Junior Officer',5000,35000);
insert into jobs values(131, 'Senior Officer',10000,50000);
insert into employee VALUES(2, 'Selam Daniel', 'Sarbet', 251911333344,4000,45,111);
insert into employee VALUES(3, 'Markos Bekele', 'Merkato', 251942565897,35000,70,131);

update employee set job_id = 121 where emp_id = 10;
update employee set dept_id = 99, job_id = 111 where emp_id = 15;
update employee set dept_id = 70, job_id = 131 where emp_id = 45;
update employee set dept_id = 55, job_id = 101 where emp_id = 65;

--Horizontal Fragmentation
create table empdep as select * from employee
where dept_id = 70;
select * from empdep;

create table empdep1 as select * from employee
where dept_id = 99;
select * from empdep1;

create table dept as select * from empdep union
select * from empdep1;

select * from dept;

--Vertical Fragmentation

create table salary1 as select employee.salary, dept_id from employee;
select * from salary1;

create table salary2 as select salary, emp_name, dept_id from employee;
select * from salary2;

--Hybrid Fragmentation
--Horizontal on Vertical
create table horizontal as select * from salary2 where dept_id = 55;
select * from horizontal;

--Verical on Horizontal
create table vertical as select salary, dept_id from salary2;
select * from vertical;

--Partition by List
create table emp_list 
(
EMP_ID NUMBER(10,0), 
EMP_NAME VARCHAR2(50), 
ADDRESS VARCHAR2(50), 
PHONE_NO VARCHAR2(20), 
SALARY VARCHAR2(20), 
DEPT_ID NUMBER(10,0), 
JOB_ID NUMBER(10,0)
)
partition by list (dept_id)
(
partition p1 values ('99'),
partition p2 values ('70'),
partition p3 values ('55'),
partition p4 values ('45'),
partition p5 values ('25')
) enable row movement;
insert into emp_list select * from employee;
select * from emp_list;
commit;
select * from emp_list partition(p1);
select * from emp_list where dept_id = 70;
select * from employee where dept_id = 70;

EXEC DBMS_STATS.gather_table_stats('mesi', 'EMP_List');

commit;

--Partition by Range
create table emp_ran 
(
EMP_ID NUMBER(10,0), 
EMP_NAME VARCHAR2(50), 
ADDRESS VARCHAR2(50), 
PHONE_NO VARCHAR2(20), 
SALARY number(7,2), 
DEPT_ID NUMBER(10,0), 
JOB_ID NUMBER(10,0)
)
partition by range (salary)
interval(2500)
(
partition p1 values less than (2500),
partition p2 values less than (5000),
partition p3 values less than (10000),
partition p4 values less than (25000),
partition p5 values less than (50000)
) enable row movement;

insert into emp_ran select * from employee;
select * from emp_ran where salary>40000;

EXEC DBMS_STATS.gather_table_stats('mesi', 'EMP_ran');

commit;

-- Replication

-- Create a database link to the target database where the replication will be performed:
   
   CREATE DATABASE LINK target_dblink
   CONNECT TO target_user IDENTIFIED BY target_password
   USING 'target_tns';
   

-- Create a trigger on the source table to capture changes and propagate them to the target database:
   
   CREATE OR REPLACE TRIGGER replication_trigger
   AFTER INSERT OR UPDATE OR DELETE ON source_table
   FOR EACH ROW
   DECLARE
     v_action VARCHAR2(10);
   BEGIN
     IF INSERTING THEN
       v_action := 'INSERT';
     ELSIF UPDATING THEN
       v_action := 'UPDATE';
     ELSIF DELETING THEN
       v_action := 'DELETE';
     END IF;
   
     -- Propagate the change to the target database using the database link
     EXECUTE IMMEDIATE '
       INSERT INTO target_table (col1, col2, col3)
       VALUES (:new.col1, :new.col2, :new.col3)'
     USING :new.col1, :new.col2, :new.col3;
   END;
   
-- Repeat step 2 for each table that needs to be replicated.
