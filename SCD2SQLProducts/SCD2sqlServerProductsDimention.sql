create database scd2

use scd2

----data warehouse schema creation

create schema dw;

----staging layer schema 

create schema stg;

-----Create Tables 
--Dimension Table (SCD Type 2 enabled)

drop table dw.dim_customers

create table dw.dim_customers
(
	dimCustomersSK	INt IDENTITY(1,1) PRIMARY KEY,
	customerID		INT,
	fullName		NVARCHAR(100),
	city			NVARCHAR(50),
	email			NVARCHAR(100),
	startDate		DATE,
	endDate			DATE,
	isCurrent		BIT
)

-----Create Tables 
-----Staging Table (raw data load)

drop table stg.staging_customers

create table stg.staging_customers
(
	customerID		INT,
	fullName		NVARCHAR(100),
	city			NVARCHAR(50),
	email			NVARCHAR(100)
)

----Load Initial Data into Staging

TRUNCATE TABLE stg.staging_customers

INSERT INTO 
	stg.staging_customers(customerID, fullName, city, email)
values
	(1, 'Ravi Kumar', 'Hyderabad', 'ravi.kumar@email.com'),
	(2, 'Priya Sharma', 'Mumbai', 'priya.sharma@email.com'),
	(3, 'Anil Reddy', 'Chennai', 'anil.reddy@email.com');


---- Initial load into dimension table datawarehouse

insert into 
	dw.dim_customers (customerID, fullName, city, email, startDate, endDate, isCurrent)
select
	customerID,
	fullName,
	city,
	email,
	CAST(GETDATE() as DATE),
	'9999-12-31',
	1
from
	stg.staging_customers

----verify data load from staging to Dimension table

select * from dw.dim_customers

---- verify data load

select * from stg.staging_customers

----Simulate Data Change (Updates in Staging)
 TRUNCATE TABLE stg.staging_customers

 INSERT INTO
	stg.staging_customers(CustomerID, FullName, City, Email)
VALUES
	(1, 'Ravi Kumar', 'Bangalore', 'ravi.kumar@email.com'),
	(2, 'Priya Sharma', 'Mumbai', 'priya.sharma@email.com'),
	(3, 'Anil Reddy', 'Chennai', 'anilreddy.new@email.com');

-----SCD2 logic

MERGE dw.dim_customers as target
USING stg.staging_customers as source
on	target.customerID=source.customerID
and target.isCurrent=1
WHEN MATCHED and
	isnull(target.fullname,'') <> isnull(source.fullName,'') OR
	isnull(target.city,'') <> isnull(source.city,'')OR
	isnull(target.email,'') <> isnull(source.email,'')
THEN
	update set
		target.endDate = DATEADD(DAY,-1,CAST(GETDATE() as DATE)),
		target.Iscurrent = 0
WHEN NOT MATCHED BY TARGET
THEN 
	INSERT
		(CustomerID, FullName, City, Email, StartDate, EndDate, IsCurrent)
	VALUES
		(source.customerID, source.fullName, source.city, source.email, CAST(GETDATE() as Date), '9999-12-31', 1);

----Inser the new version of changed recoreds

INSERT INTO 
	dw.dim_customers(CustomerID, FullName, City, Email, StartDate, EndDate, IsCurrent)
select
	s.CustomerID, 
	s.FullName, 
	s.City, 
	s.Email,
    CAST(GETDATE() AS DATE), '9999-12-31', 1
from stg.staging_customers s
join dw.dim_customers d
on s.customerID=d.customerID
and d.isCurrent=0
and d.endDate=DATEADD(DAY, -1, CAST(GETDATE() as DATE));


SELECT * FROM dw.dim_customers ORDER BY CustomerID, StartDate;

---- creating a view on top of dimention table

create or alter view currentCustomers_vw
as
select *
from dw.dim_customers

create or alter view customerStatus_vw
as
select *,
case
when Iscurrent=1 then 'Current'
else 'Historical'
end as status
from dw.dim_customers

select * from customerStatus_vw

---wrapping entire logic in a procedure


create procedure sp_updateDimCustomers
as
BEGIN
	set NOCOUNT ON;

    -------------------------------------------
    -- Step 1: Expire old records that changed
    -------------------------------------------

merge dw.dim_customers as Target
using stg.staging_customers as source
on target.customerID=source.customerID
and target.Iscurrent=1
When matched and
	isnull(target.fullname,'') <> isnull(source.fullName,'') OR
	isnull(target.city,'') <> isnull(source.city,'')OR
	isnull(target.email,'') <> isnull(source.email,'')
THEN
	update set
		target.endDate = DATEADD(DAY, -1, CAST(GETDATE() as DATE)),
		IsCurrent = 0
WHEN NOT MATCHED BY TARGET
THEN
	INSERT
		(customerID, fullName, city, email, startDate, endDate, IsCurrent)
	VALUES
		(source.customerID, source.fullName, source.city, source.email, CAST(GETDATE() as DATE),'9999-12-31',1);

---------------------------------------------------
    -- Step 2: Insert new version for changed rows
---------------------------------------------------
insert into dw.dim_customers (CustomerID, FullName, City, Email, StartDate, EndDate, IsCurrent)
SELECT s.CustomerID, 
		s.FullName, 
		s.City, 
		s.Email,
        CAST(GETDATE() AS DATE), '9999-12-31', 1
from
stg.staging_customers s
join dw.dim_customers d
on d.customerID=s.customerID
and d.isCurrent=0
and d.EndDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
END
go