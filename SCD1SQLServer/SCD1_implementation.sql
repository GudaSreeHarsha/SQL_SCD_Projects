create database scd1
drop database scd1
use scd1

--dimension table (SCD Type 1: overwrite; no history)

---create schema
create schema dw;

create table dw.dim_customers
(
	DimCustomersSK	INT	identity(1,1) Primary Key,
	CustomerID		INT		NOT NULL,
	FirstName		NVARCHAR(50)	NULL,
	LastName		NVARCHAR(50)	NULL,
	City			NVARCHAR(50)	NULL,
	Email			NVARCHAR(100)	NULL,

--audit/ops columns
		
	CreatedAt		DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	UpdatedAt		DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	SourceSystem	NVARCHAR(50) NULL
);

---Uniqueness on the business key

create UNIQUE INDEX IXU_Dim_Customers_BK on dw.dim_customers(CustomerID);

---Staging table (usually truncated and reloaded each batch)

create schema stg

drop table stg.staging_customer

create table stg.staging_customer
(
	CustomerID		INT,
	FirstName		NVARCHAR(50),
	LastName		NVARCHAR(50),
	City			NVARCHAR(50),
	Email			NVARCHAR(100),
	SourceSystem	NVARCHAR(50),
	LoadAt			DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
----- Verify created tables

--Data warehouse
select * from dw.dim_customers

--Staging
select * from stg.staging_customer

-----Insert the data into dw.dim_customers

insert into 
	dw.dim_customers(CustomerID, FirstName, LastName, City, Email, SourceSystem)
values
	(1, 'John', 'Doe',   'Boston',   'john.doe@old.com',  'CRM'),
	(2, 'Jane', 'Smith', 'Chicago',  'jane@company.com',  'CRM'),
	(4, 'Ava',  'Li',    'Seattle',  'ava.li@company.com','CRM')

-----stage incoming data (some changes + one brand new)

-- id=1 exists but city changed to Boston to New York and email is also changed
-- id=2 exists and is identical (should NOT update)
-- id=3 is new (should INSERT)

Truncate table stg.staging_customer;

insert into 
	stg.staging_customer(CustomerID, FirstName, LastName, City, Email, SourceSystem)
values
	(1, 'John', 'Doe', 'New York', 'john.doe@company.com', 'CRM'), 
	(2, 'Jane', 'Smith', 'Chicago', 'jane@company.com', 'CRM'),
	(3, 'Mike', 'Brown', 'Dallas', 'mike.b@company.com', 'CRM');

----MERGE (Type 1: overwrite on change) + see what happened
-- capture actions for visibility

DECLARE @actions table(ActionTaken NVARCHAR(10));

merge dw.Dim_Customers as T
using(
	select 
		s.CustomerID,
		s.FirstName, s.LastName,
		s.City,
		s.Email,
		s.SourceSystem
	from stg.staging_customer s
)as S
on T.CustomerID=S.customerID
WHEN MATCHED AND
(
	ISNULL(T.FirstName,'') <> ISNULL(S.FirstName,'') OR
	ISNULL(T.LastName,'') <> ISNULL(S.LastName,'') OR
	ISNULL(T.City,'') <> ISNULL (S.City,'') OR
	ISNULL(T.Email,'') <> ISNULL (S.City,'') OR
	ISNULL(T.SourceSystem,'') <> ISNULL (S.SourceSystem,'')
)
THEN UPDATE SET
	T.FirstName = S.FirstName,
	T.LastName = S.lastName,
	T.City = S.City,
	T.Email = S.Email,
	T.SourceSystem = S.SourceSystem,
	T.UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET
THEN INSERT 
	(CustomerID, FirstName, LastName, City, Email, SourceSystem)
Values
	(S.CustomerID, S.FirstName, S.LastName, S.City, S.Email, S.SourceSystem)
Output $action into @actions;

--What did MERGE do?
 select ActionTaken, count(*) as RowsAffected
 from @actions
 Group By ActionTaken;

select
	DimCustomersSK, CustomerID, FirstName, LastName, City, Email, SourceSystem, CreatedAt, UpdatedAt
from
	dw.dim_customers
Order by
	CustomerID

----load #2 — change a few things and re-run
-- id=1: change city again; same email
-- id=2: change email (update)
-- id=3: unchanged (no update)
-- id=5: brand new record

truncate table stg.Staging_Customer;

Insert into 
	stg.staging_customer(CustomerID, FirstName, LastName, City, Email, SourceSystem)
VALUES
	(1, 'John', 'Doe', 'San Diego', 'john.doe@company.com', 'CRM'), 
	(2, 'Jane', 'Smith', 'Chicago', 'jane.new@company.com', 'CRM'),
	(3, 'Mike', 'Brown', 'Dallas', 'mike.b@company.com', 'CRM'),
	(5, 'Sara', 'Kim', 'Austin', 'sara.k@company.com', 'CRM');

DECLARE @actions2 table(ActionTaken NVARCHAR(10));

MERGE dw.Dim_Customers T
USING(
	SELECT
		s.CustomerID,
		s.FirstName, s.LastName,
		s.City,
		s.Email,
		s.SourceSystem
	From
		stg.Staging_Customer s
) as S
ON 
	T.CustomerID=S.CustomerID
WHEN MATCHED AND
(
    ISNULL(T.FirstName,'') <> ISNULL(S.FirstName,'') OR
    ISNULL(T.LastName ,'') <> ISNULL(S.LastName ,'') OR
    ISNULL(T.City     ,'') <> ISNULL(S.City     ,'') OR
    ISNULL(T.Email    ,'') <> ISNULL(S.Email    ,'') OR
    ISNULL(T.SourceSystem,'') <> ISNULL(S.SourceSystem,'')
)
THEN UPDATE SET
	T.FirstName    = S.FirstName,
    T.LastName     = S.LastName,
    T.City         = S.City,
    T.Email        = S.Email,
    T.SourceSystem = S.SourceSystem,
    T.UpdatedAt    = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET
THEN INSERT 
	(CustomerID, FirstName, LastName, City, Email, SourceSystem)
VALUES
	(S.CustomerID, S.FirstName, S.LastName, S.City, S.Email, S.SourceSystem)
OUTPUT $action into @actions2;

SELECT ActionTaken, count(*) as RowsAffected
from @actions2
Group By ActionTaken;

SELECT DimCustomersSK, CustomerID, FirstName, LastName, City, Email, SourceSystem, CreatedAt, UpdatedAt
FROM dw.dim_customers
ORDER BY CustomerID;

---NULL-safe & whitespace-safe comparisons
--If your source sometimes has NULL or extra spaces, normalize first with a view:

create or alter view stg.Staging_Customer_Clean_vw 
as
SELECT
	CustomerID,
	NULLIF(LTRIM(RTRIM(FirstName)),'') as FirstName,
	NULLIF(LTRIM(RTRIM(LastName)),'') as LastName,
	NULLIF(LTRIM(RTRIM(City)),'') as City,
	NULLIF(LTRIM(RTRIM(Email)),'') as Email,
	SourceSystem
from
	stg.Staging_Customer
GO

----Verify view
SELECT * FROM stg.Staging_Customer_Clean_vw 

---wrap as a stored procedure (reusable)

create or alter procedure dw.upcert_dimCustomers_SCD1
as
BEGIN
	SET NOCOUNT ON;
	
	BEGIN TRY
		BEGIN TRAN;

		DECLARE @log table(action_taken NVARCHAR(10));

		MERGE dw.dim_customers
		as T
		USING
		(
			SELECT
				s.CustomerID,
				s.FirstName, s.LastName,
				s.City,
				s.Email,
				s.SourceSystem
			From
				stg.Staging_Customer s
		) as S
			ON 
				T.CustomerID=S.CustomerID
		WHEN MATCHED AND
		(
			ISNULL(T.FirstName,'') <> ISNULL(S.FirstName,'') OR
			ISNULL(T.LastName ,'') <> ISNULL(S.LastName ,'') OR
			ISNULL(T.City     ,'') <> ISNULL(S.City     ,'') OR
			ISNULL(T.Email    ,'') <> ISNULL(S.Email    ,'') OR
			ISNULL(T.SourceSystem,'') <> ISNULL(S.SourceSystem,'')
		)
		THEN UPDATE SET
			T.FirstName    = S.FirstName,
			T.LastName     = S.LastName,
			T.City         = S.City,
			T.Email        = S.Email,
			T.SourceSystem = S.SourceSystem,
			T.UpdatedAt    = SYSUTCDATETIME()
		WHEN NOT MATCHED BY TARGET
		THEN INSERT 
			(CustomerID, FirstName, LastName, City, Email, SourceSystem)
		VALUES
			(S.CustomerID, S.FirstName, S.LastName, S.City, S.Email, S.SourceSystem)
		OUTPUT $action INTO @log;

		COMMIT;

		SELECT action_taken, count(*) as rows_affected
		from @log
		Group By action_taken

		END TRY
		BEGIN Catch
			IF @@TRANCOUNT>0 ROLLBACK;
			THROW;
		END CATCH
END;
GO

---Sample run

TRUNCATE TABLE stg.Staging_Customer;

INSERT INTO stg.Staging_Customer (CustomerID, FirstName, LastName, City, Email, SourceSystem)
VALUES
(1, 'John', 'Doe',   'Los Angeles', 'john.doe@company.com', 'CRM'),  -- changed city
(2, 'Jane', 'Smith', 'Chicago',     'jane.s@company.com',   'CRM'),  -- unchanged
(3, 'Mike', 'Brown', 'Dallas',      'mike.b@company.com',   'CRM'),  -- unchanged
(4, 'Lisa', 'Wong',  'Seattle',     'lisa.w@company.com',   'CRM');  -- new record

EXEC dw.upcert_dimCustomers_SCD1;

---quick “spot checks”
select count(*) as dimCount
from dw.dim_customers

select * from dw.dim_customers

select CustomerID, City, Email, UpdatedAt
from dw.dim_customers
where CustomerID=1

select * 
from dw.dim_customers
where UpdatedAt>=DATEADD(MINUTE,-5,SYSUTCDATETIME())