use scd1

create schema pdw ---product data warehouse schema creation

create schema pstg ---product staging level schema creation

--Dimention table

create  table pdw.dim_products
(
	dimProductSK	INT IDENTITY(1,1) PRIMARY KEY,
	productID		INT NOT NULL,
	productName		NVARCHAR(100),
	category		NVARCHAR(50),
	price			DECIMAL(10,2),
	supplier		NVARCHAR(100),
----audit/ops columns
	createdAt		DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	modifiedAt		DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()	
)

CREATE UNIQUE INDEX UIX_dim_products_BK on pdw.dim_products(productID)

-----Staging Table

create table pstg.staging_products
(
	productID		INT NOT NULL,
	productName		NVARCHAR(100),
	category		NVARCHAR(50),
	price			DECIMAL(10,2),
	supplier		NVARCHAR(100),
	loadedAt		DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
)

------ verify both primary and staging table is created sucessfully

select * from pdw.dim_products

select * from pstg.staging_products

------ Load Initial data in primary table

INSERT INTO
	pdw.dim_products(productID, productName, category, price, supplier)
VALUES
	(101, 'iPhone 14', 'Mobile', 999.99, 'Apple'),
	(102, 'Galaxy S23', 'Mobile', 899.99, 'Samsung'),
	(103, 'MacBook Air', 'Laptop', 1199.99, 'Apple');

------ load the new incoming data with some changes in staging table

TRUNCATE TABLE pstg.staging_products

INSERT INTO
	pstg.staging_products(productID, productName, category, price, supplier)
VALUES
	(101, 'iPhone 14', 'Mobile', 949.99, 'Apple'), ---Price is changed
	(102, 'Galaxy S23', 'Mobile', 899.99, 'Samsung'), ---Nothing is changed
	(104, 'ThinkPad X1', 'Laptop', 1399.99, 'Lenovo') ---New Product

------Merge Logic

-- Declare an action variable to save the merge activity

DECLARE @actions TABLE(actionTaken NVARCHAR(50));

MERGE pdw.dim_products as T
USING(
		SELECT
			s.productID, s.productName, s.category, s.price, s.supplier
		from
			pstg.staging_products s
	)as S
		ON T.productID = S.ProductID
WHEN MATCHED AND
(
	ISNULL(T.productName,'') <> ISNULL(S.productName,'') OR
	ISNULL(T.Category,'')    <> ISNULL(S.Category,'') OR
    ISNULL(T.Price,0)        <> ISNULL(S.Price,0) OR
    ISNULL(T.Supplier,'')    <> ISNULL(S.Supplier,'')
)
THEN UPDATE SET
	T.productName = S.productName,
	T.category = S.category,
	T.price = S.price,
	T.supplier = S.supplier,
	T.modifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET
THEN INSERT
	(productID, productName, category, price, supplier)
VALUES
	(S.productID, S.productName, S.category, S.price, S.supplier)
OUTPUT $action INTO @actions;

-----Verify the actions table 


DECLARE @actions TABLE(actionTaken NVARCHAR(50));

SELECT actionTaken, Count(*) as rowsAffected
from @actions
group by actionTaken

-- Correct: Declaring @actions before use
DECLARE @actions TABLE (actionTaken NVARCHAR(50));
SELECT * FROM @actions;

-----Add new data to staging

TRUNCATE TABLE pstg.staging_products

----New batch of data insertion into staging layer

INSERT INTO
	pstg.staging_products(productID, productName, category, price, supplier)
VALUES
	(101, 'iPhone 14', 'Mobile', 949.99, 'Apple'),      -- price changed
    (102, 'Galaxy S23', 'Mobile', 899.99, 'Samsung'),   -- unchanged
    (104, 'ThinkPad X1', 'Laptop', 1399.99, 'Lenovo'),  -- new product
    (105, 'Surface Pro 9', 'Laptop', 1299.99, 'Microsoft'), -- new product
    (103, 'MacBook Air', 'Laptop', 1149.99, 'Apple');   -- price changed

-- Declare table variable to capture actions

DECLARE @actions2 TABLE(actionTaken2 NVARCHAR(50));

MERGE pdw.dim_products as T
USING(
	SELECT
		s.ProductID, s.ProductName, s.Category, s.Price, s.Supplier
	FROM 
		pstg.staging_products s
)AS S
	ON T.productID=S.productID
WHEN MATCHED AND
	(
		ISNULL(T.productName,'') <> ISNULL(S.productName,'') OR
		ISNULL(T.category,'') <> ISNULL(S.category,'') OR
		ISNULL(T.price,0) <> ISNULL(S.price,0) OR
		ISNULL(T.supplier,'') <> ISNULL(S.supplier,'')
	)
THEN UPDATE SET
	T.productName = S.productName,
    T.category = S.category,
    T.price = S.price,
    T.supplier = S.supplier,
    T.modifiedAt = SYSUTCDATETIME()

WHEN NOT MATCHED BY TARGET
THEN INSERT 
	(productID, productName, category, price, supplier)
VALUES 
	(S.productID, S.productName, S.category, S.price, S.supplier)
OUTPUT $action into @actions2;



SELECT * FROM pdw.Dim_Products
ORDER BY ProductID;

----- Trim Spaces and add into view

create or alter view pstg.staging_products_clean_vw
as
select
	productID,
	NULLIF(LTRIM(RTRIM(productName)),'') as productName,
	NULLIF(LTRIM(RTRIM(category)),'') as category,
	price,
	NULLIF(LTRIM(RTRIM(supplier)),'') as supplier
from 
	pstg.staging_products

-----WRAP the execution into procedure

create or alter procedure pdw.upsert_dim_products_scd1
as
begin
	set nocount on;

	begin try
		begin tran;

		  DECLARE @actions TABLE(ActionTaken NVARCHAR(50));

		  --MERGE clean staging into dimention
		  
		 MERGE pdw.dim_products as T
		 USING(
				SELECT
					s.ProductID, s.ProductName, s.Category, s.Price, s.Supplier
				FROM 
					pstg.staging_products s
			  )AS S
				ON T.productID=S.productID
		 WHEN MATCHED AND
		   (
            ISNULL(T.ProductName,'') <> ISNULL(S.ProductName,'') OR
            ISNULL(T.Category,'') <> ISNULL(S.Category,'') OR
            ISNULL(T.Price,0) <> ISNULL(S.Price,0) OR
            ISNULL(T.Supplier,'') <> ISNULL(S.Supplier,'')
        )
		THEN UPDATE SET
            T.ProductName = S.ProductName,
            T.Category = S.Category,
            T.Price = S.Price,
            T.Supplier = S.Supplier,
            T.ModifiedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET
        THEN INSERT (ProductID, ProductName, Category, Price, Supplier)
        VALUES (S.ProductID, S.ProductName, S.Category, S.Price, S.Supplier)
        OUTPUT $action INTO @actions;

		COMMIT;
		-- Return action summary
        SELECT ActionTaken, COUNT(*) AS RowsAffected
        FROM @actions
        GROUP BY ActionTaken;
		    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;
    END CATCH
END;

EXEC pdw.upsert_dim_products_scd1;


select * from pdw.dim_products