use scd2

---- products data warehouse schema creatin

create schema pdw

---- products staging schema creation

create schema pstg

----creating dimension table for products
drop table pdw.dim_products

create table pdw.dim_products
(
	dimProductsSK		INT identity(1,1) Primary Key,
	productID			INT NOT NULL,
	productName			NVARCHAR(100),
	category			NVARCHAR(50),
	brand				NVARCHAR(50),
	price				decimal(10,2),
	startDate			DATE,
	endDate				DATE,
	isCurrent			bit
)

----creating staging table for products
drop table pstg.staging_products

create table pstg.staging_products
(
	productID			INT,
	productName			NVARCHAR(100),
	category			NVARCHAR(50),
	brand				NVARCHAR(50),
	price				decimal(10,2)
)

----Initial load into staging table

truncate table pstg.staging_products

insert into pstg.staging_products
	(productID, productName, category, brand, price)
values
	(101, 'iPhone 13', 'Phones', 'Apple', 799.00),
	(102, 'Galaxy S22', 'Phones', 'Samsung', 749.00),
	(103, 'ThinkPad X1 Carbon', 'Laptops', 'Lenovo', 1499.00),
	(104, 'PlayStation 5', 'Consoles', 'Sony', 499.00),
	(105, 'Bose QC45', 'Headphones','Bose', 329.00);

---- Load staging layer into dim_products as initial load

insert into pdw.dim_products
	(productID, productName, category, brand, price, startDate, endDate, isCurrent)
select
	productID, 
	productName, 
	category,
	brand, 
	price,
	cast(getdate() as date),
	'9999-12-31',
	1
from
	pstg.staging_products

-----verify dim_products table

select* from pdw.dim_products

-----verify staging_products

select * from pstg.staging_products

----Simulate day-2 changes: load new snapshot INTO STAGING

--101-price drop (change)
--102-price change
--103-category change
--104-no change
--105-brand new product

truncate table pstg.staging_products

insert into 
	pstg.staging_products (productID, productName, category, brand, price)
values
	(101, 'iPhone 13', 'Phones', 'Apple', 749.00),   
	(102, 'Galaxy S22', 'Phones', 'Samsung', 699.00),
	(103, 'ThinkPad X1 Carbon', 'Ultrabooks', 'Lenovo', 1499.00),
	(104, 'PlayStation 5', 'Consoles', 'Sony', 499.00),
	(106, 'iPad Air', 'Tablets', 'Apple', 599.00);

----SCD Type 2 ETL Logic

MERGE pdw.dim_products as target
USING pstg.staging_products as source
on target.productID=source.productID
and target.isCurrent=1
WHEN MATCHED AND
	ISNULL(target.productName,'') <> ISNULL(source.productName,'') OR
	ISNULL(target.category,'') <> ISNULL(source.category,'') OR
	ISNULL(target.brand,'') <> ISNULL(source.brand,'') OR
	ISNULL(target.price,'') <> ISNULL(source.price,'')
THEN UPDATE SET
	target.endDate = dateadd(day, -1, CAST(GETDATE() as DATE)),
	target.isCurrent=0

WHEN NOT MATCHED
THEN
insert	
	(productID, productName, category, brand, price, startDate, endDate, isCurrent)
values
	(source.productID, source.productName, source.category, source.brand, source.price, CAST(GETDATE() as date), '9999-12-31', 1);

----Inser the new version of changed recoreds

INSERT INTO
	pdw.dim_products(productID, productName, category, brand, price, startDate, endDate, isCurrent)
select
	s.productID, 
	s.productName, 
	s.category, 
	s.brand, 
	s.price, 
	CAST(GETDATE() as date), 
	'9999-12-31', 
	1
from
	pstg.staging_products s
join pdw.dim_products d
on s.productID=d.productID
and d.endDate=DATEADD(DAY, -1, cast(getdate() as date));


select * from pdw.dim_products

----Creating a view on top of dim_products

create or alter view pdw.dim_products_active_vw
as
select * 
from pdw.dim_products
where IScurrent=1

----Wrap entire query in a procedure

create procedure dw.usp_load_products_scd2
as
begin
	SET NOCOUNT ON;
	 MERGE pdw.dim_products AS target
    USING pstg.staging_products AS source
    ON target.productID = source.productID
       AND target.isCurrent = 1
    WHEN MATCHED AND 
        (
            ISNULL(target.productName, '') <> ISNULL(source.productName, '') OR
            ISNULL(target.category, '') <> ISNULL(source.category, '') OR
            ISNULL(target.brand, '') <> ISNULL(source.brand, '') OR
            ISNULL(CAST(target.price AS VARCHAR(50)), '') <> ISNULL(CAST(source.price AS VARCHAR(50)), '')
        )
    THEN UPDATE
        SET target.endDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE)),
            target.isCurrent = 0

    -- Step 2: Insert new records if product does not exist
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (productID, productName, category, brand, price, startDate, endDate, isCurrent)
         VALUES (source.productID, source.productName, source.category, source.brand, source.price, 
                 CAST(GETDATE() AS DATE), '9999-12-31', 1);

    -- Step 3: Insert the new version of changed records
    INSERT INTO pdw.dim_products(productID, productName, category, brand, price, startDate, endDate, isCurrent)
    SELECT 
        s.productID, 
        s.productName, 
        s.category, 
        s.brand, 
        s.price, 
        CAST(GETDATE() AS DATE), 
        '9999-12-31', 
        1
    FROM pstg.staging_products s
    JOIN pdw.dim_products d
        ON s.productID = d.productID
       AND d.endDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));

	   END;
GO

