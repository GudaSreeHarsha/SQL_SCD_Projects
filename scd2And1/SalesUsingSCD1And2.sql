create database retailDW
go
use retailDW
go

-----Dimension Tables

---Customer (SCD2)

CREATE TABLE dimCustomers
(
	dimCustomerSK	INT	IDENTITY(1,1) Primary KEY,
	customerID		INT NOT NULL,
	fullName		NVARCHAR(100),
	city			NVARCHAR(100),
	email			NVARCHAR(100),
	startDate		DATE,
	endDate			DATE,
	isCurrent		BIT
);

---Products (SCD1)

create table dimProducts
(
	productID		INT Primary Key,
	productName		NVARCHAR(100),
	category		NVARCHAR(100),
	price			DECIMAL(10,2)
)

---Stores (SCD2)
drop table dimStores
create table dimStores
(
	dimStoreSK		INT	IDENTITY(1,1) Primary Key,
	storeID			INT	NOT NULL,
	storeName		NVARCHAR(100),
	city			NVARCHAR(100),
	startDate		DATE,
	endDate			DATE,
	isCurrent		BIT
)

-----Fact tables
---sales fact table
drop table factSales
create table factSales
(
	saleID			INT IDENTITY(1,1) Primary Key,
	dimCustomerSK	INT,
	productID		INT,
	dimStoreSK		INT,
	saleDate		DATE,
	quantity		INT,
	totalAmount		DECIMAL(10,2),
	FOREIGN KEY (dimCustomerSK) REFERENCES dimCustomers(dimCustomerSk),
	FOREIGN KEY (productID) REFERENCES dimProducts(productID),
	FOREIGN KEY	(dimStoreSK) REFERENCES dimStores(dimStoreSK)
);


----Staging Tables

--stg_customers

create table stgCustomers
(
	customerID		INT,
	fullName		NVARCHAR(100),
	city			NVARCHAR(100),
	email			NVARCHAR(100),
)

--stg_products

create table stgProducts
(
	productID		INT ,
	productName		NVARCHAR(100),
	category		NVARCHAR(100),
	price			DECIMAL(10,2)
)

--stg_stores

create table stgStores
(
	storeID			INT	NOT NULL,
	storeName		NVARCHAR(100),
	city			NVARCHAR(100)
)

--stg_sales

create table stgSales
(
    CustomerID		INT,
    ProductID		INT,
    StoreID			INT,
    SaleDate		DATE,
    Quantity		INT
)

----Sample Data for Staging

--Customers

INSERT INTO 
	stgCustomers(customerID, fullName, city, email)
values
	(1, 'Ravi Kumar', 'Hyderabad', 'ravi.kumar@email.com'),
	(2, 'Priya Sharma', 'Mumbai', 'priya.sharma@email.com'),
	(3, 'Anil Reddy', 'Chennai', 'anil.reddy@email.com');
	
--Products

INSERT INTO 
	stgProducts(productID, category, productName, price)
VALUES
	(101, 'Laptop', 'Electronics', 750.00),
	(102, 'Smartphone', 'Electronics', 500.00),
	(103, 'Headphones', 'Accessories', 50.00);

--Stores

INSERT INTO 
	stgStores(storeID, storeName, city)
VALUES
	(201, 'Central Mall', 'Hyderabad'),
	(202, 'City Plaza', 'Mumbai'),
	(203, 'Sunshine Store', 'Chennai');

--Sales

INSERT INTO 
	stgSales(CustomerID, ProductID, StoreID, SaleDate, Quantity)
VALUES
	(1, 101, 201, '2025-08-25', 2),
	(2, 102, 202, '2025-08-25', 1),
	(3, 103, 203, '2025-08-25', 3);


----Verify all the created tables

-----Dimension Tables

select * from dimCustomers

select * from dimProducts

select * from dimStores

-----Fact table

select * from factSales

-----Staging tables

select * from stgCustomers

select * from stgProducts

select * from stgSales

select * from stgStores

-----ETL Scripts

--Customers (SCD2)

---- End current records if changed

update d
set 
endDate = GETDATE(),
isCurrent=0
from dimCustomers d
join stgCustomers s 
on d.customerID = s.customerID
where d.isCurrent=1
and
ISNULL(d.fullName,'') <> ISNULL(s.fullName,'')OR
ISNULL(d.city,'') <> ISNULL(s.city,'')OR
ISNULL(d.email,'') <> ISNULL(s.email,'')

---Insert New Version of records

INSERT INTO
	dimCustomers(CustomerID, FullName, City, Email, StartDate, EndDate, IsCurrent)
SELECT
	s.customerID,
	s.fullName,
	s.city,
	s.email,
	GETDATE(),
	'9999-12-31',
	1
from stgCustomers s
WHERE NOT EXISTS
(
	SELECT 
		1
	FROM 
		dimCustomers d
	WHERE
		d.customerID=s.customerID 
		and isCurrent=1
)

----products (SCD1-overwrite)

update p
set 
	p.productName=s.productName,
	p.category=s.category,
	p.price=s.price
from dimProducts p
join stgProducts s
on p.productID=s.productID

--Insert new 

insert into 
	dimProducts(productID, productName, category, price)
select
	s.productID,
	s.productName,
	s.category,
	s.price
from
	stgProducts s
where not exists (select 1 from dimProducts p where p.productID=s.productID)


-----Stores (SCD2 – history maintained)

-- Step 1: End current record if data changed

-- Step 1: Expire changed rows
UPDATE d
SET EndDate = CAST(GETDATE() AS DATE),
    IsCurrent = 0
FROM dimStores d
JOIN (
    SELECT DISTINCT StoreID, StoreName, City
    FROM stgStores
) s ON d.StoreID = s.StoreID
WHERE d.IsCurrent = 1
  AND (ISNULL(d.StoreName,'') <> ISNULL(s.StoreName,'')
       OR ISNULL(d.City,'') <> ISNULL(s.City,''));

-- Step 2: Insert new or changed stores
INSERT INTO dimStores (StoreID, StoreName, City, StartDate, EndDate, IsCurrent)
SELECT s.StoreID, s.StoreName, s.City,
       CAST(GETDATE() AS DATE), '9999-12-31', 1
FROM (
    SELECT DISTINCT StoreID, StoreName, City
    FROM stgStores
) s
WHERE NOT EXISTS (
    SELECT 1 FROM dimStores d
    WHERE d.StoreID = s.StoreID
      AND d.IsCurrent = 1
);
---Fact Table Load

INSERT INTO factSales (DimCustomerSK, ProductID, DimStoreSK, SaleDate, Quantity, TotalAmount)
SELECT c.DimCustomerSK, p.ProductID, st.DimStoreSK, sa.SaleDate, sa.Quantity, sa.Quantity * p.Price
FROM stgSales sa
JOIN dimCustomers c ON sa.CustomerID = c.CustomerID AND c.IsCurrent = 1
JOIN dimProducts p ON sa.ProductID = p.ProductID
JOIN dimStores st ON sa.StoreID = st.StoreID AND st.IsCurrent = 1;


----Test queries

-- Check Customers history
SELECT * FROM dimCustomers ORDER BY CustomerID, StartDate;

-- Check Products
SELECT * FROM dimProducts;

-- Check Stores history
SELECT * FROM dimStores ORDER BY StoreID, StartDate;

-- Check Sales fact table
SELECT f.SaleID, c.FullName, p.ProductName, st.StoreName, f.Quantity, f.TotalAmount
FROM factSales f
JOIN dimCustomers c ON f.DimCustomerSK = c.DimCustomerSK
JOIN dimProducts p ON f.ProductID = p.ProductID
JOIN dimStores st ON f.DimStoreSK = st.DimStoreSK;
