SCD Type 1 Implementation for dim_products
Objective

This project demonstrates how to implement a Slowly Changing Dimension (Type 1) for a dim_products table in a data warehouse using SQL Server.

Type 1 SCD: Overwrites existing records when changes occur. No history is preserved.

Uses staging tables, MERGE statements, and audit columns to track changes.

Database & Schema Setup
-- Create database
USE scd1;

-- Create schemas
CREATE SCHEMA pdw;  -- Product Data Warehouse
CREATE SCHEMA pstg; -- Product Staging Layer

Dimension Table: pdw.dim_products
CREATE TABLE pdw.dim_products
(
    dimProductSK INT IDENTITY(1,1) PRIMARY KEY,
    productID INT NOT NULL,
    productName NVARCHAR(100),
    category NVARCHAR(50),
    price DECIMAL(10,2),
    supplier NVARCHAR(100),
    createdAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    modifiedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE UNIQUE INDEX UIX_dim_products_BK ON pdw.dim_products(productID);


Primary key: dimProductSK (surrogate key)

Business key: productID (unique index ensures uniqueness)

Audit columns: createdAt, modifiedAt

Staging Table: pstg.staging_products
CREATE TABLE pstg.staging_products
(
    productID INT NOT NULL,
    productName NVARCHAR(100),
    category NVARCHAR(50),
    price DECIMAL(10,2),
    supplier NVARCHAR(100),
    loadedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);


Holds incoming batch/snapshot data

Typically truncated and reloaded before each ETL run

Initial Data Load
INSERT INTO pdw.dim_products(productID, productName, category, price, supplier)
VALUES
    (101, 'iPhone 14', 'Mobile', 999.99, 'Apple'),
    (102, 'Galaxy S23', 'Mobile', 899.99, 'Samsung'),
    (103, 'MacBook Air', 'Laptop', 1199.99, 'Apple');


Populates dimension table with initial products

Load New Batch in Staging
TRUNCATE TABLE pstg.staging_products;

INSERT INTO pstg.staging_products(productID, productName, category, price, supplier)
VALUES
    (101, 'iPhone 14', 'Mobile', 949.99, 'Apple'),   -- price changed
    (102, 'Galaxy S23', 'Mobile', 899.99, 'Samsung'), -- unchanged
    (104, 'ThinkPad X1', 'Laptop', 1399.99, 'Lenovo'); -- new product


Staging table contains changes and new records for the current batch

SCD Type 1 MERGE Logic
DECLARE @actions TABLE(actionTaken NVARCHAR(50));

MERGE pdw.dim_products AS T
USING (
    SELECT productID, productName, category, price, supplier
    FROM pstg.staging_products
) AS S
ON T.productID = S.productID
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
THEN INSERT (productID, productName, category, price, supplier)
VALUES (S.productID, S.productName, S.category, S.price, S.supplier)
OUTPUT $action INTO @actions;

-- View actions summary
SELECT actionTaken, COUNT(*) AS rowsAffected
FROM @actions
GROUP BY actionTaken;


Updates existing rows if any column has changed

Inserts new rows if product doesn’t exist in dimension

Logs actions (INSERT / UPDATE) for auditing

Optional: View for Clean Staging Data
CREATE OR ALTER VIEW pstg.staging_products_clean_vw AS
SELECT
    productID,
    NULLIF(LTRIM(RTRIM(productName)),'') AS productName,
    NULLIF(LTRIM(RTRIM(category)),'') AS category,
    price,
    NULLIF(LTRIM(RTRIM(supplier)),'') AS supplier
FROM pstg.staging_products;


Trims spaces and handles empty strings

Wrap into Stored Procedure
CREATE OR ALTER PROCEDURE pdw.upsert_dim_products_scd1
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRAN;
            DECLARE @actions TABLE(ActionTaken NVARCHAR(50));

            MERGE pdw.dim_products AS T
            USING (SELECT ProductID, ProductName, Category, Price, Supplier FROM pstg.staging_products) AS S
            ON T.productID = S.productID
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

-- Execute procedure
EXEC pdw.upsert_dim_products_scd1;


Wraps MERGE logic into a reusable ETL procedure

Ensures transactional safety and action logging

Verify Dimension Table
SELECT *
FROM pdw.dim_products
ORDER BY productID;


Confirms that all inserts and updates from staging have been applied

Key Concepts Covered

SCD Type 1 (overwrite, no history)

Staging table pattern for batch ETL

MERGE statement with conditional update

Logging actions with table variables

Audit columns (createdAt, modifiedAt)

Cleaning staging data using a view

Wrapping ETL into a stored procedure