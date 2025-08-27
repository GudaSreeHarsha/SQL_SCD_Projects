# SCD Type 1 Implementation in SQL Server

This project demonstrates how to implement **Slowly Changing Dimension (SCD) Type 1** in SQL Server using T-SQL.

SCD Type 1 means that **old data is overwritten with new data** — history is not maintained.

---

## 📌 Project Overview
- **Dimension Table (`dw.Dim_Customer`)**: Holds the current state of customer data.
- **Staging Table (`stage.Staging_Customer`)**: Temporary area where new data loads before merging.
- **ETL Process**: Uses `MERGE` to upsert rows into the dimension table.

---

## 🛠️ Steps Implemented
1. **Setup**  
   - Created a sandbox database and schemas (`stage`, `dw`).

2. **Create Tables**  
   - `dw.Dim_Customer`: Customer dimension (target).  
   - `stage.Staging_Customer`: Staging area (source).

3. **Seed Initial Data**  
   - Inserted existing customer records into `Dim_Customer`.

4. **ETL Load #1**  
   - Loaded new records into staging.  
   - Ran `MERGE` to insert new rows and update changed ones.

5. **ETL Load #2**  
   - Simulated another batch with more changes.  
   - Validated that updates overwrite previous values.

6. **Stored Procedure (Optional)**  
   - Wrapped MERGE logic inside a reusable procedure.

---

## 🔄 Data Flow
