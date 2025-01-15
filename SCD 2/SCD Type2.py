# Databricks notebook source
# SCD TYPE 2 WITH GENERATED IDENTITY/SURROGATE KEYS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the input tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cust_input (
# MAGIC     cust_id STRING,
# MAGIC     name STRING
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TABLE txn_input (
# MAGIC     txn_id STRING,
# MAGIC     txn_amt INT,
# MAGIC     txn_date DATE,
# MAGIC     cust_id STRING
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the dimension and fact tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the  dimension table. Note the use of generated columns to generate primary a unique ID for each row and then referencing that as the primary key
# MAGIC CREATE OR REPLACE TABLE jri.gold.dim_cust (
# MAGIC     cust_id_pk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     cust_id STRING NOT NULL,
# MAGIC     cust_name VARCHAR(50),
# MAGIC     Effective_Start_Date DATE,
# MAGIC     Effective_End_Date DATE,
# MAGIC     IsCurrent INT,
# MAGIC     processed_timestamp TIMESTAMP, CONSTRAINT cust_pk PRIMARY KEY(cust_id_pk)
# MAGIC );
# MAGIC
# MAGIC -- Create the  fact table. Note the referencing of the primary key in the dimension table as a foreign key in the fact table
# MAGIC CREATE OR REPLACE TABLE jri.gold.fact_txn (
# MAGIC     txn_id STRING,
# MAGIC     txn_amt INT,
# MAGIC     txn_date DATE,
# MAGIC     cust_id STRING,
# MAGIC     cust_id_pk BIGINT, CONSTRAINT cust_fk FOREIGN KEY (cust_id_pk) REFERENCES jri.gold.dim_cust
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate the Input cust and txn tables

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cust_input VALUES ("A100","Jack");
# MAGIC INSERT INTO cust_input VALUES ("A200","John");
# MAGIC
# MAGIC INSERT INTO txn_input VALUES ("TXN002", 200, "2025-10-10", "A100");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge and populate the Dimension table

# COMMAND ----------

# MAGIC %sql
# MAGIC   MERGE INTO jri.gold.dim_cust B
# MAGIC     USING (
# MAGIC             SELECT cust_input.cust_id as merge_key, cust_input.*,current_date() as Effective_Start_Date,NULL as Effective_End_Date,current_timestamp() as processed_timestamp
# MAGIC             FROM cust_input
# MAGIC             UNION ALL
# MAGIC             SELECT NULL as merge_key, cust_input.*,Effective_Start_Date,Effective_End_Date,processed_timestamp
# MAGIC             FROM cust_input
# MAGIC             JOIN jri.gold.dim_cust A ON cust_input.cust_id = A.cust_id
# MAGIC             WHERE A.IsCurrent = 1 and hash(A.cust_name) != hash(cust_input.name)  
# MAGIC           ) staged_updates
# MAGIC         ON B.cust_id = merge_key 
# MAGIC         WHEN MATCHED AND B.IsCurrent = 1 AND hash(B.cust_name) != hash(staged_updates.name) THEN
# MAGIC           UPDATE SET IsCurrent = 0, Effective_End_Date = current_date()
# MAGIC         WHEN NOT MATCHED THEN 
# MAGIC           INSERT (cust_id, cust_name, IsCurrent, Effective_Start_Date, Effective_End_Date, processed_timestamp)
# MAGIC           VALUES (staged_updates.cust_id, staged_updates.name, 1, staged_updates.Effective_Start_Date, NULL, staged_updates.processed_timestamp);
# MAGIC
# MAGIC SELECT * FROM jri.gold.dim_cust;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert into the transaction fact table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO jri.gold.fact_txn 
# MAGIC     SELECT txn_id, txn_amt, txn_date, A.cust_id, B.cust_id_pk FROM txn_input A LEFT JOIN jri.gold.dim_cust B ON  A.cust_id = B.cust_id where IsCurrent = 1;
# MAGIC
# MAGIC SELECT * FROM jri.gold.fact_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT txn_id, txn_amt, txn_date, A.cust_id, cust_name, A.cust_id_pk FROM jri.gold.fact_txn A LEFT JOIN jri.gold.dim_cust B ON A.cust_id_pk = B.cust_id_pk;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's say a customer name change comes in for cust id A100 from Jack to Jill

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE cust_input VALUES ("A100","Jill");
# MAGIC
# MAGIC SELECT * FROM cust_input;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A new transaction comes for same cust id A100

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE txn_input VALUES ("TXN003", 300, "2025-11-10", "A100");
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rerun the Merge to populate the dimension table

# COMMAND ----------

# MAGIC %sql
# MAGIC   MERGE INTO jri.gold.dim_cust B
# MAGIC     USING (
# MAGIC             SELECT cust_input.cust_id as merge_key, cust_input.*,current_date() as Effective_Start_Date,NULL as Effective_End_Date,current_timestamp() as processed_timestamp
# MAGIC             FROM cust_input
# MAGIC             UNION ALL
# MAGIC             SELECT NULL as merge_key, cust_input.*,Effective_Start_Date,Effective_End_Date,processed_timestamp
# MAGIC             FROM cust_input
# MAGIC             JOIN jri.gold.dim_cust A ON cust_input.cust_id = A.cust_id
# MAGIC             WHERE A.IsCurrent = 1 and hash(A.cust_name) != hash(cust_input.name)  
# MAGIC           ) staged_updates
# MAGIC         ON B.cust_id = merge_key 
# MAGIC         WHEN MATCHED AND B.IsCurrent = 1 AND hash(B.cust_name) != hash(staged_updates.name) THEN
# MAGIC           UPDATE SET IsCurrent = 0, Effective_End_Date = current_date()
# MAGIC         WHEN NOT MATCHED THEN 
# MAGIC           INSERT (cust_id, cust_name, IsCurrent, Effective_Start_Date, Effective_End_Date, processed_timestamp)
# MAGIC           VALUES (staged_updates.cust_id, staged_updates.name, 1, staged_updates.Effective_Start_Date, NULL, staged_updates.processed_timestamp);
# MAGIC
# MAGIC SELECT * FROM jri.gold.dim_cust;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert the new record into the fact table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO jri.gold.fact_txn 
# MAGIC     SELECT txn_id, txn_amt, txn_date, A.cust_id, B.cust_id_pk FROM txn_input A LEFT JOIN jri.gold.dim_cust B ON  A.cust_id = B.cust_id where IsCurrent = 1;
# MAGIC
# MAGIC SELECT * FROM jri.gold.fact_txn;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Depending on the requriements, we will execute the corresponding query

# COMMAND ----------

# MAGIC %sql
# MAGIC --If we want the transactions that have come for the customer after the update happened
# MAGIC SELECT txn_id, txn_amt, txn_date, A.cust_id, cust_name, A.cust_id_pk FROM jri.gold.fact_txn A LEFT JOIN jri.gold.dim_cust B ON A.cust_id_pk = B.cust_id_pk where IsCurrent = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --If we want all prior transactions but show the cust_name as of the transaction date 
# MAGIC SELECT txn_id, txn_amt, txn_date, A.cust_id, cust_name, A.cust_id_pk FROM jri.gold.fact_txn A LEFT JOIN jri.gold.dim_cust B ON A.cust_id_pk = B.cust_id_pk;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --If we want all prior transactions but show only the latest cust_name 
# MAGIC SELECT txn_id, txn_amt, txn_date, A.cust_id, cust_name, A.cust_id_pk FROM jri.gold.fact_txn A LEFT JOIN jri.gold.dim_cust B ON A.cust_id = B.cust_id where IsCurrent = 1;
