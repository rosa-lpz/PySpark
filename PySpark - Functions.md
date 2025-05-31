

# Get Data (Databricks)

To get data using Databricks, we write PySpark or SQL code within a notebook, and we can access data from many sources like:

* Delta Lake tables
* External databases (via JDBC)
* Cloud storage (S3, ADLS, GCS)
* Databricks-managed tables

```python
# Read a managed or external Delta table
df = spark.read.table("database_name.table_name")

# or using SQL
df = spark.sql("SELECT * FROM database_name.table_name")
```
