# PySpark - DataFrame

# Introduction 

Spark SQL works with DataFrames. A DataFrame is a relational representation of data. It provides functions with SQL-like capabilities. It also allows you to write SQL-like queries for your data analysis.

DataFrames are similar to relational tables in Python/R, although they implement many optimizations that are hidden from the user. There are several ways to create DataFrames from collections, HIVE tables, relational tables, and RDDs.

```python
import findspark
findspark.init()

  
import pandas as pd
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
```

Create session
```python
spark = SparkSession.builder.getOrCreate()
```


# DataFrame Creation

**Syntax**

```python
df = spark.createDataFrame(DF_name, ["column1", "column2"])
```

## Example 1

```python
emp = [(1, "AAA", "dept1", 1000),
    (2, "BBB", "dept1", 1100),
    (3, "CCC", "dept1", 3000),
    (4, "DDD", "dept1", 1500),
    (5, "EEE", "dept2", 8000),
    (6, "FFF", "dept2", 7200),
    (7, "GGG", "dept3", 7100),
    (8, "HHH", "dept3", 3700),
    (9, "III", "dept3", 4500),
    (10, "JJJ", "dept5", 3400)]

dept = [("dept1", "Department - 1"),
        ("dept2", "Department - 2"),
        ("dept3", "Department - 3"),
        ("dept4", "Department - 4")

       ]

                                    
df = spark.createDataFrame(emp, ["id", "name", "dept", "salary"])

deptdf = spark.createDataFrame(dept, ["id", "name"]) 
```


df.show()

```python
+---+----+-----+------+
| id|name| dept|salary|
+---+----+-----+------+
|  1| AAA|dept1|  1000|
|  2| BBB|dept1|  1100|
|  3| CCC|dept1|  3000|
|  4| DDD|dept1|  1500|
|  5| EEE|dept2|  8000|
|  6| FFF|dept2|  7200|
|  7| GGG|dept3|  7100|
|  8| HHH|dept3|  3700|
|  9| III|dept3|  4500|
| 10| JJJ|dept5|  3400|
+---+----+-----+------+
```

deptdf.show()
```python
+-----+--------------+
|   id|          name|
+-----+--------------+
|dept1|Department - 1|
|dept2|Department - 2|
|dept3|Department - 3|
|dept4|Department - 4|
+-----+--------------+
```


# Basic operations in DataFrames


| Operation        | Description                                 | Example                         |
| ---------------- | ------------------------------------------- | ------------------------------- |
| df.count()       | Counts row number                           |                                 |
| df.columns()     | Return the column names                     |                                 |
| df.dtypes        | Access to the dataype inside the DF columns |                                 |
| df.schema        | Check how Spark stores the DataFrame schema |                                 |
| df.printSchema() | Prints the schema                           |                                 |
| df.select()      | Select columns of a DataFrame               | df.select("c1,c2").show()       |
| df.filter()      | filter a Dataframe                          | df.filter(df["id"] == 1).show() |
| df.drop()        | Remove a particular column                  | df.drop("colum1")               |
|                  |                                             |                                 |
|                  |                                             |                                 |

## Data Types

```python
df.dtypes
```


Output

```python
[('id', 'bigint'),
 ('name', 'string'),
 ('dept', 'string'),
 ('salary', 'bigint')]
```

## Schema


```python
df.printSchema()
```


Output

```python
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: long (nullable = true)
```

## Select
Select columns of the DataFrame

```python
df.select("id", "name").show()
```


Output

```python
+---+----+
| id|name|
+---+----+
|  1| AAA|
|  2| BBB|
|  3| CCC|
|  4| DDD|
|  5| EEE|
|  6| FFF|
|  7| GGG|
|  8| HHH|
|  9| III|
| 10| JJJ|
+---+----+
```

## Filter
Filter rows based on a condition.
* Let's try to find rows with id = 1.
There are several ways to specify the condition.

```python
df.filter(df["id"] == 1).show()

df.filter(df.id == 1).show()
```


Output

```python
+---+----+-----+------+
| id|name| dept|salary|
+---+----+-----+------+
|  1| AAA|dept1|  1000|
+---+----+-----+------+

+---+----+-----+------+
| id|name| dept|salary|
+---+----+-----+------+
|  1| AAA|dept1|  1000|
+---+----+-----+------+
```

## Drop

Deletes a particular row

```python
newdf = df.drop("id")

newdf.show(2)
```


Output

```python
+----+-----+------+
|name| dept|salary|
+----+-----+------+
| AAA|dept1|  1000|
| BBB|dept1|  1100|
+----+-----+------+
only showing top 2 rows
```


## Aggregations
We can use the groupBy function to group the data and then use the "agg" function to perform aggregation of grouped data.


## Convert Pandas to PySpark (Spark) DataFrame

Convert Pandas to PySpark (Spark) DataFrame
```python
import pandas as pd
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Example").getOrCreate()

# Create Pandas DataFrame
pdf = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})

# Convert to PySpark DataFrame
df_spark = spark.createDataFrame(pdf)

# Convert back to Pandas DataFrame
pdf_new = df_spark.toPandas()Powered By 
```
# References

* Convert Pandas to PySpark (Spark) DataFrame: https://sparkbyexamples.com/pandas/convert-pandas-to-pyspark-dataframe/
