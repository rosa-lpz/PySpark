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
## Change Column Names & DataTypes while Converting

If we want to change the schema (column name & data type) while converting pandas to PySpark DataFrame, we can create a [PySpark Schema using StructType](https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/) and use it for the schema.

```python
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
#Create User defined Custom Schema using StructType
mySchema = StructType([ StructField("First Name", StringType(), True)\
                       ,StructField("Age", IntegerType(), True)])

#Create DataFrame by changing schema
sparkDF2 = spark.createDataFrame(pandasDF,schema=mySchema)
sparkDF2.printSchema()
sparkDF2.show()

#Outputs below schema & DataFrame

root
 |-- First Name: string (nullable = true)
 |-- Age: integer (nullable = true)

+----------+---+
|First Name|Age|
+----------+---+
|     Scott| 50|
|      Jeff| 45|
|    Thomas| 54|
|       Ann| 34|
+----------+---+
```

## Use Apache Arrow to Convert Pandas to Spark DataFrame

Using Apache Arrow to convert a Pandas DataFrame to a Spark DataFrame involves leveraging Arrow’s efficient in-memory columnar representation for data interchange between Pandas and Spark. This process enhances performance by minimizing data serialization and deserialization overhead.

To accomplish this conversion, first, ensure that both Pandas and PySpark are Arrow-enabled. Then, utilize Arrow’s capabilities to directly convert Pandas DataFrame to Arrow format, followed by converting Arrow format to Spark DataFrame using PyArrow.

Install `pip install pyspark[sql]` or by directly downloading from [Apache Arrow for Python](https://arrow.apache.org/docs/python/install.html) to work with Arrow.

```python
spark.conf.set("spark.sql.execution.arrow.enabled","true")
sparkDF=spark.createDataFrame(pandasDF) 
sparkDF.printSchema()
sparkDF.show()
```

To utilize the above approach, it’s necessary to have [Apache Arrow installed and compatible with Spark](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#recommended-pandas-and-pyarrow-versions). If Apache Arrow is not installed, you will encounter the following error message.

```python
\apps\Anaconda3\lib\site-packages\pyspark\sql\pandas\conversion.py:289: UserWarning: createDataFrame attempted Arrow optimization because 'spark.sql.execution.arrow.pyspark.enabled' is set to true; however, failed by the reason below:
  PyArrow >= 0.15.1 must be installed; however, it was not found.
Attempting non-optimization as 'spark.sql.execution.arrow.pyspark.fallback.enabled' is set to true.
```

In the event of an error, Spark will automatically revert to its non-Arrow optimization implementation. This behavior can be managed through the `spark.sql.execution.arrow.pyspark.fallback.enabled parameter`.

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
```

**Note** that Apache Arrow does not support complex types like  `MapType`, `ArrayType` of `TimestampType`, and nested [StructType](https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield).

## Complete Example of Convert Pandas to Spark DataFrame

```python
import pandas as pd    
data = [['Scott', 50], ['Jeff', 45], ['Thomas', 54],['Ann',34]] 
  
# Create the pandas DataFrame 
pandasDF = pd.DataFrame(data, columns = ['Name', 'Age']) 
  
# print dataframe. 
print(pandasDF)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

sparkDF=spark.createDataFrame(pandasDF) 
sparkDF.printSchema()
sparkDF.show()

#sparkDF=spark.createDataFrame(pandasDF.astype(str)) 
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
mySchema = StructType([ StructField("First Name", StringType(), True)\
                       ,StructField("Age", IntegerType(), True)])

sparkDF2 = spark.createDataFrame(pandasDF,schema=mySchema)
sparkDF2.printSchema()
sparkDF2.show()

# Enable Apache Arrow to convert Pandas to PySpark DataFrame
spark.conf.set("spark.sql.execution.arrow.enabled","true")
sparkDF2=spark.createDataFrame(pandasDF) 
sparkDF2.printSchema()
sparkDF2.show()

#Convert PySpark DataFrame to Pandas
pandasDF2=sparkDF2.select("*").toPandas
print(pandasDF2)
```

# References

* Convert Pandas to PySpark (Spark) DataFrame: https://sparkbyexamples.com/pandas/convert-pandas-to-pyspark-dataframe/
