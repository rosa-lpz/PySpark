Using **PySpark for Exploratory Data Analysis (EDA)** is essential when dealing with **large-scale datasets** that can't fit into memory. PySpark allows distributed processing using the Spark engine and is particularly useful for data profiling, summary statistics, and preprocessing before modeling.

------

## PySpark EDA Workflow

Here's a structured guide to performing EDA with PySpark:

------

### 1. **Initialize Spark Session**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EDA in PySpark") \
    .getOrCreate()
```

------

### 2. **Load and Inspect Data**

```python
# Load CSV or Parquet
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Show sample records
df.show(5)

# Schema overview
df.printSchema()

# Column names
df.columns
```

------

### 3. **Summary Statistics**

```python
# Basic stats for numeric columns
df.describe().show()

# Summary including percentiles (from Spark 3.1+)
df.summary().show()
```

------

### 4. **Missing Values**

```python
from pyspark.sql.functions import col, count, when

# Count missing/nulls per column
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
```

------

### 5. **Value Counts / Frequencies**

```python
# Categorical frequency
df.groupBy("category_column").count().orderBy("count", ascending=False).show()
```

------

### 6. **Correlations (for numeric columns)**

```python
# Pearson correlation
df.stat.corr("col1", "col2")
```

To compute correlation matrix:

```python
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

numeric_cols = ["col1", "col2", "col3"]
vec_assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
vec_df = vec_assembler.transform(df.select(numeric_cols)).select("features")

corr_matrix = Correlation.corr(vec_df, "features").head()[0].toArray()
```

------

### 7. **Distribution Analysis**

Histograms and boxplots aren't built into PySpark but you can compute bin counts:

```python
df.select("numeric_col").rdd.flatMap(lambda x: x).histogram(10)
```

Or convert a small sample to Pandas for visual plots:

```python
pandas_df = df.select("numeric_col").sample(False, 0.01).toPandas()

import matplotlib.pyplot as plt
pandas_df.hist()
plt.show()
```

------

### 8. **Outlier Detection**

Using IQR:

```python
from pyspark.sql.functions import expr

# Approx quantiles
q1, q3 = df.approxQuantile("numeric_col", [0.25, 0.75], 0.01)
iqr = q3 - q1

# Filter outliers
outliers = df.filter((col("numeric_col") < (q1 - 1.5 * iqr)) | (col("numeric_col") > (q3 + 1.5 * iqr)))
outliers.show()
```

------

### 9. **Feature Relationships**

```python
# Grouped statistics
df.groupBy("category_col").agg({"numeric_col": "mean"}).show()
```

------

### 10. **Time Series Aggregation (if applicable)**

```python
from pyspark.sql.functions import to_timestamp, window

df = df.withColumn("timestamp", to_timestamp("timestamp_col"))
df.groupBy(window("timestamp", "1 hour")).agg({"value": "mean"}).show()
```

------

## Caching and Sampling

```python
df.cache()  # Speeds up repeated operations
df_sample = df.sample(False, 0.01, seed=42)  # Use small sample for quick exploration
```

## Limitations & Tips

- **No native plotting**: Use sampling + Pandas for plots (`df.limit(n).toPandas()`).
- **Spark SQL**: Useful for complex queries with `.createOrReplaceTempView("df")` and SQL.
- **Use Notebooks**: Jupyter or Databricks are great environments for PySpark EDA.

------

