Using **PySpark for Exploratory Data Analysis (EDA)** is essential when dealing with **large-scale datasets** that can't fit into memory. PySpark allows distributed processing using the Spark engine and is particularly useful for data profiling, summary statistics, and preprocessing before modeling.

------

# PySpark EDA Workflow

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



Great! Let's walk through how to do **Exploratory Data Analysis (EDA) for Time Series using PySpark**. Time series EDA has unique aspects, like trend analysis, seasonality, missing timestamps, and temporal aggregations. Here's a **PySpark-centric workflow**, with optional Pandas+Matplotlib for visualization.

------

# PySpark Time Series EDA Workflow

### 0. **Setup SparkSession**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TimeSeriesEDA") \
    .getOrCreate()
```

------

### 1. **Load and Prepare Data**

```python
from pyspark.sql.functions import to_timestamp

# Assume you have a CSV with a datetime column
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Convert string to timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Sort by time
df = df.orderBy("timestamp")
df.printSchema()
```

------

### 2. **Check for Missing Timestamps (Time Gaps)**

Detect gaps if you expect regular intervals (e.g., hourly):

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, col

windowSpec = Window.orderBy("timestamp")
df = df.withColumn("prev_timestamp", lag("timestamp").over(windowSpec))
df = df.withColumn("gap_seconds", 
    (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")))

df.select("timestamp", "prev_timestamp", "gap_seconds").filter(col("gap_seconds") > 3600).show()
```

------

### 3. **Resample / Aggregate Over Time Windows**

Aggregate values by fixed time windows (e.g., hourly/daily mean):

```python
from pyspark.sql.functions import window, avg

# Group by 1-hour window
agg_df = df.groupBy(window("timestamp", "1 hour")) \
           .agg(avg("value").alias("mean_value")) \
           .orderBy("window")

agg_df.show()
```

------

### 4. **Extract Time-based Features**

```python
from pyspark.sql.functions import hour, dayofweek, dayofmonth, month

df = df.withColumn("hour", hour("timestamp")) \
       .withColumn("weekday", dayofweek("timestamp")) \
       .withColumn("day", dayofmonth("timestamp")) \
       .withColumn("month", month("timestamp"))

df.select("timestamp", "hour", "weekday", "month").show(5)
```

------

### 5. **Rolling Statistics (Window-based)**

Use Spark window functions to compute rolling features (e.g., moving average):

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, stddev

rolling_window = Window.orderBy("timestamp").rowsBetween(-23, 0)

df = df.withColumn("rolling_avg_24", avg("value").over(rolling_window))
df = df.withColumn("rolling_std_24", stddev("value").over(rolling_window))
```

------

### 6. **Trend & Seasonality Checks**

Check average values by hour/day:

```python
# Hourly average (seasonality)
df.groupBy("hour").agg(avg("value")).orderBy("hour").show()

# Day of week average
df.groupBy("weekday").agg(avg("value")).orderBy("weekday").show()
```

------

### 7. **Convert to Pandas for Visualization**

For plotting (Spark can't do it directly):

```python
# Downsample or limit first!
pandas_df = df.select("timestamp", "value", "rolling_avg_24").dropna().limit(1000).toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(12, 5))
plt.plot(pandas_df["timestamp"], pandas_df["value"], label="Original")
plt.plot(pandas_df["timestamp"], pandas_df["rolling_avg_24"], label="24-point Rolling Mean")
plt.title("Time Series EDA")
plt.xlabel("Time")
plt.ylabel("Value")
plt.legend()
plt.tight_layout()
plt.show()
```

------

### 8. **Outlier Detection (Optional)**

Use IQR-based logic to flag potential outliers:

```python
q1, q3 = df.approxQuantile("value", [0.25, 0.75], 0.01)
iqr = q3 - q1
outliers = df.filter((col("value") < (q1 - 1.5 * iqr)) | (col("value") > (q3 + 1.5 * iqr)))
outliers.select("timestamp", "value").show()
```

------

## Summary: Key Techniques for Time Series EDA in PySpark

| Technique                  | PySpark Support           |
| -------------------------- | ------------------------- |
| Missing timestamps         | ✅ via lag()               |
| Rolling windows            | ✅ with Window             |
| Time-based features        | ✅ hour(), day()           |
| Resampling                 | ✅ window()                |
| Trend/Seasonality analysis | ✅ via grouping            |
| Plotting                   | ❌ (use Pandas+Matplotlib) |
| Outlier detection          | ✅ with approxQuantile     |

------

Would you like this wrapped as a reusable PySpark EDA script or notebook template? Or tailored for anomaly detection/forecasting next?
