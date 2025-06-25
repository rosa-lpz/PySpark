Perfect — let’s extend the **Time Series EDA in PySpark** into **Anomaly Detection and Forecasting**. Here's a practical, scalable workflow using **PySpark for preprocessing**, with **optional MLlib or Pandas-based modeling** depending on your dataset size and use case.

------

## 🔍 Anomaly Detection & Forecasting with PySpark

### 📌 Assumptions

- Time series: `timestamp`, `value`
- Data is **regularly sampled** (e.g., hourly/daily)
- You're working in a distributed environment (e.g., Spark or Databricks)

------

## Part 1: Preprocessing with PySpark

```python
from pyspark.sql.functions import to_timestamp, col, avg, stddev, window
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Convert to timestamp & sort
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.orderBy("timestamp")

# Add rolling stats
rolling_window = Window.orderBy("timestamp").rowsBetween(-23, 0)
df = df.withColumn("rolling_mean", avg("value").over(rolling_window))
df = df.withColumn("rolling_std", stddev("value").over(rolling_window))
```

------

## Part 2: Anomaly Detection (Rolling Z-score Method)

This simple, unsupervised method works well for detecting **spikes or dips**.

```python
df = df.withColumn("z_score", 
                   (col("value") - col("rolling_mean")) / col("rolling_std"))

# Mark as anomaly if |z| > threshold
threshold = 3
df = df.withColumn("anomaly", (F.abs(col("z_score")) > threshold).cast("int"))

df.select("timestamp", "value", "z_score", "anomaly").filter("anomaly = 1").show()
```

------

## Optional: Advanced Anomaly Detection (Isolation Forest or Autoencoder)

PySpark MLlib supports models like Random Forest, but not Isolation Forest. So you'd:

- Extract engineered features in PySpark
- Convert a subset to Pandas
- Use `scikit-learn`, `PyOD`, or `Torch` for modeling

```python
features_df = df.select("value", "rolling_mean", "rolling_std", "z_score") \
                .dropna() \
                .limit(10000) \
                .toPandas()

# Then use sklearn or PyOD
from sklearn.ensemble import IsolationForest

model = IsolationForest(contamination=0.01)
features_df["anomaly"] = model.fit_predict(features_df[["value", "z_score"]])
```

------

## Part 3: Forecasting with PySpark (Option 1: MLlib Regression)

PySpark doesn’t have ARIMA/LSTM natively, but you can use **lag features + regression** as a baseline forecaster:

```python
from pyspark.sql.functions import lag
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create lag features
lags = [1, 2, 3]
for lag_i in lags:
    df = df.withColumn(f"lag_{lag_i}", lag("value", lag_i).over(rolling_window))

# Remove nulls
df_model = df.dropna()

# Assemble features
assembler = VectorAssembler(inputCols=[f"lag_{i}" for i in lags], outputCol="features")
df_model = assembler.transform(df_model)

# Train model
lr = LinearRegression(featuresCol="features", labelCol="value")
model = lr.fit(df_model)

# Predict future values
predictions = model.transform(df_model)
predictions.select("timestamp", "value", "prediction").show()
```

------

## Optional: Deep Learning Forecasting (LSTM, Prophet, etc.)

If you want to use **advanced forecasting models**, use:

- **Prophet** (in Pandas or Spark-TS)
- **GluonTS, DeepAR, TFT** in PyTorch/Deep Learning frameworks
- **Databricks Forecasting Solutions Accelerator**

For example, using Pandas:

```python
# Convert to Pandas
pdf = df.select("timestamp", "value").dropna().toPandas()
pdf.columns = ["ds", "y"]

from prophet import Prophet
model = Prophet()
model.fit(pdf)

future = model.make_future_dataframe(periods=24, freq='H')
forecast = model.predict(future)

model.plot(forecast)
```

------

## Visualization of Forecast vs. Actual

```python
import matplotlib.pyplot as plt

# If using Pandas forecast
plt.figure(figsize=(12, 6))
plt.plot(pdf["ds"], pdf["y"], label="Actual")
plt.plot(forecast["ds"], forecast["yhat"], label="Forecast")
plt.fill_between(forecast["ds"], forecast["yhat_lower"], forecast["yhat_upper"], alpha=0.2)
plt.legend()
plt.title("Forecast vs Actual")
plt.show()
```

------

## Summary Table

| Task                      | PySpark Support      | Comments               |
| ------------------------- | -------------------- | ---------------------- |
| Rolling features          | ✅ Yes (Window)       | For Z-score, smoothing |
| Z-score anomaly detection | ✅ Yes                | Easy and effective     |
| Isolation Forest          | ❌ Use `sklearn`/PyOD | Convert to Pandas      |
| Forecasting (regression)  | ✅ MLlib              | Basic autoregression   |
| Deep forecasting models   | ❌ External libraries | Use Pandas/PyTorch     |

------

Would you like:

- A full notebook combining this end-to-end?
- Deployment tips (e.g., anomaly alerts in production)?
- Forecasting with seasonal adjustment?

Let me know!
