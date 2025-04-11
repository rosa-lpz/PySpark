# PySpark Introduction

## What is PySpark?

This collaboration between Python and Apache Spark facilitates data processing and analysis, even for massive data sets. It supports Apache Spark's various features, including its machine learning library (MLlib), DataFrames, and SparkSQL. Using PySpark, you can also transition between Apache Spark and Pandas, perform stream processing and streaming computation, and interface with Java virtual machine (JVM) objects. It is compatible with external libraries, including GraphFrames, which is valuable for efficient graph analysis, and PySparkSQL, which makes tackling massive amounts of data easier.

## What is PySpark used for?

PySpark makes it possible to harness the speed of Apache Spark while processing data on data sets of any size, including massive sizes associated with big data. You can analyze data interactively using the PySpark shell, with performance that’s exponentially faster than if you did it in Python alone. It offers various features, including in-memory computation, fault tolerance, distributed processing, and support for cluster managers like Yarn, Spark, and Mesos.

## What are some PySpark alternatives?

While PySpark is a popular tool among machine learning professionals and data scientists, you have other options to consider. The list below offers a brief synopsis of a few popular PySpark alternatives.

- **Dask:** This Python framework primarily supports Python only but will work with Python-linked code in languages like C++ and Fortran. It offers lighter weight and more flexible performance but lacks PySpark’s all-in-one capabilities.
  
- **Google Cloud Platform:** It provides a serverless, autoscaling option to work with Spark while integrating with Google's array of tools. While PySpark primarily aims to aid DevOps teams, the Google Cloud Platform's robust list of features serves IT professionals, developers, and users of all types. You can use it to work with big data, machine learning, AI, and other computing tasks.
  
- **Polars:** This open-source performance-focused data wrangling solution offers fast installation and support for various data formats, including CSV, JSON, Feather, MySQL, Oracle, Parquet, Azure File, and more. It is a Rust-based solution that relies on Apache Arrow's memory model, enhancing your ability to integrate it with other data tools you're using.
## What are the main advantages of using PySpark over traditional Python for big data processing?

PySpark, the Python API for Apache Spark, offers several advantages over traditional Python for big data processing. 

**Advantages**

- Scalability for handling massive datasets.
- High performance through parallel processing (Distributed Processing).
- Fault tolerance for data reliability.
- Integration with other big data tools within the Apache ecosystem.
- Others
	- Easy to use
	- Framework handles errors
	- Support multiple data formats (CSV, Parquet, JSON, Avro, and others)
	- Machine learning support
	- Real-time stream processing

**Disadvantages**
* Learning Curve
* Slower performance for small datasets
* Not as feature-rich as pandas
* The disadvantages include complicated debugging. PySpark often shows errors in Python code and Java stack, making the process more complex. Finding data quality issues can also be challenging, particularly with large-scale data sets.

## SparkSession in PySpark
In PySpark, `SparkSession` is the entry point to using the Spark functionality, and it’s created using the `SparkSession.builder` API. 

Its main uses include:

- Interacting with Spark SQL to process structured data.
- Creating DataFrames.
- Configuring Spark properties.
- Managing SparkContext and SparkSession lifecycle.


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

spark = SparkSession.builder \
         .appName("MySparkApp") \
         .master("local[*]") \
         .getOrCreate()	
```



# References

* Convert Pandas to PySpark (Spark) DataFrame: https://sparkbyexamples.com/pandas/convert-pandas-to-pyspark-dataframe/
