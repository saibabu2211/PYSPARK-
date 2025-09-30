PySpark Master Workbook Structure
🔰 Module 1: Basics
What is PySpark and Spark architecture

Creating RDDs and DataFrames

DataFrame vs RDD comparison

Actions vs Transformations

📥 Module 2: Reading & Writing Data
Read CSV, JSON, Parquet, Avro, ORC

Schema enforcement (manual + DDL)

Error handling: DROPMALFORMED, badRecordsPath

Write operations: modes, partitioning, formats

🧪 Module 3: DataFrame Transformations
select, withColumn, drop, filter, distinct

orderBy, sort, limit

when, otherwise, expr, lit

groupBy, agg, pivot

🔗 Module 4: Joins & Set Operations
Join types: inner, left, right, outer, semi, anti

Join on multiple columns

Broadcast joins

union, unionAll, intersect, subtract

🪟 Module 5: Window Functions
partitionBy, orderBy

row_number, rank, dense_rank, lead, lag

Aggregations over windows

🧠 Module 6: Advanced Transformations
UDFs: creation, registration, performance tips

flatMap, map, reduce

SQL functions: expr, selectExpr, explode, array, map

🧰 Module 7: Memory & Performance
repartition, coalesce

cache, persist

Shuffle optimization

Skew handling

Partition pruning

🏗️ Module 8: Data Engineering Workflows
Delta Lake: read/write, schema evolution, time travel

Partitioning and bucketing

Null handling and data cleaning

Integration with Azure Blob, GCP Buckets

🧹 Module 9: Job Orchestration
Apache Airflow DAGs

Databricks Jobs UI

Monitoring with job clusters

🔄 Module 10: Structured Streaming
Read/write streaming data

Kafka integration

Watermarking and windowed aggregations

🧩 Bonus Tools & Templates
✅ Reusable ingestion function with schema enforcement

✅ Transformation pipeline with joins, windows, and aggregations

✅ Write module with partitioning and overwrite logic

✅ Logging and error handling wrapper

✅ Config-driven pipeline template

🧠 Learning Resources
Spark By Examples

GeeksforGeeks PySpark

Databricks Academy

PySpark GitHub Learning Repo

I’ll start building each module with code, examples, and exercises. Would you like me to begin with Module 1: Basics, or jump straight into Module 2: Reading & Writing Data since you’re already working with ingestion?

yes module 1
Fantastic, Saibabu — let’s kick off Module 1: PySpark Basics of your Master Workbook. This module lays the foundation for everything that follows: understanding Spark architecture, creating RDDs and DataFrames, and mastering basic transformations and actions.

🔰 Module 1: PySpark Basics
🧠 1. What Is PySpark?
PySpark is the Python API for Apache Spark — a distributed computing engine built for big data processing. It supports:

In-memory computation for speed

Lazy evaluation for optimization

Fault tolerance via lineage

Parallelism across clusters

Spark has 5 core components:

Spark Core (RDDs)

Spark SQL (DataFrames)

Spark Streaming

MLlib (Machine Learning)

GraphX (Graph processing)

⚙️ 2. SparkSession Setup
python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySparkBasics") \
    .getOrCreate()
This is your entry point to all Spark functionality. You only need one SparkSession per app.

🔁 3. Creating RDDs
RDD = Resilient Distributed Dataset (low-level abstraction)

python
rdd = spark.sparkContext.parallelize([("Toyota", 2018), ("Honda", 2020)])
rdd.collect()
Transformations:

python
rdd2 = rdd.map(lambda x: (x[0], x[1] + 1))
Actions:

python
rdd2.count()
rdd2.take(1)
📊 4. Creating DataFrames
From list of tuples:

python
data = [(1, "Toyota", 2018, 15000.0), (2, "Honda", 2020, 18000.0)]
columns = ["car_id", "brand", "year", "price"]
df = spark.createDataFrame(data, columns)
df.show()
From RDD:

python
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
🧬 5. DataFrame vs RDD
Feature	RDD	DataFrame
Abstraction	Low-level	High-level
Schema	No	Yes
Optimization	Manual	Catalyst engine
Ease of use	Functional only	SQL + functions
Use RDDs for fine-grained control; use DataFrames for performance and simplicity.

🧪 6. Basic Transformations
python
df.select("brand", "price").show()
df.filter(df.price > 16000).show()
df.withColumn("discounted", df.price * 0.9).show()
🧠 7. Basic Actions
python
df.show()
df.count()
df.collect()
df.head()
df.take(2)
🧾 8. Print Schema and Metadata
python
df.printSchema()
df.columns
df.dtypes
🧰 9. Convert to Pandas
python
pdf = df.toPandas()  # Use only for small datasets
🧠 10. Practice Exercise
Try this:

python
data = [(1, "Ford", 2019, 22000.0), (2, "Hyundai", 2021, 17000.0)]
columns = ["car_id", "brand", "year", "price"]
df = spark.createDataFrame(data, columns)

df.withColumn("taxed_price", df.price * 1.18).show()
Would you like me to generate a quiz or workbook exercise set for Module 1 next? Or move on to Module 2: Reading & Writing Data?

yes
Here’s your interactive quiz for Module 1: PySpark Basics — designed to reinforce your understanding of Spark architecture, RDDs, DataFrames, transformations, and actions. Let’s test your mastery 💪

Quiz

Amazing work!
You got 100% right.
Review explanationsTake another quiz
module 2
Let’s dive into Module 2: Reading & Writing Data in PySpark, Saibabu — this is the backbone of any data pipeline. You’ll learn how to ingest structured and semi-structured data, enforce schemas, handle errors, and write clean outputs in multiple formats.

📥 Module 2: Reading Data
✅ 1. Read CSV
python
df = spark.read.csv("cars.csv", header=True, inferSchema=True)
Options:

python
spark.read.option("header", True) \
          .option("inferSchema", True) \
          .option("sep", ",") \
          .option("nullValue", "NA") \
          .csv("cars.csv")
✅ 2. Read JSON
python
df = spark.read.json("cars.json")
Multiline JSON:

python
spark.read.option("multiline", True).json("nested.json")
✅ 3. Read Parquet
python
df = spark.read.parquet("cars.parquet")
✅ 4. Read Text
python
df = spark.read.text("notes.txt")
✅ 5. Read with Manual Schema
python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("brand", StringType(), True),
    StructField("model", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

df = spark.read.csv("cars.csv", header=True, schema=schema)
✅ 6. Read with DDL String
python
schema_str = "car_id INT, brand STRING, model STRING, year INT, price DOUBLE"
df = spark.read.schema(schema_str).csv("cars.csv", header=True)
✅ 7. Error Handling
python
spark.read.option("mode", "DROPMALFORMED").csv("cars.csv")
spark.read.option("badRecordsPath", "/tmp/bad_rows").csv("cars.csv")
spark.read.option("mode", "FAILFAST").csv("cars.csv")
✅ 8. Read from RDD
python
rdd = sc.parallelize([(1, "Toyota", "Corolla", 2018, 15000.0)])
df = spark.createDataFrame(rdd, schema=schema)
✅ 9. Read from JDBC
python
df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/cars",
    driver="com.mysql.jdbc.Driver",
    dbtable="car_table",
    user="root",
    password="password"
).load()
💾 Module 2: Writing Data
✅ 1. Write CSV
python
df.write.mode("overwrite").csv("output/cars_csv")
Modes:

overwrite

append

ignore

error (default)

✅ 2. Write Parquet
python
df.write.parquet("output/cars_parquet")
✅ 3. Write JSON
python
df.write.json("output/cars_json")
✅ 4. Partitioned Write
python
df.write.partitionBy("year").parquet("output/cars_by_year")
✅ 5. Write to JDBC
python
df.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/cars",
    driver="com.mysql.jdbc.Driver",
    dbtable="car_table",
    user="root",
    password="password"
).mode("append").save()
✅ 6. Write with Compression
python
df.write.option("compression", "snappy").parquet("output/compressed")
🧠 Practice Exercise
python
schema = "car_id INT, brand STRING, model STRING, year INT, price DOUBLE"
df = spark.read.schema(schema).csv("cars.csv", header=True)

df.write.mode("overwrite").partitionBy("year").parquet("output/cars_partitioned")
Would you like a quiz for Module 2 next, or jump into Module 3: Transformations with select, withColumn, filter, and more? I can also build a reusable ingestion + write function for your car pipeline.

explain module 2 in detail
Absolutely, Saibabu — here’s your deep-dive into Module 2: Reading & Writing Data in PySpark, crafted for production-grade ingestion pipelines. This module covers every nuance of how PySpark interacts with structured and semi-structured data, with schema enforcement, error handling, partitioning, and output strategies.

📥 Part 1: Reading Data in PySpark
🔹 1. Reading CSV Files
python
df = spark.read.csv("cars.csv", header=True, inferSchema=True)
🔧 Common Options:
Option	Purpose
header=True	Treat first row as column names
inferSchema=True	Automatically detect data types
sep=","	Specify delimiter (e.g., tab: \t)
nullValue="NA"	Treat "NA" as null
quote='"'	Handle quoted fields
escape="\\"	Escape special characters
multiLine=True	Parse multi-line records (slow)
🔹 2. Reading JSON Files
python
df = spark.read.json("cars.json")
🔧 For nested or multiline JSON:
python
df = spark.read.option("multiline", True).json("nested.json")
🔹 3. Reading Parquet Files
python
df = spark.read.parquet("cars.parquet")
✅ Parquet is columnar, compressed, and ideal for big data.

🔹 4. Reading with Manual Schema
python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("brand", StringType(), True),
    StructField("model", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

df = spark.read.csv("cars.csv", header=True, schema=schema)
✅ Manual schema is faster and safer than inferSchema.

🔹 5. Reading with DDL String
python
schema_str = "car_id INT, brand STRING, model STRING, year INT, price DOUBLE"
df = spark.read.schema(schema_str).csv("cars.csv", header=True)
✅ Great for dynamic schema generation.

🔹 6. Error Handling During Read
Mode	Behavior
PERMISSIVE	Default; fills corrupt rows with nulls
DROPMALFORMED	Drops malformed rows
FAILFAST	Fails immediately on bad records
python
df = spark.read.option("mode", "DROPMALFORMED").csv("cars.csv")
df = spark.read.option("badRecordsPath", "/tmp/bad_rows").csv("cars.csv")
🔹 7. Reading from RDD
python
rdd = sc.parallelize([(1, "Toyota", "Corolla", 2018, 15000.0)])
df = spark.createDataFrame(rdd, schema=schema)
✅ Useful for synthetic data or intermediate transformations.

🔹 8. Reading from JDBC (Databases)
python
df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/cars",
    driver="com.mysql.jdbc.Driver",
    dbtable="car_table",
    user="root",
    password="password"
).load()
✅ Ideal for integrating with MySQL, PostgreSQL, Oracle, etc.

💾 Part 2: Writing Data in PySpark
🔹 1. Write CSV
python
df.write.mode("overwrite").csv("output/cars_csv")
Modes:
overwrite: replaces existing files

append: adds to existing files

ignore: skips if path exists

error (default): throws error if path exists

🔹 2. Write Parquet
python
df.write.parquet("output/cars_parquet")
✅ Recommended for analytics and storage efficiency.

🔹 3. Write JSON
python
df.write.json("output/cars_json")
🔹 4. Partitioned Write
python
df.write.partitionBy("year").parquet("output/cars_by_year")
✅ Improves query performance by pruning irrelevant partitions.

🔹 5. Write to JDBC
python
df.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/cars",
    driver="com.mysql.jdbc.Driver",
    dbtable="car_table",
    user="root",
    password="password"
).mode("append").save()
🔹 6. Write with Compression
python
df.write.option("compression", "snappy").parquet("output/compressed")
✅ Supported codecs: snappy, gzip, lz4, zstd

🧠 Pro Tips for Production
Always define schema manually for speed and safety

Use partitioned writes for large datasets

Avoid inferSchema in production

Use badRecordsPath for audit trails

Prefer Parquet for analytics, CSV for interoperability

Would you like me to generate a quiz for Module 2 next, or move on to Module 3: Transformations with select, withColumn, filter, groupBy, and more? I can also wrap this into a reusable ingestion + write class for your car analytics pipeline.

how to read bunch of parquet files in folder and writeen as single file
To read a bunch of Parquet files from a folder and write them out as a single consolidated file, here’s your bulletproof PySpark approach, Saibabu:

✅ Step-by-Step Workflow
🔹 1. Read All Parquet Files in a Folder
python
df = spark.read.parquet("path/to/folder/")
This automatically reads all .parquet files in that folder into a single DataFrame.

🔹 2. Coalesce to a Single Partition
python
df_single = df.coalesce(1)
✅ This ensures the output is written as one file, not multiple part-files.

🔹 3. Write as a Single Parquet File
python
df_single.write.mode("overwrite").parquet("path/to/output/single_file/")
💡 The output folder will contain:

A single .parquet data file

_SUCCESS marker

Metadata files

🧠 Pro Tips
Use coalesce(1) only for small to medium datasets — it forces all data into one partition.

For large datasets, consider writing multiple files and merging externally.

You can also write as CSV or JSON using the same pattern:

python
df_single.write.csv("output.csv")  # writes one CSV file
Would you like me to wrap this into a reusable function or add schema enforcement and logging? I can also show how to merge multiple folders or filter files by timestamp.

for all csv and json files same exection
Perfect, Saibabu — here’s a unified PySpark ingestion and consolidation script that reads all CSV and JSON files from a folder, merges them into a single DataFrame, and writes the result as a single output file (CSV, JSON, or Parquet). This is ideal for batch ingestion pipelines.

🧩 PySpark: Read All CSV & JSON Files → Write One File
python
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

# 1. Start Spark session
spark = SparkSession.builder.appName("BatchIngestion").getOrCreate()

# 2. Define input folders
csv_folder = "path/to/csv_folder/"
json_folder = "path/to/json_folder/"

# 3. Read all CSV files
csv_df = spark.read.option("header", True).option("inferSchema", True).csv(csv_folder)

# 4. Read all JSON files
json_df = spark.read.option("multiline", True).json(json_folder)

# 5. Union both DataFrames (must have same schema)
merged_df = csv_df.unionByName(json_df)

# 6. Coalesce to single partition
single_df = merged_df.coalesce(1)

# 7. Write as single output file (choose format)
single_df.write.mode("overwrite").csv("output/merged_csv")      # CSV
# single_df.write.mode("overwrite").json("output/merged_json")  # JSON
# single_df.write.mode("overwrite").parquet("output/merged_parquet")  # Parquet
🧠 Notes
✅ csv() and json() automatically read all matching files in the folder.

✅ unionByName() merges DataFrames by column name (safe even if order differs).

✅ coalesce(1) ensures a single output file.

✅ You can switch output format by changing .csv() to .json() or .parquet().

Would you like this wrapped into a reusable function or class with schema enforcement, logging, and file type detection? I can also add filtering by filename, timestamp, or file size.

what is difference between multiline and single line in json
Great question, Saibabu — understanding the difference between multiline and single-line JSON is crucial for correctly ingesting semi-structured data in PySpark.

🧾 Single-line JSON
Each line in the file is a complete JSON object.

🔹 Example:
json
{"car_id": 1, "brand": "Toyota", "year": 2018}
{"car_id": 2, "brand": "Honda", "year": 2020}
✅ Use Case:
Flat JSON structure

Logs or streaming data

Can be parsed line-by-line

✅ PySpark Read:
python
df = spark.read.json("cars.json")
📄 Multiline JSON
The entire file (or block) is a single JSON object or array, possibly nested.

🔹 Example:
json
[
  {"car_id": 1, "brand": "Toyota", "year": 2018},
  {"car_id": 2, "brand": "Honda", "year": 2020}
]
Or:

json
{
  "cars": [
    {"car_id": 1, "brand": "Toyota"},
    {"car_id": 2, "brand": "Honda"}
  ]
}
❌ Without multiline option, PySpark will fail to parse this.
✅ PySpark Read:
python
df = spark.read.option("multiline", True).json("nested.json")
🧠 Summary
Format	Structure	PySpark Option Needed
Single-line	One JSON per line	multiline=False (default)
Multiline	Nested or array JSON	multiline=True
Would you like me to show how to flatten nested JSON or extract arrays using explode() and selectExpr() next? That’s often the next challenge after reading multiline JSON.

give previous three into single
Here’s your unified, production-ready PySpark module, Saibabu — it reads multiple CSV, JSON, or Parquet files from folders, merges them into a single DataFrame, and writes the result as a single output file in your chosen format. This is ideal for batch ingestion pipelines, audit-ready exports, and scalable analytics.

🧩 PySpark: Multi-Format Ingestion → Single File Writer
python
from pyspark.sql import SparkSession

def read_and_write_single_file(
    spark,
    input_path,
    file_type="csv",           # Options: 'csv', 'json', 'parquet'
    output_path="output/",
    output_format="parquet",   # Options: 'csv', 'json', 'parquet'
    schema=None,               # Optional: StructType or DDL string
    options=None               # Optional: read options as dict
):
    """
    Reads multiple files of a given type from a folder, merges them, and writes as a single file.
    """

    # 1. Configure reader
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    if options:
        for key, value in options.items():
            reader = reader.option(key, value)

    # 2. Read files by type
    if file_type == "csv":
        df = reader.csv(input_path)
    elif file_type == "json":
        df = reader.json(input_path)
    elif file_type == "parquet":
        df = reader.parquet(input_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # 3. Coalesce to single partition
    df_single = df.coalesce(1)

    # 4. Write as single file
    if output_format == "csv":
        df_single.write.mode("overwrite").option("header", True).csv(output_path)
    elif output_format == "json":
        df_single.write.mode("overwrite").json(output_path)
    elif output_format == "parquet":
        df_single.write.mode("overwrite").parquet(output_path)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    print(f"✅ Written single {output_format} file to: {output_path}")
🧪 Example Usage
python
spark = SparkSession.builder.appName("UnifiedIngestion").getOrCreate()

read_and_write_single_file(
    spark=spark,
    input_path="data/cars_csv/",
    file_type="csv",
    output_path="output/merged_csv",
    output_format="csv",
    options={"header": "true", "inferSchema": "true"}
)

read_and_write_single_file(
    spark=spark,
    input_path="data/cars_json/",
    file_type="json",
    output_path="output/merged_json",
    output_format="json",
    options={"multiline": "true"}
)

read_and_write_single_file(
    spark=spark,
    input_path="data/cars_parquet/",
    file_type="parquet",
    output_path="output/merged_parquet",
    output_format="parquet"
)
🧠 Bonus Features You Can Add
✅ Schema enforcement (StructType or DDL)

✅ Logging malformed rows

✅ Filtering by filename or timestamp

✅ Partitioned writes for large datasets

✅ Compression: option("compression", "snappy")

Would you like me to extend this into a class with logging, config files, and dynamic format detection? Or build a matching transformation module to clean, enrich, and join the ingested data?

module 3
Here we go, Saibabu — Module 3: PySpark DataFrame Transformations. This is where the real power of PySpark kicks in: manipulating, enriching, and reshaping data for analytics, reporting, and machine learning. I’ll walk you through each transformation type with syntax, examples, and practical notes.

🔧 Module 3: DataFrame Transformations
🧱 1. Select Columns
python
df.select("brand", "price").show()
df.select(df.brand.alias("car_brand")).show()
✅ Use alias() to rename columns inline.

➕ 2. Add or Update Columns (withColumn)
python
df.withColumn("discounted_price", df.price * 0.9).show()
df.withColumn("year_plus_one", df.year + 1).show()
✅ Use withColumn to create derived columns.

🧹 3. Drop Columns
python
df.drop("model").show()
✅ Removes one or more columns from the DataFrame.

🔍 4. Filter Rows (filter / where)
python
df.filter(df.price > 20000).show()
df.where((df.year >= 2020) & (df.brand == "Toyota")).show()
✅ Use &, |, ~ for logical conditions.

🧠 5. Conditional Logic (when / otherwise)
python
from pyspark.sql.functions import when

df.withColumn("price_category", 
    when(df.price > 20000, "High")
    .when(df.price > 15000, "Medium")
    .otherwise("Low")
).show()
✅ Use for bucketing, flags, or derived labels.

🧮 6. Expression-Based Columns (expr / selectExpr)
python
from pyspark.sql.functions import expr

df.select(expr("price * 0.9 as discounted")).show()
df.selectExpr("brand", "price * 1.18 as taxed_price").show()
✅ Great for SQL-style transformations.

🧼 7. Distinct and Deduplication
python
df.select("brand").distinct().show()
df.dropDuplicates(["brand", "year"]).show()
✅ Use dropDuplicates() for multi-column deduplication.

📊 8. Sorting and Limiting
python
df.orderBy("price", ascending=False).show()
df.sort("year").show()
df.limit(5).show()
✅ Use limit() for sampling or previewing.

🧬 9. Type Casting
python
df.withColumn("year_str", df.year.cast("string")).printSchema()
✅ Use cast() to change column types.

🧪 10. Column Functions
python
from pyspark.sql.functions import col, lit

df.withColumn("tax", col("price") * 0.18)
df.withColumn("country", lit("India"))
✅ lit() adds a constant column.

🧠 Practice Exercise
python
df.withColumn("price_category", 
    when(df.price > 20000, "High")
    .otherwise("Low")
).filter(df.year >= 2020).select("brand", "price", "price_category").show()
Would you like a quiz for Module 3 next, or move on to Module 4: Joins & Set Operations? I can also bundle all transformations into a reusable enrichment function for your pipeline.

explain in detail about module 3
Absolutely, Saibabu — here’s your comprehensive breakdown of Module 3: PySpark DataFrame Transformations, tailored for real-world analytics, reporting, and pipeline engineering. This module is all about reshaping, enriching, and filtering data — the heart of any data-driven workflow.

🔧 Module 3: PySpark DataFrame Transformations (Detailed)
🧱 1. Selecting Columns
Use select() to extract specific columns or rename them.

python
df.select("brand", "price").show()
df.select(df.brand.alias("car_brand")).show()
✅ Use alias() to rename columns inline ✅ Supports expressions: df.selectExpr("price * 0.9 as discounted")

➕ 2. Adding or Updating Columns (withColumn)
Use withColumn() to create new columns or modify existing ones.

python
df.withColumn("discounted_price", df.price * 0.9).show()
df.withColumn("year_plus_one", df.year + 1).show()
✅ Can use functions, literals, or expressions ✅ Overwrites if column already exists

🧹 3. Dropping Columns
Use drop() to remove one or more columns.

python
df.drop("model").show()
df.drop("brand", "year").show()
✅ Useful for cleaning up intermediate columns

🔍 4. Filtering Rows (filter / where)
Use filter() or where() to select rows based on conditions.

python
df.filter(df.price > 20000).show()
df.where((df.year >= 2020) & (df.brand == "Toyota")).show()
✅ Supports complex conditions with &, |, ~ ✅ Equivalent to SQL WHERE clause

🧠 5. Conditional Logic (when / otherwise)
Use when() to apply if-else logic to columns.

python
from pyspark.sql.functions import when

df.withColumn("price_category", 
    when(df.price > 20000, "High")
    .when(df.price > 15000, "Medium")
    .otherwise("Low")
).show()
✅ Chain multiple when() conditions ✅ Use otherwise() for fallback

🧮 6. Expression-Based Columns (expr / selectExpr)
Use SQL-style expressions for transformations.

python
from pyspark.sql.functions import expr

df.select(expr("price * 0.9 as discounted")).show()
df.selectExpr("brand", "price * 1.18 as taxed_price").show()
✅ Great for quick math, casting, or logic ✅ selectExpr() supports multiple expressions

🧼 7. Distinct and Deduplication
Use distinct() or dropDuplicates() to remove duplicates.

python
df.select("brand").distinct().show()
df.dropDuplicates(["brand", "year"]).show()
✅ distinct() works on entire row ✅ dropDuplicates() works on selected columns

📊 8. Sorting and Limiting
Use orderBy() or sort() to sort rows.

python
df.orderBy("price", ascending=False).show()
df.sort("year").show()
df.limit(5).show()
✅ limit() is useful for sampling or previews ✅ orderBy() supports multiple columns

🧬 9. Type Casting
Use cast() to change column data types.

python
df.withColumn("year_str", df.year.cast("string")).printSchema()
✅ Useful for schema alignment or formatting

🧪 10. Column Functions
Use col() and lit() for column references and constants.

python
from pyspark.sql.functions import col, lit

df.withColumn("tax", col("price") * 0.18)
df.withColumn("country", lit("India"))
✅ lit() adds a constant value ✅ col() is preferred for chaining

🧠 Real-World Use Case
Let’s say you’re building a car pricing report:

python
from pyspark.sql.functions import when, col

df = df.withColumn("price_category", 
    when(col("price") > 20000, "High")
    .otherwise("Low")
).filter(col("year") >= 2020)

df.select("brand", "model", "price", "price_category").orderBy("price", ascending=False).show()
✅ This filters recent cars, adds a price label, and sorts by price.

🧪 Practice Challenge
Create a DataFrame and:

Add a column for GST (18%)

Label cars as “Premium” if price > ₹20,000

Drop the model column

Sort by year descending

Would you like a quiz for Module 3 next, or move on to Module 4: Joins & Set Operations? I can also bundle these transformations into a reusable enrichment function for your pipeline.

module 4
Let’s power through Module 4: Joins & Set Operations in PySpark, Saibabu — this is where you connect datasets, enrich records, and build relational logic across sources. I’ll break down each join type, show how to use them, and cover union, intersect, and subtract operations with examples.

🔗 Module 4: Joins & Set Operations (Detailed)
🧩 1. Join Syntax
python
df1.join(df2, df1.key == df2.key, "inner")
✅ You can join on:

Single column: "key"

Multiple columns: on=["col1", "col2"]

Expressions: df1.col == df2.col

🔄 2. Join Types Explained
Type	Description	Example
inner	Matches rows with keys in both DataFrames	df1.join(df2, "id", "inner")
left	All rows from left + matched from right	df1.join(df2, "id", "left")
right	All rows from right + matched from left	df1.join(df2, "id", "right")
outer	All rows from both, matched or unmatched	df1.join(df2, "id", "outer")
semi	Only rows from left that match right	df1.join(df2, "id", "left_semi")
anti	Only rows from left that don’t match right	df1.join(df2, "id", "left_anti")
cross	Cartesian product (all combinations)	df1.crossJoin(df2)
🧠 Example: Left Join
python
cars = spark.createDataFrame([
    (1, "Toyota"), (2, "Honda"), (3, "Ford")
], ["car_id", "brand"])

info = spark.createDataFrame([
    ("Toyota", "Japan"), ("Honda", "Japan")
], ["brand", "origin"])

cars.join(info, on="brand", how="left").show()
✅ Ford will have null for origin.

🚀 Broadcast Join (Optimization)
python
from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), "id", "inner")
✅ Use when df2 is small — avoids shuffle.

🧮 Set Operations
🔹 1. Union
python
df1.union(df2)
✅ Requires same schema and column order.

🔹 2. UnionAll (Deprecated)
Use union() — unionAll() is legacy.

🔹 3. Intersect
python
df1.intersect(df2)
✅ Returns common rows.

🔹 4. Subtract
python
df1.subtract(df2)
✅ Returns rows in df1 not in df2.

🧠 Real-World Use Case
Join car sales with brand info:

python
sales = spark.read.csv("sales.csv", header=True, inferSchema=True)
brands = spark.read.csv("brands.csv", header=True, inferSchema=True)

enriched = sales.join(brands, on="brand", how="left")
✅ Now you can group by origin, rank by sales, etc.

🧪 Practice Challenge
Create two DataFrames:

df1: car_id, brand

df2: brand, origin

Try:

Inner join

Left join

Anti join

Union

Intersect

Would you like a quiz for Module 4 next, or move on to Module 5: Window Functions? I can also bundle join logic into a reusable enrichment function for your pipeline.

module 5
Let’s dive into Module 5: PySpark Window Functions, Saibabu — this is where you unlock advanced analytics like ranking, running totals, lead/lag comparisons, and partitioned aggregations. Window functions are essential for reporting, time-series analysis, and feature engineering.

🪟 Module 5: PySpark Window Functions (Detailed)
🧠 What Is a Window Function?
A window function performs calculations across a set of rows that are related to the current row, defined by:

Partition: groups of rows (like SQL GROUP BY)

Order: sorting within each partition

Frame: range of rows to operate on (default: current row)

🔧 Step-by-Step Setup
✅ 1. Import Required Modules
python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, ntile, sum, avg
✅ 2. Define a Window Spec
python
windowSpec = Window.partitionBy("brand").orderBy("price")
✅ You can also use rowsBetween or rangeBetween for custom frames.

🔢 Common Window Functions
🔹 1. row_number()
Assigns a unique row number within each partition.

python
df.withColumn("row_num", row_number().over(windowSpec)).show()
🔹 2. rank() and dense_rank()
Ranks rows with ties.

python
df.withColumn("rank", rank().over(windowSpec)).show()
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
✅ rank() skips numbers on ties; dense_rank() does not.

🔹 3. lead() and lag()
Access next or previous row values.

python
df.withColumn("next_price", lead("price", 1).over(windowSpec)).show()
df.withColumn("prev_price", lag("price", 1).over(windowSpec)).show()
✅ Useful for time-series or change detection.

🔹 4. ntile(n)
Divides rows into n buckets.

python
df.withColumn("bucket", ntile(3).over(windowSpec)).show()
✅ Great for quantile-based segmentation.

🔹 5. Aggregations Over Windows
python
df.withColumn("running_total", sum("price").over(windowSpec)).show()
df.withColumn("avg_price", avg("price").over(windowSpec)).show()
✅ Use for cumulative metrics, moving averages, etc.

🧠 Real-World Use Case
Rank cars by price within each brand:

python
windowSpec = Window.partitionBy("brand").orderBy(df.price.desc())
df.withColumn("price_rank", row_number().over(windowSpec)).show()
✅ This helps identify top models per brand.

🧪 Practice Challenge
Create a DataFrame with:

brand, model, price

Try:

Rank models by price within brand

Add previous and next price columns

Compute running total of price per brand

Would you like a quiz for Module 5 next, or move on to Module 6: Advanced Transformations like UDFs, flatMap, map, and SQL expressions? I can also bundle window logic into a reusable ranking function for your pipeline.

explain in detail about module 5
Absolutely, Saibabu — here’s your deep-dive into Module 5: PySpark Window Functions, crafted for production-grade analytics, ranking, and time-series logic. This module unlocks powerful row-wise computations across partitions, enabling cumulative metrics, comparisons, and advanced reporting.

🪟 What Are Window Functions?
Unlike regular aggregations that collapse rows, window functions preserve row-level granularity while computing metrics across a defined “window” of rows.

Each window function operates over:

Partition: groups of rows (like SQL GROUP BY)

Order: sorting within each partition

Frame: range of rows to include (default: unbounded preceding to current row)

🔧 Step-by-Step Setup
✅ 1. Import Required Modules
python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, ntile, sum, avg
✅ 2. Define a Window Spec
python
windowSpec = Window.partitionBy("brand").orderBy("price")
You can also define frames:

python
Window.partitionBy("brand").orderBy("price").rowsBetween(Window.unboundedPreceding, Window.currentRow)
🔢 Common Window Functions
🔹 1. row_number()
Assigns a unique sequential number to each row within a partition.

python
df.withColumn("row_num", row_number().over(windowSpec)).show()
✅ No gaps, always increasing.

🔹 2. rank() and dense_rank()
Ranks rows based on order:

rank(): skips numbers on ties

dense_rank(): no gaps

python
df.withColumn("rank", rank().over(windowSpec)).show()
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
✅ Useful for top-N queries, leaderboard logic.

🔹 3. lead() and lag()
Access values from next or previous rows.

python
df.withColumn("next_price", lead("price", 1).over(windowSpec)).show()
df.withColumn("prev_price", lag("price", 1).over(windowSpec)).show()
✅ Great for change detection, deltas, comparisons.

🔹 4. ntile(n)
Divides rows into n buckets (quantiles).

python
df.withColumn("bucket", ntile(4).over(windowSpec)).show()
✅ Use for quartiles, deciles, segmentation.

🔹 5. Aggregations Over Windows
python
df.withColumn("running_total", sum("price").over(windowSpec)).show()
df.withColumn("avg_price", avg("price").over(windowSpec)).show()
✅ Enables cumulative metrics, moving averages, rolling sums.

🧠 Real-World Use Case
Let’s say you want to rank car models by price within each brand:

python
windowSpec = Window.partitionBy("brand").orderBy(df.price.desc())

df.withColumn("price_rank", row_number().over(windowSpec)) \
  .select("brand", "model", "price", "price_rank") \
  .show()
✅ This helps identify top models per brand.

🧪 Practice Challenge
Create a DataFrame:

python
data = [("Toyota", "Corolla", 18000), ("Toyota", "Camry", 24000),
        ("Honda", "Civic", 20000), ("Honda", "Accord", 26000)]
columns = ["brand", "model", "price"]
df = spark.createDataFrame(data, columns)
Try:

Rank models by price within brand

Add previous and next price columns

Compute running total of price per brand

Assign quartile buckets using ntile(4)

🧩 Bonus: Frame Control
You can define custom frames for rolling windows:

python
Window.orderBy("date").rowsBetween(-2, 0)  # Last 3 rows including current
✅ Enables rolling averages, trailing metrics.

Would you like a quiz for Module 5 next, or move on to Module 6: Advanced Transformations like UDFs, flatMap, map, and SQL expressions? I can also wrap window logic into a reusable ranking function for your pipeline.

rows between explain in detail
Absolutely, Saibabu — let’s break down the rowsBetween clause in PySpark Window functions. This is a powerful tool for defining the exact range of rows over which a window function should operate, especially useful for rolling aggregates, moving averages, and custom frame logic.

🪟 What Is rowsBetween?
rowsBetween(start, end) defines a physical window frame relative to the current row:

start: how many rows before the current row to include

end: how many rows after the current row to include

It works with row-based offsets, not values or timestamps.

🔢 Common Frame Boundaries
Boundary	Meaning
Window.unboundedPreceding	Start from the first row in partition
Window.unboundedFollowing	End at the last row in partition
Window.currentRow	Only the current row
-n	n rows before current row
+n	n rows after current row
🔧 Syntax Example
python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

windowSpec = Window.partitionBy("brand").orderBy("price") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("running_total", sum("price").over(windowSpec)).show()
✅ This computes a cumulative sum of price from the first row up to the current row within each brand.

🧠 Use Cases
🔹 1. Cumulative Sum
python
Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
✅ Running total up to current row

🔹 2. Moving Average (Last 3 Rows)
python
Window.rowsBetween(-2, 0)
✅ Includes current row and 2 rows before

🔹 3. Future Forecast (Next 2 Rows)
python
Window.rowsBetween(0, 2)
✅ Includes current row and 2 rows after

🔹 4. Full Partition
python
Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
✅ Aggregates over entire partition

🧪 Practice Challenge
Create a DataFrame:

python
data = [("Toyota", 18000), ("Toyota", 24000), ("Toyota", 22000),
        ("Honda", 20000), ("Honda", 26000)]
columns = ["brand", "price"]
df = spark.createDataFrame(data, columns)
Try:

Cumulative sum of price per brand

Moving average of last 2 prices

Sum of current + next price

Would you like me to build a reusable rolling metric function using rowsBetween, or move on to Module 6: Advanced Transformations like UDFs, flatMap, and SQL expressions?

module 6
Here’s your deep-dive into Module 6: Advanced Transformations in PySpark, Saibabu — this module unlocks custom logic, functional programming, and SQL-style power. You’ll learn how to use UDFs, flatMap, map, and expression-based transformations to build flexible, scalable pipelines.

🧠 Module 6: Advanced Transformations
🔹 1. UDF (User Defined Function)
Use UDFs to apply custom Python logic to DataFrame columns.

✅ Define and Register
python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def label_price(price):
    return "High" if price > 20000 else "Low"

label_udf = udf(label_price, StringType())
✅ Apply to DataFrame
python
df.withColumn("price_label", label_udf(df.price)).show()
🧠 Notes:
UDFs are slower than built-in functions (no Catalyst optimization)

Prefer when, expr, or transform() when possible

Use pandas_udf for vectorized performance

🔹 2. flatMap() Transformation (RDD only)
Expands each input row into multiple output rows.

python
rdd = sc.parallelize(["Toyota Honda", "Ford"])
rdd.flatMap(lambda x: x.split(" ")).collect()
✅ Useful for tokenization, exploding arrays, or flattening nested data.

🔹 3. map() Transformation (RDD only)
Applies a function to each row.

python
rdd.map(lambda x: x.upper()).collect()
✅ Use for row-wise transformations, formatting, or enrichment.

🔹 4. SQL Expressions (expr, selectExpr)
Use SQL-style logic directly in DataFrames.

python
from pyspark.sql.functions import expr

df.select(expr("price * 0.9 as discounted")).show()
df.selectExpr("brand", "price * 1.18 as taxed_price").show()
✅ Supports math, logic, casting, and column creation.

🔹 5. Explode Arrays or Maps
python
from pyspark.sql.functions import explode

df.withColumn("feature", explode(df.features)).show()
✅ Turns array/map columns into multiple rows.

🔹 6. Create Arrays, Maps, Structs
python
from pyspark.sql.functions import array, struct, lit

df.withColumn("combo", array("brand", "model"))
df.withColumn("meta", struct("year", "price"))
df.withColumn("constant_map", lit({"origin": "Japan"}))
✅ Useful for nesting, exporting, or JSON-style structures.

🔹 7. transform() for Array Columns
python
from pyspark.sql.functions import transform

df.withColumn("features_upper", transform(df.features, lambda x: x.upper()))
✅ Applies logic to each element in an array column.

🧠 Real-World Use Case
Let’s say you want to label cars, explode features, and enrich with a struct:

python
df.withColumn("price_label", when(df.price > 20000, "High").otherwise("Low")) \
  .withColumn("meta", struct("year", "price")) \
  .withColumn("feature", explode(df.features)) \
  .select("brand", "model", "price_label", "meta", "feature").show()
✅ This builds a rich, nested, and labeled dataset.

🧪 Practice Challenge
Create a DataFrame:

python
data = [("Toyota", "Corolla", 18000, ["ABS", "Airbags"]),
        ("Honda", "Civic", 22000, ["Cruise", "Bluetooth"])]
columns = ["brand", "model", "price", "features"]
df = spark.createDataFrame(data, columns)
Try:

Label price as High/Low

Explode features

Create a struct column with year and price

Use selectExpr to compute taxed price

Would you like a quiz for Module 6 next, or move on to Module 7: Performance Tuning with caching, partitioning, skew handling, and DAG analysis? I can also wrap these transformations into a reusable enrichment module for your pipeline.

explain in detail
Absolutely, Saibabu — here’s your in-depth breakdown of Module 6: Advanced Transformations in PySpark, designed for custom logic, functional programming, and expressive data manipulation. This module is essential when built-in functions aren’t enough and you need full control over how data is transformed.

🧠 Why Advanced Transformations Matter
While PySpark offers powerful built-in functions, real-world data often requires:

Custom business logic

Complex row-wise operations

Nested or array-based transformations

SQL-style expressions for dynamic pipelines

This module covers:

UDFs (User Defined Functions)

RDD-level transformations (map, flatMap)

SQL expressions (expr, selectExpr)

Array/map/struct manipulation

🔹 1. UDF (User Defined Function)
✅ What It Is:
A UDF lets you apply custom Python logic to DataFrame columns.

✅ How to Use:
python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def label_price(price):
    return "High" if price > 20000 else "Low"

label_udf = udf(label_price, StringType())
df.withColumn("price_label", label_udf(df.price)).show()
🧠 Notes:
UDFs are not optimized by Spark Catalyst, so they’re slower.

Avoid UDFs when built-in functions (when, expr) can do the job.

For performance, use pandas_udf (vectorized) if needed.

🔹 2. RDD Transformations: map() and flatMap()
✅ map() — One-to-One Transformation
python
rdd = sc.parallelize(["Toyota", "Honda"])
rdd.map(lambda x: x.upper()).collect()
✅ flatMap() — One-to-Many Transformation
python
rdd = sc.parallelize(["Toyota Honda", "Ford"])
rdd.flatMap(lambda x: x.split(" ")).collect()
🧠 Use Cases:
Tokenization

Exploding nested structures

Custom row-wise logic before converting to DataFrame

🔹 3. SQL Expressions: expr() and selectExpr()
✅ expr() — Inline SQL Logic
python
from pyspark.sql.functions import expr
df.select(expr("price * 0.9 as discounted")).show()
✅ selectExpr() — Multiple SQL Expressions
python
df.selectExpr("brand", "price * 1.18 as taxed_price", "cast(year as string)").show()
🧠 Benefits:
Concise syntax

Supports math, casting, logic

Ideal for dynamic pipelines

🔹 4. Explode Arrays and Maps
✅ explode() — Flatten Array/Map Columns
python
from pyspark.sql.functions import explode

df.withColumn("feature", explode(df.features)).show()
🧠 Use Cases:
Feature engineering

JSON flattening

Multi-value fields

🔹 5. Create Arrays, Maps, Structs
✅ array(), map(), struct()
python
from pyspark.sql.functions import array, struct, lit

df.withColumn("combo", array("brand", "model"))
df.withColumn("meta", struct("year", "price"))
🧠 Use Cases:
Nesting for export (e.g., JSON)

Grouping related fields

Building complex schemas

🔹 6. Transform Array Elements
✅ transform() — Apply Logic to Each Array Element
python
from pyspark.sql.functions import transform

df.withColumn("features_upper", transform(df.features, lambda x: x.upper()))
✅ This is a Catalyst-optimized alternative to UDFs for arrays.

🧠 Real-World Pipeline Example
Let’s say you have a car dataset with features and prices:

python
df.withColumn("price_label", when(df.price > 20000, "High").otherwise("Low")) \
  .withColumn("meta", struct("year", "price")) \
  .withColumn("feature", explode(df.features)) \
  .select("brand", "model", "price_label", "meta", "feature").show()
✅ This builds a labeled, nested, and exploded dataset — ready for reporting or ML.

🧪 Practice Challenge
Create a DataFrame:

python
data = [("Toyota", "Corolla", 18000, ["ABS", "Airbags"]),
        ("Honda", "Civic", 22000, ["Cruise", "Bluetooth"])]
columns = ["brand", "model", "price", "features"]
df = spark.createDataFrame(data, columns)
Try:

Label price as High/Low using when

Explode features into rows

Create a struct column with year and price

Use selectExpr to compute taxed price

Would you like a quiz for Module 6 next, or move on to Module 7: Performance Tuning with caching, partitioning, skew handling, and DAG analysis? I can also wrap these transformations into a reusable enrichment module for your pipeline.

module 7
Here’s your deep-dive into Module 7: PySpark Performance Tuning, Saibabu — this module is all about making your pipelines faster, leaner, and more scalable. Whether you're working with massive datasets or optimizing joins and shuffles, these techniques will help you build production-grade workflows.

⚙️ Module 7: Performance Tuning in PySpark
🧠 Why It Matters
PySpark is powerful, but without tuning:

Jobs run slowly

Memory gets wasted

Shuffles explode

Skewed data causes bottlenecks

This module covers:

Caching and persistence

Partitioning strategies

Shuffle optimization

Skew handling

DAG and Spark UI analysis

🔹 1. Caching and Persistence
✅ cache() — Stores DataFrame in memory
python
df.cache()
df.count()  # triggers caching
✅ persist() — Choose storage level
python
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
Level	Description
MEMORY_ONLY	Fastest, fails if memory is full
MEMORY_AND_DISK	Falls back to disk if needed
DISK_ONLY	Avoids memory, slower
✅ Use when DataFrame is reused multiple times.

🔹 2. Partitioning Strategies
✅ repartition() — Increase partitions
python
df.repartition(10)
✅ Triggers full shuffle — use for parallelism.

✅ coalesce() — Reduce partitions
python
df.coalesce(1)
✅ Avoids shuffle — use for writing single file.

✅ Partition by column
python
df.write.partitionBy("year").parquet("output/")
✅ Improves read performance via pruning.

🔹 3. Shuffle Optimization
Shuffles happen during:

Joins

GroupBy

Repartition

✅ Tips:
Use broadcast() for small tables

Avoid wide transformations unless necessary

Use salting for skewed keys

🔹 4. Skew Handling
✅ Detect Skew:
python
df.groupBy("key").count().orderBy("count", ascending=False).show()
✅ Fix Skew:
Salting: Add random prefix to skewed keys

Broadcast Join: Avoid shuffle

Custom partitioning: Use partitionBy() with hash

🔹 5. Job and DAG Analysis
✅ Spark UI:
Stages: Logical units of execution

Tasks: Parallel units within stages

DAG: Directed Acyclic Graph of transformations

✅ Use Spark UI to:

Spot long-running stages

Identify shuffle-heavy tasks

Analyze memory usage

🧠 Real-World Pipeline Tips
Cache intermediate DataFrames used in multiple joins

Repartition before wide aggregations

Coalesce before writing single output file

Use broadcast joins for small lookup tables

Monitor Spark UI for skew and shuffle

🧪 Practice Challenge
Create a DataFrame:

python
data = [("Toyota", 18000), ("Toyota", 24000), ("Honda", 20000), ("Honda", 26000)]
df = spark.createDataFrame(data, ["brand", "price"])
Try:

Cache the DataFrame

Repartition to 4 partitions

Coalesce to 1 partition before writing

Use Spark UI to inspect DAG

Would you like a quiz for Module 7 next, or move on to Module 8: Data Engineering Workflows with Delta Lake, cloud storage, and null handling? I can also build a reusable performance wrapper for your pipeline.

eplain in detail with claer explanation
Absolutely, Saibabu — here’s a clear, detailed, and production-focused explanation of Module 7: PySpark Performance Tuning, tailored for your scalable analytics workflows. This module helps you optimize memory, CPU, and execution time across transformations, joins, and writes.

⚙️ Module 7: PySpark Performance Tuning (Detailed)
🔧 Why Tune Performance?
PySpark runs distributed jobs across executors. Without tuning:

Jobs may run slowly or crash

Memory may overflow

Shuffles may explode

Skewed data may cause bottlenecks

Performance tuning ensures:

Efficient resource usage

Faster job completion

Scalable pipelines

🔹 1. Caching and Persistence
✅ cache(): Store DataFrame in memory (default: MEMORY_AND_DISK)
python
df.cache()
df.count()  # triggers caching
✅ persist(): Choose storage level
python
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
Level	Description
MEMORY_ONLY	Fastest, fails if memory is full
MEMORY_AND_DISK	Falls back to disk if needed
DISK_ONLY	Avoids memory, slower
🧠 Use when:

DataFrame is reused multiple times

You want to avoid recomputation

🔹 2. Partitioning Strategies
✅ repartition(n): Increase number of partitions
python
df.repartition(10)
✅ Triggers a full shuffle ✅ Use before wide operations (e.g., joins, groupBy)

✅ coalesce(n): Reduce number of partitions
python
df.coalesce(1)
✅ Avoids shuffle ✅ Use before writing a single output file

✅ Partitioned Write
python
df.write.partitionBy("year").parquet("output/")
✅ Improves read performance via partition pruning ✅ Ideal for time-series or category-based data

🔹 3. Shuffle Optimization
Shuffles happen during:

groupBy

join

distinct

repartition

✅ Tips:
Use broadcast() for small lookup tables

python
from pyspark.sql.functions import broadcast
df.join(broadcast(dim_table), "key")
Avoid unnecessary wide transformations

Use salting for skewed keys (see below)

🔹 4. Skew Handling
✅ Detect Skew:
python
df.groupBy("key").count().orderBy("count", ascending=False).show()
✅ Fix Skew:
🔸 Salting
python
from pyspark.sql.functions import concat, lit, rand

df_skewed = df.withColumn("salted_key", concat(df.key, lit("_"), (rand() * 10).cast("int")))
✅ Distributes skewed keys across partitions

🔸 Broadcast Join
Use when one table is small enough to fit in memory

🔸 Custom Partitioning
python
df.repartition("key")
✅ Ensures even distribution across executors

🔹 5. Job and DAG Analysis
✅ Spark UI: http://localhost:4040 (or cluster URL)
Key tabs:

Jobs: Overall job status

Stages: Logical units of execution

Tasks: Parallel units within stages

Storage: Cached/persisted data

SQL: Query plans and physical execution

✅ Use Spark UI to:

Spot long-running stages

Identify shuffle-heavy tasks

Analyze memory usage

Understand DAG lineage

🧠 Real-World Pipeline Tips
Cache intermediate DataFrames used in multiple joins

Repartition before wide aggregations

Coalesce before writing single output file

Use broadcast joins for small lookup tables

Monitor Spark UI for skew and shuffle

🧪 Practice Challenge
Create a DataFrame:

python
data = [("Toyota", 18000), ("Toyota", 24000), ("Honda", 20000), ("Honda", 26000)]
df = spark.createDataFrame(data, ["brand", "price"])
Try:

Cache the DataFrame

Repartition to 4 partitions

Coalesce to 1 partition before writing

Use Spark UI to inspect DAG and stages

Would you like a quiz for Module 7 next, or move on to Module 8: Data Engineering Workflows with Delta Lake, cloud storage, and null handling? I can also build a reusable performance wrapper for your pipeline with caching, partitioning, and broadcast logic.

explain skew handling
Great question, Saibabu — skew handling is one of the most important performance tuning techniques in PySpark, especially when working with large datasets and joins. Let’s break it down clearly and practically.

🧠 What Is Data Skew?
Data skew happens when certain keys (values in a column) appear much more frequently than others. This causes:

Uneven partition sizes

Executor imbalance

Long-running tasks

Shuffle bottlenecks

🔍 Example:
python
df.groupBy("customer_id").count().orderBy("count", ascending=False).show()
If one customer_id appears 1 million times and others only 100 times, that’s skew.

⚠️ Why Skew Is Bad
Spark distributes data across partitions.

If one partition has too much data (due to skew), it takes longer to process.

Other partitions finish quickly and sit idle.

This leads to poor parallelism and slow jobs.

🛠️ How to Handle Skew in PySpark
🔹 1. Detect Skew
python
df.groupBy("key").count().orderBy("count", ascending=False).show()
✅ Look for keys with disproportionately high counts.

🔹 2. Broadcast Join (Best for Small Tables)
If one side of the join is small (e.g., a lookup table), broadcast it:

python
from pyspark.sql.functions import broadcast

df_large.join(broadcast(df_small), "key")
✅ Avoids shuffle altogether ✅ Ideal for dimension tables

🔹 3. Salting Technique
Add randomness to skewed keys to spread them across partitions.

✅ Step-by-step:
python
from pyspark.sql.functions import concat, lit, rand

# Add salt to skewed keys
df_skewed = df.withColumn("salted_key", concat(df.key, lit("_"), (rand() * 10).cast("int")))

# Add matching salt to the other table
df_lookup = df_lookup.withColumn("salted_key", concat(df_lookup.key, lit("_"), lit(5)))  # example salt

# Join on salted_key
df_skewed.join(df_lookup, "salted_key")
✅ Distributes skewed keys across multiple partitions ✅ Requires post-processing to remove salt if needed

🔹 4. Custom Partitioning
Use repartition() or partitionBy() to control distribution:

python
df.repartition("key")  # hash-based partitioning
✅ Helps balance load across executors ✅ Use with caution — may trigger shuffle

🔹 5. Skew Join Optimization in Spark Config
If using Databricks or Spark 3+, enable automatic skew join handling:

python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
✅ Spark will detect skew and split large partitions automatically

🧠 Summary
Technique	Best For	Notes
Broadcast Join	Small lookup tables	Avoids shuffle completely
Salting	High-frequency keys	Adds randomness to keys
Repartitioning	Moderate skew	Redistributes data
Adaptive Skew Join	Spark 3+ / Databricks	Automatic optimization
🧪 Practice Challenge
Create a skewed dataset:

python
data = [("A", 1)] * 1000 + [("B", 1)] * 10 + [("C", 1)] * 10
df = spark.createDataFrame(data, ["key", "value"])
Try:

Detect skew with groupBy().count()

Apply salting

Join with a lookup table using salted keys

Repartition and observe task distribution in Spark UIAbsolutely, Saibabu — here’s your **comprehensive breakdown of Module 9 and Module 10**, designed for real-world deployment and streaming pipelines in PySpark. These modules elevate your workflows from development to production-grade orchestration and real-time data processing.

---

# 🚀 Module 9: Advanced Orchestration & Deployment

This module focuses on **running, scheduling, and managing PySpark jobs** in production environments like clusters, cloud platforms, and Databricks.

---

## 🔧 1. Spark Submit: Cluster Deployment

Use `spark-submit` to run PySpark scripts on clusters (YARN, Kubernetes, Mesos, or standalone).

### ✅ Example:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 4G \
  --executor-cores 2 \
  my_job.py
```

### 🔹 Key Flags:
- `--master`: cluster manager (e.g., `yarn`, `local`, `k8s`)
- `--deploy-mode`: `client` or `cluster`
- `--conf spark.executor.memory`: fine-tune memory
- `--files`, `--jars`: include dependencies

---

## 📦 2. Modular Job Design

Structure your PySpark jobs for maintainability:
- Use `argparse` for dynamic configs
- Externalize paths and schema to YAML/JSON
- Separate ingestion, transformation, and write logic
- Add logging and error handling

✅ Makes jobs reusable, testable, and easier to orchestrate

---

## 🔄 3. Databricks Jobs & Workflows

Databricks lets you schedule notebooks or scripts as jobs.

### ✅ Features:
- Task chaining with dependencies
- Retry policies and alerts
- Parameterized runs
- REST API triggers

✅ Ideal for cloud-native orchestration

---

## 🔁 4. Delta Live Tables (DLT)

DLT is a declarative framework for building reliable pipelines.

### ✅ Benefits:
- Auto schema evolution
- Built-in data quality checks
- Incremental processing
- Versioned lineage

✅ Great for streaming + batch hybrid pipelines

---

## ⏱️ 5. Incremental Processing

Design jobs to process only new or changed data:
- Use ingestion timestamps
- Partition by date
- Use Delta Lake `MERGE` for upserts
- Track file lineage with `input_file_name()`

---

## 📊 6. Monitoring & Alerting

- Use Spark UI or Databricks Job UI
- Log job status to audit tables
- Send alerts via email, Slack, or webhook
- Track metrics like duration, shuffle size, skew

---

# 🔄 Module 10: Streaming & Real-Time Processing

This module teaches you how to build **Structured Streaming pipelines** for logs, events, IoT, and real-time analytics.

---

## 📥 1. Reading Streaming Data

### ✅ From File Directory:
```python
df = spark.readStream \
    .option("header", True) \
    .schema(schema) \
    .csv("path/to/input/")
```

✅ Spark watches the folder and ingests new files

---

### ✅ From Kafka:
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_name") \
    .load()
```

✅ Kafka is ideal for high-throughput, low-latency pipelines

---

## 🔧 2. Transforming Streaming Data

Use standard DataFrame operations:
```python
df.selectExpr("CAST(value AS STRING)").filter("value LIKE '%error%'")
```

✅ Supports `select`, `filter`, `groupBy`, `withColumn`, `window`, etc.

---

## 💾 3. Writing Streaming Output

### ✅ To Console:
```python
df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
```

### ✅ To Parquet:
```python
df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/") \
    .option("checkpointLocation", "checkpoints/") \
    .start()
```

✅ Checkpointing is **mandatory** for fault tolerance

---

## ⏱️ 4. Output Modes

| Mode      | Description                              |
|-----------|------------------------------------------|
| `append`  | Only new rows are written                |
| `complete`| Entire result table is written           |
| `update`  | Only changed rows are written            |

✅ Choose based on aggregation type and sink capabilities

---

## 🧠 5. Windowed Aggregations

```python
from pyspark.sql.functions import window

df.groupBy(window("timestamp", "10 minutes")).count()
```

✅ Enables time-based grouping for metrics, alerts, etc.

---

## 🔐 6. Fault Tolerance & Checkpointing

Checkpointing stores:
- Progress (offsets)
- State (for aggregations)
- Metadata (schema, sink)

✅ Required for recovery after failure  
✅ Use persistent storage (e.g., S3, ADLS)

---

## 📊 7. Monitoring & Scaling

- Use Spark UI or Databricks Job UI
- Monitor micro-batch duration, throughput, latency
- Tune `trigger` intervals and memory settings
- Use autoscaling on cloud clusters

---

## 🧪 Practice Challenge

Build a streaming pipeline that:
- Reads JSON logs from a folder
- Filters for errors
- Groups by 5-minute window
- Writes to Delta Lake with checkpointing

---
