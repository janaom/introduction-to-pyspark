# ✨ Introduction to PySpark
This repository serves as a comprehensive guide to PySpark, featuring theory and exercises sourced from [DataCamp](https://app.datacamp.com/learn/courses/introduction-to-pyspark). It is designed for beginners looking to understand the fundamentals of PySpark and its applications in big data processing. Here, you'll find detailed explanations, practical examples, and hands-on exercises to reinforce your learning. Whether you're just starting out or looking to enhance your skills, this repo provides valuable resources to help you master PySpark.

------

# Getting to know PySpark

## What is Spark, anyway?

Spark is a platform for cluster computing. Spark lets you spread data and computations over clusters with multiple nodes  (think of each node as a separate computer). Splitting up your data  makes it easier to work with very large datasets because each node only  works with a small amount of data.
As each node works on its own subset of the total data, it also  carries out a part of the total calculations required, so that both data  processing and computation are performed in parallel over the nodes in the cluster. It is a fact that parallel computation can make certain types of programming tasks much faster.
However, with greater computing power comes greater complexity.
Deciding whether or not Spark is the best solution for your problem takes some experience, but you can consider questions like:
- Is my data too big to work with on a single machine?
- Can my calculations be easily parallelized?

## Using Spark in Python

The first step in using Spark is connecting to a cluster.
In practice, the cluster will be hosted on a remote machine that's  connected to all other nodes. There will be one computer, called the master  that manages splitting up the data and the computations. The master is  connected to the rest of the computers in the cluster, which are called worker. The master sends the workers data and calculations to run, and they send their results back to the master.
When you're just getting started with Spark it's simpler to just run a  cluster locally. Thus, for this course, instead of connecting to  another computer, all computations will be run on DataCamp's servers in a  simulated cluster.
Creating the connection is as simple as creating an instance of the `SparkContext`  class. The class constructor takes a few optional arguments that allow  you to specify the attributes of the cluster you're connecting to.
An object holding all these attributes can be created with the `SparkConf()` constructor. Take a look at the [documentation](https://spark.apache.org/docs/latest/) for all the details!
For the rest of this course you'll have a `SparkContext` called `sc` already available in your workspace.

## Examining The SparkContext

In this exercise you'll get familiar with the `SparkContext`.
You'll probably notice that code takes longer to run than you might  expect. This is because Spark is some serious software. It takes more  time to start up than you might be used to. You may also find that  running simpler computations might take longer than expected. That's  because all the optimizations that Spark has under its hood are designed  for complicated operations with big data sets. That means that for  simple or small problems Spark may actually perform worse than some  other solutions!

### Instructions

- Get to know the `SparkContext`.
- Call `print()` on `sc` to verify there's a `SparkContext` in your environment.
- `print()` `sc.version` to see what version of Spark is running on your cluster.

```python
# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)
```

## Using DataFrames

Spark's  core data structure is the Resilient Distributed Dataset (RDD). This is a  low level object that lets Spark work its magic by splitting data  across multiple nodes in the cluster. However, RDDs are hard to work  with directly, so in this course you'll be using the Spark DataFrame  abstraction built on top of RDDs.
The Spark DataFrame was designed to behave a lot like a SQL table (a  table with variables in the columns and observations in the rows). Not  only are they easier to understand, DataFrames are also more optimized  for complicated operations than RDDs.
When you start modifying and combining columns and rows of data,  there are many ways to arrive at the same result, but some often take  much longer than others. When using RDDs, it's up to the data scientist  to figure out the right way to optimize the query, but the DataFrame  implementation has much of this optimization built in!
To start working with Spark DataFrames, you first have to create a `SparkSession` object from your `SparkContext`. You can think of the `SparkContext` as your connection to the cluster and the `SparkSession` as your interface with that connection.
Remember, for the rest of this course you'll have a `SparkSession` called `spark` available in your workspace!

## Creating a SparkSession

We've already created a `SparkSession` for you called `spark`, but what if you're not sure there already is one? Creating multiple `SparkSessions` and `SparkContexts` can cause issues, so it's best practice to use the `SparkSession.builder.getOrCreate()` method. This returns an existing `SparkSession` if there's already one in the environment, or creates a new one if necessary!

### Instructions

- Import `SparkSession` from `pyspark.sql`.
- Make a new `SparkSession` called my_spark using `SparkSession.builder.getOrCreate()`.
- Print `my_spark` to the console to verify it's a `SparkSession`.

```python
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

#Result
<pyspark.sql.session.SparkSession object at 0x7ffaa959a490>
```

## Viewing tables

Once you've created a `SparkSession`, you can start poking around to see what data is in your cluster!
Your `SparkSession` has an attribute called `catalog` which lists all the data inside the cluster. This attribute has a few methods for extracting different pieces of information.
One of the most useful is the `.listTables()` method, which returns the names of all the tables in your cluster as a list.

### Instructions

- See what tables are in your cluster by calling `spark.catalog.listTables()` and printing the result!

```python
# Print the tables in the catalog
print(spark.catalog.listTables())

#Result
[Table(name='flights', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]
```

## Are you query-ious?

One  of the advantages of the DataFrame interface is that you can run SQL  queries on the tables in your Spark cluster. If you don't have any  experience with SQL, don't worry, we'll provide you with queries! (To  learn more SQL, start with our [Introduction to SQL course](https://www.datacamp.com/courses/intro-to-sql-for-data-science).)
As you saw in the last exercise, one of the tables in your cluster is the `flights`  table. This table contains a row for every flight that left Portland  International Airport (PDX) or Seattle-Tacoma International Airport  (SEA) in 2014 and 2015.
Running a query on this table is as easy as using the `.sql()` method on your `SparkSession`. This method takes a string containing the query and returns a DataFrame with the results!
If you look closely, you'll notice that the table `flights`  is only mentioned in the query, not as an argument to any of the  methods. This is because there isn't a local object in your environment  that holds that data, so it wouldn't make sense to pass the table as an  argument.
Remember, we've already created a `SparkSession` called `spark` in your workspace. (It's no longer called `my_spark` because we created it for you!)

### Instructions

- Use the `.sql() method` to get the first 10 rows of the `flights` table and save the result to `flights10`. The variable `query` contains the appropriate SQL query.
- Use the DataFrame method `.show()` to print `flights10`.

```python
# Don't change this query
query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()

#Results
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
|year|month|day|dep_time|dep_delay|arr_time|arr_delay|carrier|tailnum|flight|origin|dest|air_time|distance|hour|minute|
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
|2014|   12|  8|     658|       -7|     935|       -5|     VX| N846VA|  1780|   SEA| LAX|     132|     954|   6|    58|
|2014|    1| 22|    1040|        5|    1505|        5|     AS| N559AS|   851|   SEA| HNL|     360|    2677|  10|    40|
|2014|    3|  9|    1443|       -2|    1652|        2|     VX| N847VA|   755|   SEA| SFO|     111|     679|  14|    43|
|2014|    4|  9|    1705|       45|    1839|       34|     WN| N360SW|   344|   PDX| SJC|      83|     569|  17|     5|
|2014|    3|  9|     754|       -1|    1015|        1|     AS| N612AS|   522|   SEA| BUR|     127|     937|   7|    54|
|2014|    1| 15|    1037|        7|    1352|        2|     WN| N646SW|    48|   PDX| DEN|     121|     991|  10|    37|
|2014|    7|  2|     847|       42|    1041|       51|     WN| N422WN|  1520|   PDX| OAK|      90|     543|   8|    47|
|2014|    5| 12|    1655|       -5|    1842|      -18|     VX| N361VA|   755|   SEA| SFO|      98|     679|  16|    55|
|2014|    4| 19|    1236|       -4|    1508|       -7|     AS| N309AS|   490|   SEA| SAN|     135|    1050|  12|    36|
|2014|   11| 19|    1812|       -3|    2352|       -4|     AS| N564AS|    26|   SEA| ORD|     198|    1721|  18|    12|
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
```

## Pandafy a Spark DataFrame

Suppose you've run a query on your huge dataset and aggregated it down to something a little more manageable.
Sometimes it makes sense to then take that table and work with it locally using a tool like `pandas`. Spark DataFrames make that easy with the `.toPandas()` method. Calling this method on a Spark DataFrame returns the corresponding `pandas` DataFrame. It's as simple as that!
This time the query counts the number of flights to each airport from SEA and PDX.
Remember, there's already a `SparkSession` called `spark` in your workspace!

### Instructions

- Run the query using the `.sql()` method. Save the result in `flight_counts`.
- Use the `.toPandas()` method on `flight_counts` to create a `pandas` DataFrame called `pd_counts`.
- Print the `.head()` of `pd_counts` to the console.

```python
# Don't change this query
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())

#Results
  origin dest    N
0    SEA  RNO    8
1    SEA  DTW   98
2    SEA  CLE    2
3    SEA  LAX  450
4    PDX  SEA  144
```

## Put some Spark in your data

In the last exercise, you saw how to move data from Spark to `pandas`. However, maybe you want to go the other direction, and put a pandas DataFrame into a Spark cluster! The `SparkSession` class has a method for this as well.
The `.createDataFrame()` method takes a `pandas` DataFrame and returns a Spark DataFrame.
The output of this method is stored locally, not in the `SparkSession` catalog. This means that you can use all the Spark DataFrame methods on it, but you can't access the data in other contexts.
For example, a SQL query (using the `.sql()` method) that references your DataFrame will throw an error. To access the data in this way, you have to save it as a temporary table.
You can do this using the `.createTempView()` Spark  DataFrame method, which takes  as its only argument the name of the  temporary table you'd like to register. This method registers the  DataFrame as a table in the catalog, but as this table is temporary, it  can only be accessed from the specific `SparkSession` used to create the Spark DataFrame.
There is also the method `.createOrReplaceTempView()`.  This safely creates a new temporary table if nothing was there before,  or updates an existing table if one was already defined. You'll use this  method to avoid running into problems with duplicate tables.
Check out the diagram to see all the different ways your Spark data structures interact with each other.

![spark_figure](https://github.com/user-attachments/assets/325ea62d-cf52-4492-a130-6e3a5adac1d9)

There's already a `SparkSession` called `spark` in your workspace, `numpy` has been imported as `np`, and `pandas` as `pd`.

### Instructions

- The code to create a `pandas` DataFrame of random numbers has already been provided and saved under `pd_temp`.
- Create a Spark DataFrame called `spark_temp` by calling the Spark method `.createDataFrame()` with `pd_temp` as the argument.
- Examine the list of tables in your Spark cluster and verify that the new DataFrame is not present. Remember you can use `spark.catalog.listTables()` to do so.
- Register the `spark_temp` DataFrame you just created as a temporary table using the `.createOrReplaceTempView()` method. THe temporary table should be named "temp". Remember that the table name is set including it as the only argument to your method!
- Examine the list of tables again.

```python
# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())

#Results
[Table(name='temp', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]
```

## Dropping the middle man

Now you know how to put data into Spark via `pandas`, but you're probably wondering why deal with `pandas` at all? Wouldn't it be easier to just read a text file straight into Spark? Of course it would!
Luckily, your `SparkSession` has a `.read`  attribute which has several methods for reading different data sources  into Spark DataFrames. Using these you can create a DataFrame from a  `.csv` file just like with regular `pandas` DataFrames!
The variable `file_path` is a string with the path to the file `airports.csv`. This file contains information about different airports all over the world.
A `SparkSession` named `spark` is available in your workspace.

### Instructions

- Use the `.read.csv()` method to create a Spark DataFrame called `airports`
  - The first argument is `file_path`
  - Pass the argument `header=True` so that Spark knows to take the column names from the first line of the file.
- Print out this DataFrame by calling `.show()`

```python
# Don't change this file path
file_path = "/usr/local/share/datasets/airports.csv"

# Read in the airports data
airports = spark.read.csv(file_path, header=True)

# Show the data
airports.show()

#Results
# Show the data
airports.show()
+---+--------------------+----------------+-----------------+----+---+---+
|faa|                name|             lat|              lon| alt| tz|dst|
+---+--------------------+----------------+-----------------+----+---+---+
|04G|   Lansdowne Airport|      41.1304722|      -80.6195833|1044| -5|  A|
|06A|Moton Field Munic...|      32.4605722|      -85.6800278| 264| -5|  A|
|06C| Schaumburg Regional|      41.9893408|      -88.1012428| 801| -6|  A|
|06N|     Randall Airport|       41.431912|      -74.3915611| 523| -5|  A|
|09J|Jekyll Island Air...|      31.0744722|      -81.4277778|  11| -4|  A|
|0A9|Elizabethton Muni...|      36.3712222|      -82.1734167|1593| -4|  A|
|0G6|Williams County A...|      41.4673056|      -84.5067778| 730| -5|  A|
|0G7|Finger Lakes Regi...|      42.8835647|      -76.7812318| 492| -5|  A|
|0P2|Shoestring Aviati...|      39.7948244|      -76.6471914|1000| -5|  U|
|0S9|Jefferson County ...|      48.0538086|     -122.8106436| 108| -8|  A|
|0W3|Harford County Ai...|      39.5668378|      -76.2024028| 409| -5|  A|
|10C|  Galt Field Airport|      42.4028889|      -88.3751111| 875| -6|  U|
|17G|Port Bucyrus-Craw...|      40.7815556|      -82.9748056|1003| -5|  A|
|19A|Jackson County Ai...|      34.1758638|      -83.5615972| 951| -4|  U|
|1A3|Martin Campbell F...|      35.0158056|      -84.3468333|1789| -4|  A|
|1B9| Mansfield Municipal|      42.0001331|      -71.1967714| 122| -5|  A|
|1C9|Frazier Lake Airpark|54.0133333333333|-124.768333333333| 152| -8|  A|
|1CS|Clow Internationa...|      41.6959744|      -88.1292306| 670| -6|  U|
|1G3|  Kent State Airport|      41.1513889|      -81.4151111|1134| -4|  A|
|1OH|     Fortman Airport|      40.5553253|      -84.3866186| 885| -5|  U|
+---+--------------------+----------------+-----------------+----+---+---+
only showing top 20 rows
```
