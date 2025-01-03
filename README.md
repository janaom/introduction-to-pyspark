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

# Manipulating data

## Creating columns

In this chapter, you'll learn how to use the methods defined by Spark's `DataFrame` class to perform common data operations.

Let's look at performing column-wise operations. In Spark you can do this using the `.withColumn()` method, which takes two arguments. First, a string with the name of your new column, and second the new column itself.

The new column must be an object of class `Column`. Creating one of these is as easy as extracting a column from your DataFrame using `df.colName`.

Updating a Spark DataFrame is somewhat different than working in `pandas` because the Spark DataFrame is immutable. This means that it can't be changed, and so columns can't be updated in place.

Thus, all these methods return a new DataFrame. To overwrite the original DataFrame you must reassign the returned DataFrame using the method like so:

```
df = df.withColumn("newCol", df.oldCol + 1)
```

The above code creates a DataFrame with the same columns as `df` plus a new column, `newCol`, where every entry is equal to the corresponding entry from `oldCol`, plus one.

To overwrite an existing column, just pass the name of the column as the first argument!

### Instructions


- Use the `spark.table()` method with the argument "flights" to create a DataFrame containing the values of the `flights` table in the `.catalog.` Save it as `flights`.
- Show the head of `flights` using `flights.show()`. Check the output: the column `air_time` contains the duration of the flight in minutes.
- Update `flights` to include a new column called `duration_hrs`, that contains the duration of each flight in hours (you'll need to divide `air_time` by the number of minutes in an hour).

```python
# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time/60)

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
|2014|   11|  8|    1653|       -2|    1924|       -1|     AS| N323AS|   448|   SEA| LAX|     130|     954|  16|    53|
|2014|    8|  3|    1120|        0|    1415|        2|     AS| N305AS|   656|   SEA| PHX|     154|    1107|  11|    20|
|2014|   10| 30|     811|       21|    1038|       29|     AS| N433AS|   608|   SEA| LAS|     127|     867|   8|    11|
|2014|   11| 12|    2346|       -4|     217|      -28|     AS| N765AS|   121|   SEA| ANC|     183|    1448|  23|    46|
|2014|   10| 31|    1314|       89|    1544|      111|     AS| N713AS|   306|   SEA| SFO|     129|     679|  13|    14|
|2014|    1| 29|    2009|        3|    2159|        9|     UA| N27205|  1458|   PDX| SFO|      90|     550|  20|     9|
|2014|   12| 17|    2015|       50|    2150|       41|     AS| N626AS|   368|   SEA| SMF|      76|     605|  20|    15|
|2014|    8| 11|    1017|       -3|    1613|       -7|     WN| N8634A|   827|   SEA| MDW|     216|    1733|  10|    17|
|2014|    1| 13|    2156|       -9|     607|      -15|     AS| N597AS|    24|   SEA| BOS|     290|    2496|  21|    56|
|2014|    6|  5|    1733|      -12|    1945|      -10|     OO| N215AG|  3488|   PDX| BUR|     111|     817|  17|    33|
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
only showing top 20 rows
```

## Filtering Data

Let's take a look at the `.filter()` method. As you might suspect, this is the Spark counterpart of SQL's `WHERE` clause. The `.filter()` method takes either an expression that would follow the `WHERE` clause of a SQL expression as a string, or a Spark Column of boolean (True/False) values.

For example, the following two expressions will produce the same output:

```python
flights.filter("air_time > 120").show()
flights.filter(flights.air_time > 120).show()
```
Notice that in the first case, we pass a string to `.filter()`. In SQL, we would write this filtering task as `SELECT * FROM flights WHERE air_time > 120`. Spark's `.filter()` can accept any expression that could go in the `WHERE` clause of a SQL query (in this case, "air_time > 120"), as long as it is passed as a string. Notice that in this case, we do not reference the name of the table in the string -- as we wouldn't in the SQL request.

In the second case, we actually pass a column of boolean values to `.filter()`. Remember that `flights.air_time > 120` returns a column of boolean values that has `True` in place of those records in `flights.air_time` that are over 120, and `False` otherwise.

### Instructions


- Use the `.filter()` method to find all the flights that flew over 1000 miles two ways:
    - First, pass a SQL string to `.filter()` that checks whether the distance is greater than 1000. Save this as `long_flights1`.
    - Then pass a column of boolean values to `.filter()` that checks the same thing. Save this as `long_flights2`.
- Use `.show()` to print heads of both DataFrames and make sure they're actually equal!

```python
# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

# Print the data to check they're equal
long_flights1.show()
long_flights2.show()

#Results

+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
|year|month|day|dep_time|dep_delay|arr_time|arr_delay|carrier|tailnum|flight|origin|dest|air_time|distance|hour|minute|
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
|2014|    1| 22|    1040|        5|    1505|        5|     AS| N559AS|   851|   SEA| HNL|     360|    2677|  10|    40|
|2014|    4| 19|    1236|       -4|    1508|       -7|     AS| N309AS|   490|   SEA| SAN|     135|    1050|  12|    36|
|2014|   11| 19|    1812|       -3|    2352|       -4|     AS| N564AS|    26|   SEA| ORD|     198|    1721|  18|    12|
|2014|    8|  3|    1120|        0|    1415|        2|     AS| N305AS|   656|   SEA| PHX|     154|    1107|  11|    20|
|2014|   11| 12|    2346|       -4|     217|      -28|     AS| N765AS|   121|   SEA| ANC|     183|    1448|  23|    46|
|2014|    8| 11|    1017|       -3|    1613|       -7|     WN| N8634A|   827|   SEA| MDW|     216|    1733|  10|    17|
|2014|    1| 13|    2156|       -9|     607|      -15|     AS| N597AS|    24|   SEA| BOS|     290|    2496|  21|    56|
|2014|    9| 26|     610|       -5|    1523|       65|     US| N127UW|   616|   SEA| PHL|     293|    2378|   6|    10|
|2014|   12|  4|     954|       -6|    1348|      -17|     HA| N395HA|    29|   SEA| OGG|     333|    2640|   9|    54|
|2014|    6|  4|    1115|        0|    1346|       -3|     AS| N461AS|   488|   SEA| SAN|     133|    1050|  11|    15|
|2014|    6| 26|    2054|       -1|    2318|       -6|     B6| N590JB|   907|   SEA| ANC|     179|    1448|  20|    54|
|2014|    6|  7|    1823|       -7|    2112|      -28|     AS| N512AS|   815|   SEA| LIH|     335|    2701|  18|    23|
|2014|    4| 30|     801|        1|    1757|       90|     AS| N407AS|    18|   SEA| MCO|     342|    2554|   8|     1|
|2014|   11| 29|     905|      155|    1655|      170|     DL| N824DN|  1598|   SEA| ATL|     229|    2182|   9|     5|
|2014|    6|  2|    2222|        7|      55|       15|     AS| N402AS|    99|   SEA| ANC|     190|    1448|  22|    22|
|2014|   11| 15|    1034|       -6|    1414|      -26|     AS| N589AS|   794|   SEA| ABQ|     139|    1180|  10|    34|
|2014|   10| 20|    1328|       -1|    1949|        4|     UA| N68805|  1212|   SEA| IAH|     228|    1874|  13|    28|
|2014|   12| 16|    1500|        0|    1906|       19|     US| N662AW|   500|   SEA| PHX|     151|    1107|  15|     0|
|2014|   11| 19|    1319|       -6|    1821|      -14|     DL| N309US|  2164|   PDX| MSP|     169|    1426|  13|    19|
|2014|    5| 21|     515|        0|     757|        0|     US| N172US|   593|   SEA| PHX|     143|    1107|   5|    15|
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
only showing top 20 rows

+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
|year|month|day|dep_time|dep_delay|arr_time|arr_delay|carrier|tailnum|flight|origin|dest|air_time|distance|hour|minute|
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
|2014|    1| 22|    1040|        5|    1505|        5|     AS| N559AS|   851|   SEA| HNL|     360|    2677|  10|    40|
|2014|    4| 19|    1236|       -4|    1508|       -7|     AS| N309AS|   490|   SEA| SAN|     135|    1050|  12|    36|
|2014|   11| 19|    1812|       -3|    2352|       -4|     AS| N564AS|    26|   SEA| ORD|     198|    1721|  18|    12|
|2014|    8|  3|    1120|        0|    1415|        2|     AS| N305AS|   656|   SEA| PHX|     154|    1107|  11|    20|
|2014|   11| 12|    2346|       -4|     217|      -28|     AS| N765AS|   121|   SEA| ANC|     183|    1448|  23|    46|
|2014|    8| 11|    1017|       -3|    1613|       -7|     WN| N8634A|   827|   SEA| MDW|     216|    1733|  10|    17|
|2014|    1| 13|    2156|       -9|     607|      -15|     AS| N597AS|    24|   SEA| BOS|     290|    2496|  21|    56|
|2014|    9| 26|     610|       -5|    1523|       65|     US| N127UW|   616|   SEA| PHL|     293|    2378|   6|    10|
|2014|   12|  4|     954|       -6|    1348|      -17|     HA| N395HA|    29|   SEA| OGG|     333|    2640|   9|    54|
|2014|    6|  4|    1115|        0|    1346|       -3|     AS| N461AS|   488|   SEA| SAN|     133|    1050|  11|    15|
|2014|    6| 26|    2054|       -1|    2318|       -6|     B6| N590JB|   907|   SEA| ANC|     179|    1448|  20|    54|
|2014|    6|  7|    1823|       -7|    2112|      -28|     AS| N512AS|   815|   SEA| LIH|     335|    2701|  18|    23|
|2014|    4| 30|     801|        1|    1757|       90|     AS| N407AS|    18|   SEA| MCO|     342|    2554|   8|     1|
|2014|   11| 29|     905|      155|    1655|      170|     DL| N824DN|  1598|   SEA| ATL|     229|    2182|   9|     5|
|2014|    6|  2|    2222|        7|      55|       15|     AS| N402AS|    99|   SEA| ANC|     190|    1448|  22|    22|
|2014|   11| 15|    1034|       -6|    1414|      -26|     AS| N589AS|   794|   SEA| ABQ|     139|    1180|  10|    34|
|2014|   10| 20|    1328|       -1|    1949|        4|     UA| N68805|  1212|   SEA| IAH|     228|    1874|  13|    28|
|2014|   12| 16|    1500|        0|    1906|       19|     US| N662AW|   500|   SEA| PHX|     151|    1107|  15|     0|
|2014|   11| 19|    1319|       -6|    1821|      -14|     DL| N309US|  2164|   PDX| MSP|     169|    1426|  13|    19|
|2014|    5| 21|     515|        0|     757|        0|     US| N172US|   593|   SEA| PHX|     143|    1107|   5|    15|
+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+----+--------+--------+----+------+
only showing top 20 rows
```

## Selecting

The Spark variant of SQL's `SELECT` is the `.select()` method. This method takes multiple arguments - one for each column you want to select. These arguments can either be the column name as a string (one for each column) or a column object (using the `df.colName` syntax). When you pass a column object, you can perform operations like addition or subtraction on the column to change the data contained in it, much like inside `.withColumn()`.

The difference between `.select()` and `.withColumn()` methods is that `.select()` returns only the columns you specify, while `.withColumn()` returns all the columns of the DataFrame in addition to the one you defined. It's often a good idea to drop columns you don't need at the beginning of an operation so that you're not dragging around extra data as you're wrangling. In this case, you would use `.select()` and not `.withColumn()`.

### Instructions

- Select the columns "tailnum", "origin", and "dest" from `flights` by passing the column names as strings. Save this as `selected1`.
- Select the columns "origin", "dest", and "carrier" using the `df.colName` syntax and then filter the result using both of the filters already defined for you (filterA and filterB) to only keep flights from SEA to PDX. Save this as `selected2`.

```python
# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)
```
## Selecting II

Similar to SQL, you can also use the `.select()` method to perform column-wise operations. When you're selecting a column using the `df.colName` notation, you can perform any column operation and the `.select()` method will return the transformed column. For example,

```python
flights.select(flights.air_time/60)
```

returns a column of flight durations in hours instead of minutes. You can also use the `.alias()` method to rename a column you're selecting. So if you wanted to `.select()` the column `duration_hrs` (which isn't in your DataFrame) you could do

```python
flights.select((flights.air_time/60).alias("duration_hrs"))
```

The equivalent Spark DataFrame method `.selectExpr()` takes SQL expressions as a string:

```python
flights.selectExpr("air_time/60 as duration_hrs")
```

with the SQL as keyword being equivalent to the `.alias()` method. To select multiple columns, you can pass multiple strings.

### Instructions

Create a table of the average speed of each flight both ways.

- Calculate average speed by dividing the distance by the `air_time` (converted to hours). Use the `.alias()` method name this column "avg_speed". Save the output as the variable `avg_speed`.
- Select the columns "origin", "dest", "tailnum", and `avg_speed` (without quotes!). Save this as `speed1`.
- Create the same table using `.selectExpr()` and a string containing a SQL expression. Save this as `speed2`.

```python
# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")
```

## Aggregating

All of the common aggregation methods, like `.min()`, `.max()`, and `.count()` are `GroupedData` methods. These are created by calling the `.groupBy()` DataFrame method. You'll learn exactly what that means in a few exercises. For now, all you have to do to use these functions is call that method on your DataFrame. For example, to find the minimum value of a column, `col`, in a DataFrame, `df`, you could do

```python
df.groupBy().min("col").show()
```

This creates a `GroupedData` object (so you can use the `.min()` method), then finds the minimum value in `col`, and returns it as a DataFrame.

Now you're ready to do some aggregating of your own!

### Instructions

- Find the length of the shortest (in terms of distance) flight that left PDX by first `.filter()`ing and using the `.min()` method. Perform the filtering by referencing the column directly, not passing a SQL string.
- Find the length of the longest (in terms of time) flight that left SEA by `filter()`ing and using the `.max()` method. Perform the filtering by referencing the column directly, not passing a SQL string.

```python
# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()

# Results

+-------------+
|min(distance)|
+-------------+
|          106|
+-------------+

+-------------+
|max(air_time)|
+-------------+
|          409|
+-------------+

#The filter() method is used here, and the condition is specified by directly referencing the origin column, which matches the requirement to not use a SQL string.
```

## Aggregating II

To get you familiar with more of the built in aggregation methods, here's a few more exercises involving the `flights` table!

### Instructions

- Use the `.avg()` method to get the average air time of Delta Airlines flights (where the carrier column has the value "DL") that left SEA. The place of departure is stored in the column origin. `show()` the result.
- Use the `.sum()` method to get the total number of hours all planes in this dataset spent in the air by creating a column called duration_hrs from the column air_time. `show()` the result.

```python
# Average duration of Delta flights
flights.filter(flights.origin == "SEA").filter(flights.carrier == "DL").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()

#Result

+------------------+
|     avg(air_time)|
+------------------+
|188.20689655172413|
+------------------+

+------------------+
| sum(duration_hrs)|
+------------------+
|25289.600000000126|
+------------------+
```
