# Databricks notebook source
# MAGIC %md
# MAGIC ## The objective is to create a clean dataset that can be used for Machine Learning.
# MAGIC ## The transformations are done using Spark Dataframes through the [Pyspark API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html).

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer
from spark_udfs import convert_price_to_clean

# COMMAND ----------

# Create UDFs
convert_price_udf = udf(lambda z: convert_price_to_clean(z), StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Read the listings data into a Spark DataFrame.

# COMMAND ----------

# Read the data from CSV-file
filePath = "/FileStore/tables/listings_csv.gz"
rawDF = (spark
         .read
         .option("header", "true")
         .option("multiLine", "true")
         .option("inferSchema", "true")
         .option("escape",'"')
         .csv(filePath))

# Show the data
display(rawDF)

# COMMAND ----------

# Count the number of rows in the dataset
rawDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test your solution, make sure the number of rows read from the csv-file is as expected

# COMMAND ----------

cnt = rawDF.count()
assert cnt == 3384, f"Number of records, expected 3384 found {str(cnt)}"
assert rawDF.schema[0].dataType == IntegerType(), f"Column `id` excepted to be IntegerType, got {str(rawDF.schema[0].dataType)}"
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC For the sake of simplicity, only keep certain columns from this dataset.

# COMMAND ----------

columnsToKeep = [
  "host_is_superhost",
  "instant_bookable",
  "host_total_listings_count",
  "neighbourhood_cleansed",
  "property_type",
  "room_type",
  "accommodates",
  "bedrooms",
  "beds",
  "minimum_nights",
  "number_of_reviews",
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value",
  "price",
  "longitude",
  "latitude"
]

baseDF = rawDF.select(columnsToKeep)

# COMMAND ----------

display(baseDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fixing the data types

# COMMAND ----------

# Print the schema of the data
baseDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC If you take a look above, you will see that the `price` field got picked up as *string*. Let's see why:

# COMMAND ----------

display(baseDF.select("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC In the next cell we will create a numeric *price* column:
# MAGIC  * Retain the original *price* column under the name *price_raw*
# MAGIC  * Remove the `$` and `,` characters from the *price_raw* column, cast it to the `Decimal(10,2)` type and name it back to *price*.
# MAGIC  * You can use the udf *convert_price_udf* that was created in cell number 3
# MAGIC  * Hint, use the `withColumnRenamed` and `withColumn` functions

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code

fixedPriceDF = (baseDF
                .<FILL_IN>
               )

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the results by displaying the *price* and the *price_raw* columns

# COMMAND ----------

display(fixedPriceDF.select("price_raw", "price"))

# COMMAND ----------

# Check that the number of distinct values for price is as expected
cntDistinctPriceRaw = fixedPriceDF.select("price_raw").distinct().count()
cntDistinctPrice = fixedPriceDF.select("price").distinct().count()

assert cntDistinctPriceRaw == cntDistinctPrice, f"Number of distinct prices are different between price_raw-column ({str(cntDistinctPriceRaw)}) and price-column ({str(cntDistinctPrice)}). Expected them to be equal."
assert cntDistinctPrice == 891, f"Number of distinct prices, expected 891 found {str(cnt)}"
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Get rid of price_raw column
# MAGIC * Hint, use the `drop` function

# COMMAND ----------

# TODO: Get rid of price_raw column
fixedPriceDF = fixedPriceDF.<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert columns *host_is_superhost* and *instant_bookable* to `boolean`
# MAGIC * Hint, use the `withColumn` function and potientially `withColumnRenamed` function.
# MAGIC * Hint, use the `when` and `otherwise` functions inside the `withColumn` function.
# MAGIC * Hint, use the `drop` function if you still have the original columns for *host_is_superhost* and *instant_bookable* in order to get rid of those.

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code

changedBooleanDF = (fixedPriceDF
                    .<FILL_IN>
                   )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting rid of *nulls* (empty values)
# MAGIC 
# MAGIC Let's use the *describe* DataFrame function to see whether there are *nulls* in `changedBooleanDF`. <br />
# MAGIC The field *count* will display the number of non-null values in each column.

# COMMAND ----------

changedBooleanDF.count()

# COMMAND ----------

display(changedBooleanDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC We see that there are some columns with *null* values, the ones where the count is less than 3384. <br />
# MAGIC For each numeric column, replace the *null* values with the median of the non-null values in that column.
# MAGIC 
# MAGIC We will use the `Imputer` Spark ML module for this. The `Imputer` favors `Double` type features. So firts, let's change the type of our nominal columns to `Double`.

# COMMAND ----------

integerColumns = [x.name for x in baseDF.schema.fields if x.dataType == IntegerType()]
doublesDF = changedBooleanDF

for c in integerColumns:
  doublesDF = doublesDF.withColumn(c, col(c).cast("double"))

print("Columns converted from Integer to Double:\n - {}".format("\n - ".join(integerColumns)))

# COMMAND ----------

# See that the schema has changed
doublesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now create an *Imputer*, set its *strategy* to median and impute the columns that contain null values. This will replace empty values with the median value of that column. Keep the name of the *output Columns* the same as the *input Columns*.
# MAGIC 
# MAGIC First, add all the numeric columns that contain *null* values to the `imputeCols` array. 

# COMMAND ----------

imputeCols = [
              "host_total_listings_count",
              "beds", 
              "bedrooms",
              "review_scores_rating",
              "review_scores_accuracy",
              "review_scores_cleanliness",
              "review_scores_checkin",
              "review_scores_communication",
              "review_scores_location",
              "review_scores_value"
]

imputer = Imputer()
imputer.setStrategy("median")
imputer.setInputCols(imputeCols)
imputer.setOutputCols(imputeCols)

imputedDF = imputer.fit(doublesDF).transform(doublesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Check if there are any empty values left in the data

# COMMAND ----------

impCnt = imputedDF.count()
noNullsCnt = imputedDF.na.drop().count()
assert impCnt == noNullsCnt, "Found null values in imputedDF."

print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting rid of extreme values
# MAGIC 
# MAGIC Let's take a look at the *min* and *max* values of the `price` column:
# MAGIC 
# MAGIC *Hint*: Use the `describe` function

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
display(imputedDF.<FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see first how many listings we can find where the *price* is zero.
# MAGIC * Hint, use the `filter` and `count` functions

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code

imputedDF.<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC We also have some really expensive prices. We can use a box plot to see within what range most of our prices are located

# COMMAND ----------

display(imputedDF.select("price").where("price < 2500"))

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the box plot above we will only keep rows with a positive *price* and where the price is less than or equal to `2100`.
# MAGIC * Hint, use the `count` function

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code

posPricesDF = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC Check that all values for price are positive and less than 2100

# COMMAND ----------

cnt = posPricesDF.count()
cntExtremePrices = posPricesDF.where("price <= 0 or price > 2100").count()

assert cntExtremePrices == 0, f"Found prices that are 0 or greater than 2100, expected only positive prices and prices that are maximum 2100"
assert cnt == 3013, f"Number of records, expected 3013 found {str(cnt)}"
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the *min* and *max* values of the *minimum_nights* column:

# COMMAND ----------

display(posPricesDF.select("minimum_nights").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Group the rows by *minimum_nights*, order by the `count` column descendent and display it on a barchart:

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code

# display(<FILL_IN>)

# ANSWER
display(posPricesDF.groupBy("minimum_nights").count().orderBy(col("minimum_nights"), col("minimum_nights")))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's filter out those records where the *minimum_nights* is greater than 30:

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
cleanDF = posPricesDF.<FILL_IN>

# COMMAND ----------

display(cleanDF)

# COMMAND ----------

cleanDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Check that number of rows in the final dataframe is as expected

# COMMAND ----------

cnt = cleanDF.count()
assert cnt == 2970, f"Number of records, expected 2970 found {str(cnt)}"
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to a table

# COMMAND ----------

columnOrder = ['host_total_listings_count', 'neighbourhood_cleansed', 'property_type', 'room_type', 'accommodates', 'bedrooms', 'beds', 'minimum_nights', 'number_of_reviews', 'review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness', 'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 'review_scores_value', 'longitude', 'latitude', 'price', 'host_is_superhost', 'instant_bookable']

cleanDF.select(columnOrder).write.mode("overwrite").format("delta").saveAsTable("AirbnbOslo")
