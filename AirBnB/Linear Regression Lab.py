# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC #Linear Regression Lab with Airbnb
# MAGIC 
# MAGIC The dataset we'll be working with is from Airbnb rentals in Oslo<br>
# MAGIC 
# MAGIC You can find more information here:<br>
# MAGIC http://insideairbnb.com/get-the-data.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading the data

# COMMAND ----------

# MAGIC %md
# MAGIC In order to start looking at how to use the table for Machine Learning, we need to first import the table

# COMMAND ----------

airbnbDF = spark.read.table("AirbnbOslo")

# COMMAND ----------

# MAGIC %md
# MAGIC Similarly as before, let's explore the dataset values using describe

# COMMAND ----------

display(airbnbDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC We should cache the dataset to have faster execution

# COMMAND ----------

airbnbDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make sure we don't have any null values in our DataFrame

# COMMAND ----------

recordCount = airbnbDF.count()
noNullsRecordCount = airbnbDF.na.drop().count()

print("We have {} records that contain null values.".format(recordCount - noNullsRecordCount))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Exploratory data analysis

# COMMAND ----------

# MAGIC %md
# MAGIC First, create a view calles `airbnb` from our dataset so you can move on using both the DataFrame or the SQL API.

# COMMAND ----------

airbnbDF.createOrReplaceTempView("airbnb")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make a histogram of the price column to explore it (change the number of bins inside plot options to 50).  

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
display(<FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC Is this a <a href="https://en.wikipedia.org/wiki/Log-normal_distribution" target="_blank">Log Normal</a> distribution? Take the `log` of price and check the histogram.
# MAGIC 
# MAGIC *Hint*: Use the `log` function (https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.log.html)

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
from pyspark.sql.functions import *

display(<FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC Now take a look at how `price` depends on some of the variables:
# MAGIC * Plot `price` vs `bedrooms`
# MAGIC * Plot `price` vs `accomodates`
# MAGIC 
# MAGIC *Hint* look at Plot Options

# COMMAND ----------

# TODO Plot price vs bedrooms

# COMMAND ----------

# TODO Plot price vs the "accomodates" column

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the distribution of some of our categorical features

# COMMAND ----------

display(airbnbDF.groupBy("room_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Which neighborhoods have the highest number of rentals? Display the neighbourhoods and their associated count in descending order.
# MAGIC 
# MAGIC *Hint* - Use `groupBy`, `count` and `orderBy`.

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
display(airbnbDF.groupBy(<FILL_IN>).count().orderBy(col(<FILL_IN>).desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### How much does the price depend on the location? (Zoom in to compare specific neighbourhoods)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import col
# MAGIC mapDF = spark.table("airbnb")
# MAGIC v = ",\n".join(map(lambda row: "[{}, {}, {}]".format(row[0], row[1], row[2]), mapDF.select(col("latitude"),col("longitude"),col("price")/600).collect()))
# MAGIC displayHTML("""
# MAGIC <html>
# MAGIC <head>
# MAGIC  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.3.1/dist/leaflet.css"
# MAGIC    integrity="sha512-Rksm5RenBEKSKFjgI3a41vrjkw4EVPlJ3+OiI65vTjIdo9brlAacEuKOiQ5OFh7cOI1bkDwLqdLw3Zg0cRJAAQ=="
# MAGIC    crossorigin=""/>
# MAGIC  <script src="https://unpkg.com/leaflet@1.3.1/dist/leaflet.js"
# MAGIC    integrity="sha512-/Nsx9X4HebavoBvEBuyp3I7od5tA0UzAxs+j83KgC8PU0kgB4XiK4Lfe4y4cgBtaRJQEIFCW+oC506aPT2L1zw=="
# MAGIC    crossorigin=""></script>
# MAGIC  <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
# MAGIC </head>
# MAGIC <body>
# MAGIC     <div id="mapid" style="width:700px; height:500px"></div>
# MAGIC   <script>
# MAGIC   var mymap = L.map('mapid').setView([59.9192,10.7463], 12);
# MAGIC   var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
# MAGIC     attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
# MAGIC }).addTo(mymap);
# MAGIC   var heat = L.heatLayer([""" + v + """], {radius: 25}).addTo(mymap);
# MAGIC   </script>
# MAGIC   </body>
# MAGIC   </html>
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train a Linear Regression Model
# MAGIC 
# MAGIC Before we can apply the linear regression model, we will need to do some data preparation, such as one hot encoding our categorical variables using `StringIndexer` and `OneHotEncoderEstimator`.
# MAGIC 
# MAGIC Let's start by taking a look at all of our columns, and determine which ones are categorical.

# COMMAND ----------

airbnbDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) StringIndexer
# MAGIC 
# MAGIC [Python Docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
# MAGIC 
# MAGIC [Scala Docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.StringIndexer)

# COMMAND ----------

display(airbnbDF.describe())

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

print(StringIndexer().explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now *StringIndex* all categorical features (`neighbourhood_cleansed`, `room_type`, `property_type`) and set `handleInvalid` to `skip`. Set the output columns to `cat_neighbourhood_cleansed`, `cat_room_type` and `cat_property_type`, respectively.

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
iNeighbourhood = StringIndexer(inputCol="neighbourhood_cleansed", outputCol="cat_neighborhood", handleInvalid="skip")
iRoomType = <FILL_IN>
iPropertyType = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

assert iNeighbourhood.getOutputCol() == "cat_neighborhood", "iNeighbourhood: Expected output column 'cat_neighborhood', got '" + iNeighbourhood.getOutputCol() + "'"
assert iRoomType.getOutputCol() == "cat_room_type", "iRoomType: Expected output column 'cat_room_type', got '" + iRoomType.getOutputCol() + "'"
assert iPropertyType.getOutputCol() == "cat_property_type", "iPropertyType: Expected output column 'cat_property_type', got '" + iRoomType.getOutputCol() + "'"
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) OneHotEncoder
# MAGIC 
# MAGIC One-hot encode all previously indexed categorical features. Call the output colums `vec_neighborhood`, `vec_room_type` and `vec_property_type`, respectively.

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoderEstimator # OneHotEncoder for new spark version > 3

oneHotEnc = OneHotEncoderEstimator()
oneHotEnc.setInputCols(["cat_neighborhood", "cat_room_type", "cat_property_type"])
oneHotEnc.setOutputCols(["vec_neighborhood", "vec_room_type", "vec_property_type"])

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

assert set(oneHotEnc.getInputCols()) == set(["cat_neighborhood", "cat_room_type", "cat_property_type"]), 'oneHotEnc expected inputCols: "cat_neighborhood", "cat_room_type", "cat_property_type"'
        
assert set(oneHotEnc.getOutputCols()) == set(["vec_neighborhood", "vec_room_type", "vec_property_type"]), 'oneHotEnc expected outputCols: "vec_neighborhood", "vec_room_type", "vec_property_type"'

print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train/Test Split
# MAGIC 
# MAGIC Let's keep 80% for the training set and set aside 20% of our data for the test set. 
# MAGIC 
# MAGIC The seed value is used to have reproducible results (same random numbers each time it's run)
# MAGIC 
# MAGIC *Hint*: Use `randomSplit` function

# COMMAND ----------

# # TODO: Replace <FILL_IN> with appropriate code
seed = 273
(testDF, trainDF) = airbnbDF.<FILL_IN>

print(testDF.count(), trainDF.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Pipeline
# MAGIC 
# MAGIC Let's build some of the transformations we'll need in our pipeline, such as `VectorAssembler` and `LinearRegression`.

# COMMAND ----------

# MAGIC %md
# MAGIC First, we need to choose the features that will fuel the predictions. It is common to choose a smaller subset of relevant feature to help prevent overfitting. (More info on overfitting https://www.ibm.com/cloud/learn/overfitting)

# COMMAND ----------

featureCols = [
 "accommodates",
 "bedrooms",
 "beds",
 "minimum_nights",
 "number_of_reviews",
 "review_scores_rating",
 "vec_neighborhood", 
 "vec_room_type", 
 "vec_property_type", 
]

# COMMAND ----------

# MAGIC %md
# MAGIC Set the input columns of the `VectorAssembler` to `featureCols`, the output column to `features` and create a `LinearRegression` that uses the `price` as label. :
# MAGIC 
# MAGIC  * <a href="https://spark.apache.org/docs/latest/ml-features.html#vectorassembler" target="_blank">VectorAssembler Docs</a>
# MAGIC  * <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression" target="_blank">Linear Regression Docs</a>

# COMMAND ----------

# # TODO: Replace <FILL_IN> with appropriate code

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

assembler = VectorAssembler(inputCols=<FILL_IN>, outputCol=<FILL_IN>)

lr = (LinearRegression()
      .setLabelCol("price")
      .setFeaturesCol("features"))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's put this all together in a pipeline!
# MAGIC 
# MAGIC Set `iNeighbourhood`, `iRoomType`, `iPropertyType`, `oneHotEnc`, `assembler` and `lr` as the pipeline stages and train a model on the training data:
# MAGIC 
# MAGIC *Hint*: Link use `setStages` and `fit` methods https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.Pipeline.html

# COMMAND ----------

# # TODO: Replace <FILL_IN> with appropriate code
from pyspark.ml import Pipeline

lrPipeline = Pipeline()

# Set the stages of the Pipeline
<FILL_IN>

# Train our model
lrPipelineModel = lrPipeline.<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

assert len(lrPipeline.getStages()) == 6, "Expected 6 stages in the pipeline. 3 StringIndexers, the OneHotEncoredEstimator, the VectorAssembler and the Linear Regression"

print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's apply the model to our held-out test set.
# MAGIC 
# MAGIC *Hint*: Use `transform` method

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
predictedDF = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Evaluate the Model

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator()
print(evaluator.explainParams())

# COMMAND ----------

evaluator.setLabelCol("price")
evaluator.setPredictionCol("prediction")

metricName = evaluator.getMetricName()
metricVal = evaluator.evaluate(predictedDF)

print("{}: {}".format(metricName, metricVal))

# COMMAND ----------

# MAGIC %md
# MAGIC We could wrap this into a function to make it easier to get the output of multiple metrics.

# COMMAND ----------

def printEval(df, labelCol = "price", predictionCol = "prediction"):
  evaluator = RegressionEvaluator()
  evaluator.setLabelCol(labelCol)
  evaluator.setPredictionCol(predictionCol)

  rmse = evaluator.setMetricName("rmse").evaluate(df)
  r2 = evaluator.setMetricName("r2").evaluate(df)
  print("RMSE: {}\nR2: {}".format(rmse, r2))

# COMMAND ----------

printEval(predictedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Conclusion
# MAGIC Hmmmm... our RMSE was really high. How could we lower it?  Let's try converting our price target to a logarithmic scale.
# MAGIC 
# MAGIC Let's display the histogram of log("price") to confirm that the distribution is log-normal:

# COMMAND ----------

from pyspark.sql.functions import *

display(airbnbDF.select(log("price")))

# COMMAND ----------

logTrainDF = trainDF.withColumn("log_price", log(col("price")))
logTestDF = testDF.withColumn("log_price", log(col("price")))

# COMMAND ----------

lr.setLabelCol("log_price")
logPipelineModel = lrPipeline.fit(logTrainDF)

predictedLogDF = logPipelineModel.transform(logTestDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Exponentiate
# MAGIC 
# MAGIC In order to interpret our RMSE, we need to convert our predictions back from logarithmic scale.

# COMMAND ----------

expDF = predictedLogDF.withColumn("exp_pred", exp(col("prediction")))
printEval(expDF, "price", "exp_pred")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Light GBM
# MAGIC Perhaps try a different algorithm? Let's look at Light GBM (install this Spark package: `Azure:mmlspark:0.12`). Light GBM is an alternative gradient boosting technique to XGBoost to significantly speed up the computation.

# COMMAND ----------

from mmlspark import LightGBMRegressor
# THIS NEEDS TO BE INSTALLED TO THE CLUSTER Azure:mmlspark:0.12 as maven
gbmModel = LightGBMRegressor(learningRate=.1,
                           numIterations=100,
                           numLeaves=50,
                           labelCol="log_price")

gbmPipeline = Pipeline(stages = [iNeighbourhood, iRoomType, iPropertyType, oneHotEnc, assembler, gbmModel])

gbmPipelineModel = gbmPipeline.fit(logTrainDF)
gbmLogPredictedDF = gbmPipelineModel.transform(logTestDF)

expGbmDF = gbmLogPredictedDF.withColumn("exp_pred", exp(col("prediction")))
printEval(expGbmDF, "price", "exp_pred")

# COMMAND ----------

# MAGIC %md
# MAGIC Wow! The gradient boosted trees did much better than linear regression!
# MAGIC 
# MAGIC Go back through this notebook and try to see how low you can get the RMSE!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
