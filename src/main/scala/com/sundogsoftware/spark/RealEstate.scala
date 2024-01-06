package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql._

object RealEstate {

  case class RegressionSchema(No: Int, TransactionDate: Double, HouseAge: Double, DistanceToMRT: Double,
                              NumberConvenienceStores: Int, Latitude: Double, Longitude: Double, PriceOfUnitArea: Double)
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // In machine learning lingo, "label" is just the value you're trying to predict, and
    // "feature" is the data you are given to make a prediction with. So in this example
    // the "labels" are the first column of our data, and "features" are the second column.
    // You can have more than one "feature" which is why a vector is required.

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("interSchema", "true")
      .csv("data/regression.txt")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("HouseAge", "DistanceToMRT", "NumberConvenienceStores")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
      .select("PriceOfUnitArea", "features")

    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    // Now create our linear regression model
    val lir = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")

    // Train the model using our training data
    val model = lir.fit(trainingDF)

    // Now see if we can predict values in our test data.
    // Generate predictions using our linear regression model for all features in our
    // test dataframe:
    val fullPredictions = model.transform(testDF).cache()

    // This basically adds a "prediction" column to our testDF dataframe.

    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "PriceOfUnitArea").collect()

    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    // Stop the session
    spark.stop()

  }
}