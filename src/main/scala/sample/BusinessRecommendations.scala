package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.bson.Document
    
case class UserReviewRating(userId: Int, businessId: Int, review_stars: Double)

object BusinessRecommendations {

  def main(args: Array[String]) {
  
    //Check for arguments for mongodb URI
    if (args.length != 4) {
        Console.err.println("Usage:")
        Console.err.println("      businessRecommendations.jar <host> <port> <db_name> <userId>")
        System.exit(1)
    }
  
    val HOST = args(0)
    val PORT = args(1)
    val DB = args(2)
    val USER = args(3).toInt

    val mongoURI = "mongodb://" + HOST + ":" + PORT + "/" + DB
    println( "mongoURI: " + mongoURI )
    println( "given userId: " + USER )
    
    // Set up spark configurations
    val sc = getSparkContext()
    val sqlContext = SQLContext.getOrCreate(sc)
    sc.parallelize(1 to 10000)
  
    val readReviewRatings = ReadConfig(Map("uri" -> (mongoURI + ".review_ratings")))
    val writeConfig = WriteConfig(Map("uri" -> (mongoURI + ".user_recommendations")))
    
    // Load the review review_stars data
    val reviewRatings = MongoSpark.load(sc, readReviewRatings)

    // Using MongoDB spark connector pre-filter to get 10K docs with ratings 3 or better
    val filteredRatings = reviewRatings.withPipeline(Seq(Document.parse("{$match : { review_stars: {$gte : 3}}}, {$limit: 10000}"))).toDF[UserReviewRating]

    
    // Create the ALS instance and map the review data
    val als = new ALS()
        .setCheckpointInterval(2)
        .setUserCol("userId")
        .setItemCol("businessId")
        .setRatingCol("review_stars")
    
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using the ALS evaluator.
    val paramGrid = new ParamGridBuilder()
        .addGrid(als.regParam, Array(0.1, 10.0))
        .addGrid(als.rank, Array(8, 10))
        .addGrid(als.maxIter, Array(10, 20))
        .build()
    
    // Set the training and validation split - 80% of the data will be used for training and the remaining 20% for validation.
    val trainedAndValidatedModel = new TrainValidationSplit()
        .setEstimator(als)
        .setEvaluator(new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("review_stars")
        .setPredictionCol("prediction"))
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
    
    // Calculating the best model
    val bestModel = trainedAndValidatedModel.fit(filteredRatings)
    
    // Read personal ratings collection 
    val userRatingsRDD = MongoSpark.load(sc, readReviewRatings.copy(collectionName = "personal_ratings"))

    // Using MongoDB spark connector pre-filter to get specific user's preferences
    val userRatings = userRatingsRDD.withPipeline(Seq(Document.parse("{$match : { userId: " + USER + " }}"))).toDF[UserReviewRating]
  
    // Combine the datasets
    val combinedRatings = filteredRatings.unionAll(userRatings)
  
    // Retrain using the combinedRatings
    val combinedModel = als.fit(combinedRatings, bestModel.extractParamMap())
    
    import sqlContext.implicits._

    val userId = USER;
    val unratedBusinesses = filteredRatings.filter(s"userId != $userId").select("businessId").distinct().map(r =>
          (userId, r.getAs[Int]("businessId"))).toDF("userId", "businessId")
    val recommendations = combinedModel.transform(unratedBusinesses)
  
    // Convert the recommendations into UserBusinessRatings
    val userRecommendations = recommendations.map(r =>
          UserReviewRating(userId, r.getAs[Int]("businessId"), r.getAs[Float]("prediction").toInt)).toDF()
  
    // The newly generated recommendations can now be written back to MongoDB and accessed by the business application.
    MongoSpark.save(userRecommendations, writeConfig)
  
    // Clean up
    sc.stop()
  
    }
  
    /**
     * Gets or creates the Spark Context
     */
    def getSparkContext(): SparkContext = {
      val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("BusinessRatings")
  
      val sc = SparkContext.getOrCreate(conf)
      sc.setCheckpointDir("/tmp/checkpoint/")
      sc
  }
}
