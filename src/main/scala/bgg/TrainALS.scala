package bgg

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.param.DoubleParam

object TrainALS {

  def main(args: Array[String]) {
    val JDBC_URL = "jdbc:mysql://localhost:3306/bgg?user=root&password=root"

    System.setProperty("hadoop.home.dir", "C:/Users/bear/") // hack because ./bin/winutils.exe needs to exist
    Class.forName("com.mysql.jdbc.Driver")

    val conf = new SparkConf().setAppName("ALS").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jdbcDF = sqlContext.load("jdbc", Map("url" -> JDBC_URL, "dbtable" -> "itemrating"))
    jdbcDF.registerTempTable("itemrating")
    val stholzm = sqlContext.sql("SELECT userId FROM itemrating WHERE username='stholzm'").collect()(0).getInt(0)
    val pandemic = 30549

    val ratings: RDD[Rating[Int]] = jdbcDF.map { row => Rating(row.getInt(2), row.getInt(3), row.getDouble(4).toFloat) }.cache()
    val als = new ALS().setMaxIter(3).setRank(3)

    val pipeline = new Pipeline().setStages(Array(als))

    import sqlContext.implicits._
    val model = pipeline.fit(ratings.toDF)

    val crossval = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator)
    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, Array(1, 0.1, 0.01))
      .build()
    crossval.setEstimatorParamMaps(paramGrid)
    crossval.setNumFolds(2) // Use 3+ in practice
    val cvModel = crossval.fit(ratings.toDF)

    val test = sc.parallelize(List(Rating(stholzm, pandemic, 0)))
    val prediction1 = model.getModel(als).transform(test.toDF()).collect()
    val prediction2 = cvModel.bestModel.transform(test.toDF()).collect()

    println(cvModel.bestModel.fittingParamMap.get(als.regParam))
    prediction1.foreach(println) // [106220,30549,0.0,7.794441]
    prediction2.foreach(println) // [106220,30549,0.0,6.2156687]

    //    val prediction = model.predict(stholzm, pandemic)
    //    val recommendations = model.recommendProducts(stholzm, 100)
    //
    //    println(prediction)
    //    println(recommendations.toList)
    //
    //    val itemDF = sqlContext.load("jdbc", Map("url" -> JDBC_URL, "dbtable" -> "item"))
    //    val recRdd = sc.parallelize(recommendations, 1)
    //    val left = recRdd.map { case Rating(user, product, rating) => (product, rating) }
    //    val right = itemDF.filter("ratingscount > 10").map { df => (df.getInt(0), df.getString(2)) }
    //    val newGames = left.join(right)
    //    newGames.foreach(println)
    //
    //    save(model)
  }

  def save(model: MatrixFactorizationModel) {
    model.userFeatures.saveAsObjectFile("C:/Users/bear/tmp/userFeatures.obj")
    model.productFeatures.saveAsObjectFile("C:/Users/bear/tmp/productFeatures.obj")
  }

  def load(sc: SparkContext): MatrixFactorizationModel = {
    val userFeatures: RDD[(Int, Array[Double])] = sc.objectFile("C:/Users/bear/tmp/userFeatures.obj")
    val productFeatures: RDD[(Int, Array[Double])] = sc.objectFile("C:/Users/bear/tmp/productFeatures.obj")
    new MatrixFactorizationModel(100, userFeatures, productFeatures)
  }

  def evaluator: org.apache.spark.ml.Evaluator = new Evaluator {
    // dataset.toString === [user: int, item: int, rating: float, prediction: float]
    // paramMap does not contain the actual regParam for this iteration
    def evaluate(dataset: DataFrame, paramMap: ParamMap): Double = { // higher is better (?)
      -dataset.map { row =>
        {
          val rating = row.getFloat(2).toDouble
          val prediction = row.getFloat(3).toDouble
          val dist = rating - prediction
          dist * dist
        }
      }.reduce(_+_)
    }
  }

}