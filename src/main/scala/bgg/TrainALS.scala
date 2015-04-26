package bgg

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
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

    val ratings = jdbcDF.map { row => Rating(row.getInt(2), row.getInt(3), row.getDouble(4)) }.cache()
    val model = ALS.train(ratings, 100, 60, 0.01) // hit gc overhead limit => --driver-memory 6g

    val prediction = model.predict(stholzm, pandemic)
    val recommendations = model.recommendProducts(stholzm, 100)

    println(prediction)
    println(recommendations.toList)

    val itemDF = sqlContext.load("jdbc", Map("url" -> JDBC_URL, "dbtable" -> "item"))
    val recRdd = sc.parallelize(recommendations, 1)
    val left = recRdd.map { case Rating(user, product, rating) => (product, rating) }
    val right = itemDF.filter("ratingscount > 10").map { df => (df.getInt(0), df.getString(2)) }
    val newGames = left.join(right)
    newGames.foreach(println)

    save(model)
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

}