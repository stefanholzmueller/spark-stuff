

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

// run with bin\spark-submit --class Recommendation path/to/target/jar
object Recommendation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ALS example")
    val sc = new SparkContext(conf)

    val ratings = sc.parallelize(List(      Rating(1, 1, 5),      Rating(1, 2, 1),      Rating(2, 1, 1),      Rating(2, 2, 5),      Rating(3, 1, 5)));
    // Load and parse the data
    //    val data = sc.textFile("data/mllib/als/test.data")
    //    val ratings = data.map(_.split(',') match {
    //      case Array(user, item, rate) =>
    //        Rating(user.toInt, item.toInt, rate.toDouble)
    //    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 20
    val lambda = 0.01
    val model = ALS.train(ratings, 1, 20, 0.01)

    // Evaluate the model on rating data
    val usersProducts = ratings.map {      case Rating(user, product, rate) =>        (user, product)    }
    val predictions =      model.predict(usersProducts).map {        case Rating(user, product, rate) =>          ((user, product), rate)      }
    val ratesAndPreds = ratings.map {      case Rating(user, product, rate) =>        ((user, product), rate)    }.join(predictions)
    // Array(((1,1),(5.0,4.994369365812157))
    //  ((1,2),(0.0,0.0018383536502297737)), 
    // (2,1),(1.0,1.0003445490826683)), ((3,1),(4.0,3.9978672872717453)),
    //, ((2,2),(4.0,3.9914009329176627)))
    // ((3,1),(4.0,3.9978672872717453)),
    // predict(3,2) => 0.6563867404903589
    
    val MSE = ratesAndPreds.map {      case ((user, product), (r1, r2)) =>        val err = (r1 - r2);        err * err    }.mean()
    println("Mean Squared Error = " + MSE)

    // Save and load model
    //    model.save(sc, "myModelPath")
    //    val sameModel = MatrixFactorizationModel.load(sc, "myModelPath")
  }
}