

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.junit.Test
import org.junit.Assert

class CollaborativeFilteringTest {

  def testWithCleanSlate(body: SparkContext => Unit) = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    val sc = new SparkContext("local", getClass().getSimpleName())
    try {
      body(sc)
    } finally {
      sc.stop()
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }

  @Test
  def correctPrediction = {
    testWithCleanSlate { sc =>
      val ratings = sc.parallelize(List(Rating(1, 1, 5), Rating(1, 2, 1), Rating(2, 1, 1), Rating(2, 2, 5), Rating(3, 1, 5)));
      val rank = 1
      val iterations = 20 // 10 => predict 6.065
      val lambda = 0.01
      val blocks = 1
      val seed = 123456789
      val model = ALS.train(ratings, 1, 20, 0.01, -1, 123456789)
      Assert.assertEquals(4.953176333258341, model.predict(3, 2), 0.00000001)
    }
  }

}
