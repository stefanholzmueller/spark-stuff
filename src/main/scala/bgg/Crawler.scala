package bgg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scalaj.http.Http

// use scalaj-http in spark-shell: --packages org.scalaj:scalaj-http_2.10:1.1.4
object Crawler {
  val basePath = "C:/Users/bear/Downloads/bgg"

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/bear/") 
    val conf = new SparkConf().setAppName("ALS example").setMaster("local")
    val sc = new SparkContext(conf)

    val range: Seq[Int] = (1 to 10)
    val ids = sc.parallelize(range)
    val things = ids.map(id => (id, Http("http://boardgamegeek.com/xmlapi2/thing")
      .param("id", id.toString)
      .param("ratingcomments", "1")
      .param("pagesize", "100")
      .param("page", "1")
      .asString.body))
    things.foreach { case (id, xml) => scala.tools.nsc.io.File(basePath + "/xml/things/boardgame" + id + ".xml").writeAll(xml) }
    things.saveAsTextFile(basePath + "/rdd/things")
  }
}