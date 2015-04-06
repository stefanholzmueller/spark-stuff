package bgg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scalaj.http.Http
import java.io.File

// use scalaj-http in spark-shell: --packages org.scalaj:scalaj-http_2.10:1.1.4
object Crawler {
  val basePath = "C:/Users/bear/Downloads/bgg"
  val timestamp = System.currentTimeMillis

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/bear/") // hack because ./bin/winutils.exe needs to exist
    val conf = new SparkConf().setAppName("BGG crawler").setMaster("local")
    val sc = new SparkContext(conf)

    val ids = sc.parallelize(1 to 10)
    val things = ids.map(id => (id, download(id, 1).replaceAll("\\n", ""))) // not good

    new File(s"$basePath/$timestamp/xml").mkdirs()
    things.foreach { case (id, xml) => writeFile(s"$basePath/$timestamp/xml/$id.xml", xml) }
    things.map { case (id, xml) => xml }.saveAsTextFile(s"$basePath/$timestamp/rdd") // saves tuple as string // and assumes one entry per line
  }

  def download(id: Int, page: Int): String = Http("http://boardgamegeek.com/xmlapi2/thing")
    .param("id", Integer.toString(id))
    .param("ratingcomments", "1")
    .param("pagesize", "100")
    .param("page", "1")
    .asString.body

  def writeFile(fileName: String, content: String) = scala.tools.nsc.io.File(fileName).writeAll(content)
}