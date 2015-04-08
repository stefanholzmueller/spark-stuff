package bgg

import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scalaj.http.Http

case class Item(id: Int, itemType: String, name: String)
case class ItemRating(userName: String, itemId: Int, rating: Double)

// use scalaj-http in spark-shell: --packages org.scalaj:scalaj-http_2.10:1.1.4
object Crawler {
  val BASE_PATH = "C:/Users/bear/Downloads/bgg"
  val MAX_THING_ID = 2

  val timestamp = System.currentTimeMillis

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/bear/") // hack because ./bin/winutils.exe needs to exist
    val conf = new SparkConf().setAppName("BGG crawler").setMaster("local")
    val sc = new SparkContext(conf)

    val thingIds = sc.parallelize(1 to MAX_THING_ID)
    val things = thingIds.map(id => (id, download(id, 1)))
    things.saveAsSequenceFile(s"$BASE_PATH/$timestamp/seq")
    // load again: sc.sequenceFile("C:\\Users\\bear\\Downloads\\bgg\\1428504720279\\seq", classOf[IntWritable], classOf[Text])
    new File(s"$BASE_PATH/$timestamp/xml").mkdirs()
    things.foreach { case (id, xml) => writeFile(s"$BASE_PATH/$timestamp/xml/$id.xml", xml) }

    //    val items = things.flatMap { case (id, xml) => parseXml(xml) }
  }

  def download(id: Int, page: Int): String = Http("http://boardgamegeek.com/xmlapi2/thing")
    .param("id", Integer.toString(id))
    .param("ratingcomments", "1")
    .param("stats", "1")
    .param("pagesize", "100")
    .param("page", Integer.toString(page))
    .asString.body

  def parseItemXml(xml: String): (Item, Seq[ItemRating]) = {
    val xmlElem = scala.xml.XML.loadString(xml)
    val itemId = (xmlElem \ "item" \ "@id").toString.toInt
    val itemType = (xmlElem \ "item" \ "@type").toString
    val itemName = (xmlElem \ "item" \ "name" \ "@value").toString
    val itemRatings = (xmlElem \ "item" \ "comments" \ "comment").map { commentNode =>
      val userName = (commentNode \ "@username").toString
      val rating = (commentNode \ "@rating").toString.toDouble
      ItemRating(userName, itemId, rating)
    }
    (Item(itemId, itemType, itemName), itemRatings)
  }

  def writeFile(fileName: String, content: String) = scala.tools.nsc.io.File(fileName).writeAll(content)
}