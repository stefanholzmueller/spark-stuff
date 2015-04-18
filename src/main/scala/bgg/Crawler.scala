package bgg

import scalaj.http.Http
import scalikejdbc._

case class Item(id: Int, itemType: String, name: String)
case class ItemRating(userName: String, itemId: Int, rating: Double)

object Crawler {
  val ID_BATCHES = 2
  val ID_BATCH_SIZE = 100
  val ID_START = 201

  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg", "root", "root")
  implicit val session = AutoSession

  def main(args: Array[String]) {
    for (batch <- 0 until ID_BATCHES) {
      val start = batch * ID_BATCH_SIZE + ID_START
      val thingIds = start until start + ID_BATCH_SIZE
      download(thingIds, 1)
    }
  }

  def download(ids: Seq[Int], page: Int): Unit = {
    Thread.sleep(500)

    val map: Map[String, String] = Map(
      "id" -> ids.map { i => Integer.toString(i) }.mkString(","),
      "ratingcomments" -> "1",
      "stats" -> "1",
      "pagesize" -> "100",
      "page" -> Integer.toString(page))
    val url = "http://boardgamegeek.com/xmlapi2/thing?" + map.map { case (k, v) => k + "=" + v }.mkString("&")
    try {
      val response = Http(url).timeout(connTimeoutMs = 10000, readTimeoutMs = 50000).execute()
      if (response.code == 200) {
        sql"insert into raw (url, status, body) values (${url}, ${response.code}, ${response.body})".update.apply()
      } else {
        sql"insert into raw (url, status) values (${url}, ${response.code})".update.apply()
      }
    } catch {
      case e: Exception => sql"insert into raw (url, status) values (${url}, ${e.toString})".update.apply()
    }
  }

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