package bgg

import scalikejdbc._

case class Item(itemId: Int, itemType: String, itemName: String, yearpublished: String, image: String, thumbnail: String, ratingsCount: Int)
case class ItemRating(userName: String, itemId: Int, rating: Double)

object Parser {

  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg", "root", "root")
  implicit val session = AutoSession

  def main(args: Array[String]) {
    val xmls = sql"select body from raw where status = '200'".map(_.string("body")).list.apply()
    for (xml <- xmls) {
      val items = parseItemXml(xml)
      items foreach {
        case Item(itemId, itemType, itemName, yearpublished, image, thumbnail, ratingsCount) =>
          sql"insert into item (itemId, type, name, yearpublished, image, thumbnail, ratingsCount) values (${itemId}, ${itemType}, ${itemName}, ${yearpublished}, ${image}, ${thumbnail}, ${ratingsCount})".update.apply()
      }
    }
  }

  def parseItemXml(xml: String): Seq[Item] = {
    val xmlElem = scala.xml.XML.loadString(xml)
    val items = (xmlElem \ "item").map { item =>
      val itemId = (item \ "@id").text.toInt
      val itemType = (item \ "@type").text
      val itemName = ((item \ "name").head \ "@value").text
      val yearpublished = (item \ "yearpublished" \ "@value").text
      val image = (item \ "image").text
      val thumbnail = (item \ "thumbnail").text
      val ratingsCount = (item \ "comments" \ "@totalitems").text.toInt
      Item(itemId, itemType, itemName, yearpublished, image, thumbnail, ratingsCount)
    }
    items
  }

  def parseItemRatingXml(itemId: Int, xml: String) = {
    val xmlElem = scala.xml.XML.loadString(xml)
    val itemRatings = (xmlElem \ "item" \ "comments" \ "comment").map { commentNode =>
      val userName = (commentNode \ "@username").toString
      val rating = (commentNode \ "@rating").toString.toDouble
      ItemRating(userName, itemId, rating)
    }
  }
}