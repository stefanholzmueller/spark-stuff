package bgg

import scalikejdbc._

case class Item(itemId: Int, itemType: String, itemName: String, yearpublished: String, image: String, thumbnail: String, ratingsCount: Int)
case class ItemRating(userName: String, itemId: Int, rating: Double)

object Parser {

  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg", "root", "root")
  implicit val session = AutoSession

  def main(args: Array[String]) {
    sql"delete from item".execute.apply()

    val xmls = sql"select body from raw where status = '200'".foreach { row =>
      val items = parseItemXml(row.string("body"))
      items foreach {
        case Item(itemId, itemType, itemName, yearpublished, image, thumbnail, ratingsCount) =>
          sql"""insert into item (itemId, type, name, yearpublished, image, thumbnail, ratingsCount) 
                values (${itemId}, ${itemType}, ${itemName}, ${yearpublished}, ${image}, ${thumbnail}, ${ratingsCount})""".update.apply()
      }
    }
  }

  def parseItemXml(xml: String): Seq[Item] = {
    try {
      val xmlElem = scala.xml.XML.loadString(removeNonPrintableCharacters(removeStrangeLeadingDots(xml)))
      val items = (xmlElem \ "item").filterNot { item =>
        val itemType = (item \ "@type").text
        itemType == "thing" /* item 50968 */ || itemType == "videogamehardware" /* item 65196 */
      }.map { item =>
        val itemId = (item \ "@id").text.toInt
        val itemType = (item \ "@type").text
        val itemName = ((item \ "name").head \ "@value").text // just pick the first
        val yearpublished = (item \ "yearpublished" \ "@value").text
        val image = (item \ "image").text
        val thumbnail = (item \ "thumbnail").text
        val totalitems = (item \ "comments" \ "@totalitems").text
        val ratingsCount = if (totalitems.isEmpty) 0 else totalitems.toInt
        Item(itemId, itemType, itemName, yearpublished, image, thumbnail, ratingsCount)
      }
      items
    } catch {
      case e: Exception => throw new RuntimeException(xml, e)
    }
  }

  def parseItemRatingXml(itemId: Int, xml: String) = {
    val xmlElem = scala.xml.XML.loadString(xml)
    val itemRatings = (xmlElem \ "item" \ "comments" \ "comment").map { commentNode =>
      val userName = (commentNode \ "@username").toString
      val rating = (commentNode \ "@rating").toString.toDouble
      ItemRating(userName, itemId, rating)
    }
  }

  def removeNonPrintableCharacters(string: String) = string.replaceAll("\\p{C}", "")

  def removeStrangeLeadingDots(string: String) = string.replaceFirst("^\\.+", "")

}