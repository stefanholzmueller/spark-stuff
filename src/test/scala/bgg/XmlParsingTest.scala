package bgg

import org.junit.Assert._
import org.junit.Test

class XmlParsingTest {

  @Test
  def parse1() = {
//    val xml = readFile("XmlParsingTest1.xml")
//    val (item, ratings) = Parser.parseItemXml(xml)
//    assertEquals(Item(1, "boardgame", "Die Macher"), item)
//    assertEquals(List(ItemRating("jackcres", 1, 10), ItemRating("FuManchu", 1, 10)), ratings.take(2))
  }

  @Test
  def parse2() = {
//    val xml = readFile("XmlParsingTest2.xml")
//    val (item, ratings) = Crawler.parseItemXml(xml)
//    assertEquals(Item(1234567, "boardgame", "Die Lacher"), item)
//    assertEquals(List(ItemRating("Peter McCarthy", 1234567, 9.99)), ratings.take(2))
  }

  def readFile(fileName: String): String = {
    val resUrl = classOf[XmlParsingTest].getResource(fileName)
    val resPath = java.nio.file.Paths.get(resUrl.toURI);
    new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");
  }

}