package bgg

import io.prediction.Event
import io.prediction.EventClient
import java.io.File
import scalikejdbc._

object PioEventClient extends App {
  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg", "root", "root")
  implicit val session = AutoSession

  val itemratings = sql"SELECT itemratingid, userId, itemId, rating FROM itemrating"

  System.setProperty("line.separator", "\n"); // not sure if necessary
  printToFile(new File("test.json")) { p =>
    itemratings.foreach { row =>
      val id = row.int("itemratingid")
      val userId = row.string("userId")
      val itemId = row.string("itemId")
      val rating = row.float("rating")

      val json = s"""{"event":"rate","entityType":"user","entityId":"$userId","targetEntityType":"item","targetEntityId":"$itemId","properties":{"rating":$rating}}"""
      p.println(json)
    }
  }
  // manually delete empty line at the end with:
  // truncate -s -"$(tail -n1 /tmp/test.json | wc -c)" /tmp/test.json

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def api() {
    def client = new EventClient("qSFP077xtSQbrehSQSyfZcECJ0Nkyrx4ShJtYukJcNRHb5CC0KpdlNRof5c0uXl6", "http://192.168.178.38:7070");
    def rateEvent = new Event()
      .event("rate")
      .entityType("user")
      .entityId("123123")
      .targetEntityType("item")
      .targetEntityId("123123")
      .property("rating", 10.0)
    client.createEvent(rateEvent)
  }

}