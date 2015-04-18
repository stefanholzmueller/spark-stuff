
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SequenceFile {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/bear/") // hack because ./bin/winutils.exe needs to exist

    val conf = new SparkConf().setAppName("sequence file spike").setMaster("local")
    val sc = new SparkContext(conf)

    val ids = sc.parallelize(1 to 100)
    val squares = ids.map { id =>
      Thread.sleep(1000)
      (id, id * id)
    }
    squares.saveAsSequenceFile("C:/Users/bear/tmp/seq")
    // if this program is killed while running, the re-loaded seq file is empty... saving seems to be atomic
  }
}