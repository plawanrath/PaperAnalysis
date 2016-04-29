import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/29/16.
  * spark-submit --class TopNLocations --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Path> <K>
  */

object TopNLocations {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetTopLocations(inputPath : String,k : String): Unit = {
    val dfL = Schemas.ConferenceInstancesSchema(inputPath,sc,sqlContext)
    dfL.registerTempTable("Conferences")
    val query = "SELECT Loc, COUNT(confSId) as NoOfC FROM Conferences GROUP BY Loc ORDER BY NoOfC DESC LIMIT " + k
    val results = sqlContext.sql(query)
    results.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val k = args(1)
    GetTopLocations(inputPath, k)
  }
}
