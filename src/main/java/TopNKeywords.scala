import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/28/16.
  * spark-submit --class TopNKeywords --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Path> <K>
  */
object TopNKeywords {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetTopNKeywords(inputPath : String,k : String): Unit = {
    val dfpk = Schemas.PaperKeywordsSchema(inputPath,sc,sqlContext)
    dfpk.registerTempTable("PaperKeywords")
    val query = "SELECT keyName,COUNT(pId) AS Counts FROM PaperKeywords GROUP BY keyName ORDER BY Counts DESC LIMIT " + k
    val results = sqlContext.sql(query)
    results.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val k = args(1)
    GetTopNKeywords(inputPath,k)
  }
}

