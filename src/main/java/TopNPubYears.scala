import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/29/16.
  * spark-submit --class TopNPubYears --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Path> <K>
  */
object  TopNPubYears {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetTopNYears(inputPath : String,k : String): Unit = {
    val dfP = Schemas.PapersSchema(inputPath,sc,sqlContext)
    dfP.registerTempTable("Papers")
    val query = "SELECT pPublishY, COUNT(pId) as NoOfP FROM Papers GROUP BY pPublishY ORDER BY NoOfP DESC LIMIT " + k
    val results = sqlContext.sql(query)
    results.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val k = args(1)
    GetTopNYears(inputPath,k)
  }
}

