import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/29/16.
  * spark-submit --class TopNConferences --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS PATH> <K>
  */
object TopNConferences {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetTopNConferences(inputPath : String,k : String): Unit = {
    val dfP = Schemas.PapersSchema(inputPath,sc,sqlContext)
    dfP.registerTempTable("Papers")
    val dfC = Schemas.ConferencesSchema(inputPath,sc,sqlContext)
    dfC.registerTempTable("Conferences")
    val query = "SELECT Conferences.sName,COUNT(Papers.pId) as NoOfP FROM Conferences INNER JOIN Papers ON Conferences.confSId=Papers.confSId GROUP BY Conferences.confSId,Conferences.sName ORDER BY NoOfP DESC LIMIT " + k
    val results = sqlContext.sql(query)
    results.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val k = args(1)
    GetTopNConferences(inputPath, k)
  }
}
