import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/28/16.
  * spark-submit --class TopNFieldOfStudy --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Path> <K>
  */
object TopNFieldOfStudy {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetTopNFields(inputPath : String,k : String) : Unit = {
    val dfpk = Schemas.PaperKeywordsSchema(inputPath,sc,sqlContext)
    dfpk.registerTempTable("PaperKeywords")
    val dffos = Schemas.FieldOfStudySchema(inputPath,sc,sqlContext)
    dffos.registerTempTable("FieldOfStudy")

    val query = "SELECT FieldOfStudy.FoSN, COUNT(PaperKeywords.pId) as NoOfP FROM FieldOfStudy INNER JOIN PaperKeywords ON FieldOfStudy.FoSId=PaperKeywords.FoSId GROUP BY FieldOfStudy.FoSId,FieldOfStudy.FoSN ORDER BY NoOfP DESC limit " + k
    val results = sqlContext.sql(query)
    results.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val k = args(1)
    GetTopNFields(inputPath,k)
  }
}

