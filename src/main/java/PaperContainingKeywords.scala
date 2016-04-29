import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/27/16.
  * spark-submit --class PaperContainingKeywords --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Input Path> <HDFS Output Path> <Keyword>
  */
object PaperContainingKeywords {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetPapersMatchingKeywords(inputpath : String,outputPath : String,keyword : String): Unit = {
    val dfpk = Schemas.PaperKeywordsSchema(inputpath,sc,sqlContext)
    dfpk.registerTempTable("PaperKeywords")
    val dfp = Schemas.PapersSchema(inputpath,sc,sqlContext)
    dfp.registerTempTable("Papers")

    val query = "SELECT Papers.opTitle FROM Papers INNER JOIN PaperKeywords ON Papers.pId=PaperKeywords.pId " +
    "WHERE PaperKeywords.keyName=" + "\'"+keyword+"\'"
    val results = sqlContext.sql(query)
    val intr = results.collect()
    val para = sc.parallelize(intr)
    para.saveAsTextFile(outputPath)
    println("Total Number of Papers matching the Keyword " + keyword + " = " + results.count())
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val keyword = args(2)
    GetPapersMatchingKeywords(inputPath,outputPath,keyword)
  }
}
