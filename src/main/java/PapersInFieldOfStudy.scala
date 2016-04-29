import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/29/16.
  * spark-submit --class PapersInFieldOfStudy --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Input Path> <HDFS Output Location> <Field Of Study>
  */
object PapersInFieldOfStudy {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetPapersOnFieldOfStudy(inputPath : String,outputPath : String,fos : String): Unit = {

    val dffos = Schemas.FieldOfStudySchema(inputPath,sc,sqlContext)
    dffos.registerTempTable("FieldOfStudy")
    val dfPk = Schemas.PaperKeywordsSchema(inputPath,sc,sqlContext)
    dfPk.registerTempTable("PaperKeywords")
    val dfP = Schemas.PapersSchema(inputPath,sc,sqlContext)
    dfP.registerTempTable("Papers")
    val query = "SELECT Papers.opTitle FROM Papers INNER JOIN PaperKeywords ON Papers.pId=PaperKeywords.pId INNER JOIN FieldOfStudy ON PaperKeywords.FoSId=FieldOfStudy.FoSId " +
      "WHERE FieldOfStudy.FoSN=" + "\'" + fos + "\'" + "GROUP BY Papers.pId, PaperKeywords.FoSId, Papers.opTitle, FieldOfStudy.FoSN"
    val results = sqlContext.sql(query)
    val intr = results.collect()
    val total = intr.length
    val para = sc.parallelize(intr)
    para.saveAsTextFile(outputPath)
    println("Total number of Papers Published by " + fos + " = " + total)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val fos = args(2)
    GetPapersOnFieldOfStudy(inputPath,outputPath,fos)
  }

}

