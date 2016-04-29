import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/29/16.
  * spark-submit --class PapersOfAuthor --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Input Path> <HDFS Output Location> <Author Name>
  */
object PapersOfAuthor {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetPapersOfAuthor(inputPath : String, outPutPath : String, authorName : String): Unit = {
    val dfAa = Schemas.PaperAuthorAffiliationsSchema(inputPath,sc,sqlContext)
    dfAa.registerTempTable("PaperAuthors")
    val dfA = Schemas.AuthorsSchema(inputPath,sc,sqlContext)
    dfA.registerTempTable("Authors")
    val dfP = Schemas.PapersSchema(inputPath,sc,sqlContext)
    dfP.registerTempTable("Papers")
    val query = "SELECT Papers.opTitle FROM Papers INNER JOIN PaperAuthors ON Papers.pId=PaperAuthors.pId INNER JOIN Authors ON PaperAuthors.AuthorId=Authors.AuthorId WHERE " +
      "Authors.AuthorName=" + "\'" + authorName + "\'" + "GROUP BY Papers.pId, PaperAuthors.AuthorId, Papers.opTitle, Authors.AuthorName"
    val results = sqlContext.sql(query)
    val intr = results.collect()
    val total = intr.length
    val para = sc.parallelize(intr)
    para.saveAsTextFile(outPutPath)
    println("Total number of Papers Published by " + authorName + " = " + total)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val authorName = args(2)
    GetPapersOfAuthor(inputPath,outputPath,authorName)
  }
}

