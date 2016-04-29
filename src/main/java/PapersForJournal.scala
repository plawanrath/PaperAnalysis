import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/29/16.
  * spark-submit --class PapersForJournal --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Input Path> <HDFS Output Location> <Journal Name>
  */
object PapersForJournal {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetPaperForJournal(inputPath : String,outputPath : String,journalName : String): Unit = {
    val dfP = Schemas.PapersSchema(inputPath,sc,sqlContext)
    dfP.registerTempTable("Papers")
    val dfJ = Schemas.JournalsSchema(inputPath,sc,sqlContext)
    dfJ.registerTempTable("Journals")
    val query = "SELECT Papers.opTitle FROM Papers INNER JOIN Journals ON Papers.jId=Journals.jId WHERE Journals.jName=" + "\'"+ journalName + "\'" + " GROUP BY Papers.jId,Papers.opTitle,Journals.jName"
    val results = sqlContext.sql(query)
    val intr = results.collect()
    val total = intr.length
    val para = sc.parallelize(intr)
    para.saveAsTextFile(outputPath)
    println("Total number of Papers Published in " + journalName + " = " + total)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val jName = args(3)
    GetPaperForJournal(inputPath,outputPath,jName)
  }
}
