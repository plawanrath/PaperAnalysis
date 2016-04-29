import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Plawan on 4/29/16.
  * spark-submit --class PapersInYear --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFS Input Path> <HDFS Output Location> <Year>
  */

object PapersInYear {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetAllPapersInYear(inputPath : String,outputPath : String,year : String): Unit = {
    val dfP = Schemas.PapersSchema(inputPath,sc,sqlContext)
    dfP.registerTempTable("Papers")
    val query = "SELECT opTitle FROM Papers WHERE pPublishY=" + "\'"+ year + "\'"
    val results = sqlContext.sql(query)
    val intr = results.collect()
    val total = intr.length
    val para = sc.parallelize(intr)
    para.saveAsTextFile(outputPath)
    println("Total number of Papers Published in " + year + " = " + total)
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val year = args(2)
    GetAllPapersInYear(inputPath,outputPath,year)
  }
}
