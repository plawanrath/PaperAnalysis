/**
  * Created by Plawan on 4/26/16.
  * This finds all papers for a given conference
  * And the number of Papers.
  *
  * How To run: spark-submit --class KDDSelectedPaperConferences --master local PaperAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar <HDFDS Input Path>
  *   <HDFS Output Path> <Conference Short Name>
  *
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object KDDSelectedPaperConferences {

  val conf = new SparkConf().setAppName("Total Conferences")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  sc.setLogLevel("WARN")

  def GetKDDPapersInConferences(inputPath : String,outPutPath : String,conferenceName : String): Unit = {
    val dfkdd = Schemas.SelectedPapersSchema(inputPath,sc,sqlContext)
    dfkdd.registerTempTable("KDDSelectedPapers")
    val dfconf = Schemas.ConferencesSchema(inputPath,sc,sqlContext)
    dfconf.registerTempTable("Conferences")

    val query = "SELECT KDDSelectedPapers.opTitle FROM KDDSelectedPapers INNER JOIN Conferences ON KDDSelectedPapers.confSId=Conferences.confSId AND " +
      "KDDSelectedPapers.sName=Conferences.sName WHERE KDDSelectedPapers.sName=" + "\'"+conferenceName+"\'" +  "GROUP BY KDDSelectedPapers.opTitle,  KDDSelectedPapers.confSId, KDDSelectedPapers.sName"
    val results = sqlContext.sql(query)
    val intr = results.collect()
    val para = sc.parallelize(intr)
    para.saveAsTextFile(outPutPath)
    val kddQuery = "SELECT pId FROM KDDSelectedPapers"
    val totalkddPapers = sqlContext.sql(kddQuery)
    println("Total number of Papers in KDD Selected Paper Sample = " + totalkddPapers.count())
    println("Total Number of Papers that were selected for KDD and Published in "+ conferenceName + " = " + results.count())
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val confN = args(2)
    GetKDDPapersInConferences(inputPath,outputPath,confN)
  }
}
