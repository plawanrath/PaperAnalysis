import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by Plawan on 4/26/16.
  */
object Schemas {

  def SelectedAffiliationsSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    //schema variable
    val schemaString = "affId affName"
    //creating schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.SelectedAffiliations
    val allFile = sc.textFile(inputFile)
    //converting records to RDD
    allFile.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = allFile.map(_.split("\t")).map(p => Row(p(0), p(1).trim))
    //Apply schema to RDD
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def SelectedPapersSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "pId opTitle pPublishY confSId sName"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.SelectedPapers
    val spFile = sc.textFile(inputFile)
    spFile.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = spFile.map(_.split("\t")).map(p => Row(p(0), p(1), p(2), p(3), p(4).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def AuthorsSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "AuthorId AuthorName"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.Authors
    val authorFile = sc.textFile(inputFile)
    authorFile.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = authorFile.map(_.split("\t")).map(p => Row(p(0), p(1).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def ConferenceInstancesSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "confSId confInId sName fName Loc offConfUrl confStartDate confEndDate confAbsRegDt confSubmDl confNotifDD confFinalDD"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFIle = input + "/" + InputFiles.ConferenceInstances
    val ciFile = sc.textFile(inputFIle)
    ciFile.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = ciFile.map(_.split("\t")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11).trim))
    val myDF = sqlContext.createDataFrame(rowRDD,schema)
    myDF
  }

  def ConferencesSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "confSId sName fName"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.Conferences
    val cFile = sc.textFile(inputFile)
    cFile.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = cFile.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def FieldOfStudyHierarchySchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "cFoSId cFoSL pFoSId pFoSL confidence"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.FieldOfStudyHierarchy
    val foshFile = sc.textFile(inputFile)
    foshFile.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = foshFile.map(_.split("\t")).map(p => Row(p(0), p(1), p(2), p(3), p(4).trim))
    val myDF = sqlContext.createDataFrame(rowRDD,schema)
    myDF
  }

  def FieldOfStudySchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "FoSId FoSN"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.FieldOfStudy
    val fosF = sc.textFile(inputFile)
    fosF.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = fosF.map(_.split("\t")).map(p => Row(p(0), p(1).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def JournalsSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "jId jN"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.Journals
    val jF = sc.textFile(inputFile)
    jF.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = jF.map(_.split("\t")).map(p => Row(p(0), p(1).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def PaperAuthorAffiliationsSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "pId AuthorId affId oAffName nAffName AuthorSeqNo"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.PaperAuthorAffiliations
    val jF = sc.textFile(inputFile)
    jF.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = jF.map(_.split("\t")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def PaperKeywordsSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "pId keyName FoSId"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.PaperKeywords
    val pkF = sc.textFile(inputFile)
    pkF.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = pkF.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def PaperReferencesSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "pId prId"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFile = input + "/" + InputFiles.PaperReferences
    val prF = sc.textFile(inputFile)
    prF.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = prF.map(_.split("\t")).map(p => Row(p(0), p(1).trim))
    val myDF = sqlContext.createDataFrame(rowRDD, schema)
    myDF
  }

  def PapersSchema(input : String, sc : SparkContext, sqlContext : SQLContext) : DataFrame = {
    val schemaString = "pId opTitle npTitle pPublishY pPublishD pDOI oVName nVName jId confSId pRank"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val inputFIle = input + "/" + InputFiles.Papers
    val ciFile = sc.textFile(inputFIle)
    ciFile.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val rowRDD = ciFile.map(_.split("\t")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10).trim))
    val myDF = sqlContext.createDataFrame(rowRDD,schema)
    myDF
  }
}
