package it.au.misure.ingestionMisureGasUnico.validate

import java.io.File
import it.au.misure.ingestionMisureGasUnico.flow.standard.StandardFlow
import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita.AmmissibilitaPDRSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita.AmmissibilitaPDRSchema.{cartella_cloud, codice_inamissibilita, d_caricamento, nome_file}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.TracciatoStandardEnum.TracciatoStandardEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RettificaSchema
import it.au.misure.ingestionMisureGasUnico.model.validate.ReportEsitoPDRMessage
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{EnvironmentSparkTest, ParseUtility, PropertyUtility}
import junit.framework.TestCase
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DatasetHolder, SQLContext, SparkSession}
import org.junit.{Assert, Ignore}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

class TestFlowLoadDataValidate extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {

  val ammissibilitaPath: String = "src/test/resources/isilonshare_gas"

  implicit lazy val sparkSession: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("local")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import sparkSession.implicits._

  def testRun(): Unit ={
    val testFlows = List(
      DummyStandardFlow(A01.toUpperCase)
       ,DummyStandardFlow(D01R.toUpperCase)
       ,DummyStandardFlow(FDD.toUpperCase)
       ,DummyStandardFlow(FUI.toUpperCase)
       ,DummyStandardFlow(RGL.toUpperCase)
      , DummyStandardFlow(RML.toUpperCase)
      , DummyStandardFlow(S40R.toUpperCase)
      , DummyStandardFlow(SM1.toUpperCase)
      , DummyStandardFlow(SWG1.toUpperCase)
      , DummyStandardFlow(TAL.toUpperCase)
      , DummyStandardFlow(TML.toUpperCase)
      , DummyStandardFlow(TMV.toUpperCase),
     DummyStandardFlow(RSL.toUpperCase)
    )

    val rootPathAmmissibilita = ammissibilitaPath
    deleteTxtReports(rootPathAmmissibilita)

    testFlows.foreach(stdFlow =>{
      //check configuration are loaded properly
      Assert.assertTrue(stdFlow.rootPath.equals("src/test/resources/tempRootPath"))

      val inputRdd = stdFlow.loadData
      Assert.assertFalse(inputRdd.isEmpty()) //assuming test folder is populated

      val (validationInfoRdd, validatedRdd) = stdFlow.validate(inputRdd)
      Assert.assertFalse(validationInfoRdd.isEmpty())

       val inputDf = stdFlow.parse(validatedRdd)
      println("parseFunction")
        println(inputDf.printSchema())
        println(inputDf.show(false))
      println("parseFunction")

      //      val inputDfRenamed = renameColumns(inputDf)
//      val inputDfWithCommon = addCommonColumns(inputDfRenamed, unzipTimestamp)
//      val fullDf = addNullColumns(inputDfWithCommon)
//      fullDf.persist(StorageLevel.MEMORY_AND_DISK)
//      write(fullDf)
//      writeReport(fullDf)

      val finalValidationInfoRdd = stdFlow.getReportMessagesNewLogic(validationInfoRdd).cache()
      stdFlow.writeAmmissibilitaReportsCsv(finalValidationInfoRdd)


      finalValidationInfoRdd.foreach(meta=>{
        val inputFile = new File(PropertyUtility.getUnzipInputPath + "/" + meta._1.originalRelativePath)
        val txtFile = new File(s"${rootPathAmmissibilita}/${new File(meta._1.originalRelativePath).getParent}/ReportEsitoPDR_${inputFile.getName.replace(".zip", "")}.txt")
        val existFlag = txtFile.exists
        if (meta._2.exists(esito => esito.ammissibilita.equals("N"))) { // report txt creato solo se c'è un pdr non ammissibile nella cartella di destinazione
          Assert.assertTrue(existFlag)
        } else {
          Assert.assertFalse(existFlag)
        }
      })

      finalValidationInfoRdd.flatMap({ case (gasXmlMetada, messages) =>
        messages.map(_.copy(flusso = gasXmlMetada.flusso, anno = gasXmlMetada.anno, mese = gasXmlMetada.mese, giorno = gasXmlMetada.giorno))
      }).toDF
        .withColumnRenamed("cartellaCloud", cartella_cloud)
        .withColumnRenamed("nomeFile", nome_file)
        .withColumnRenamed("codiceInamissibilita", codice_inamissibilita)
        .withColumn(d_caricamento, lit("unzipTimestamp"))
        .selectExpr(AmmissibilitaPDRSchema.getValues: _*)
        .show()
    })
  }

  def deleteTxtReports(rootPath:String):Unit = {
    FileUtils.deleteDirectory(new File(rootPath))
  }


  case class DummyStandardFlow(flow: String) extends StandardFlow{
    override val tS: TracciatoStandardEnum = null
    override val renamedColumns: Map[String, String] = null
    override val schema: SchemaEnum = null
    override val hiveTableName: String = null
    override val partitioningColumns: List[String] = null
    override def flowName: String = flow
    override val ammissPath: String = ammissibilitaPath

    override def parse(inputRdd: RDD[GasXmlMetadata]): DataFrame = {
      val dfRdd = inputRdd.flatMap(ParseUtility.parseXmlRettifica)
      val prova = Environment.getSpark.sqlContext.createDataFrame(dfRdd, RettificaSchema.createSparkSchema())

      prova
    }

    override def writeAmmissibilitaReportsHive(xmlWithMessages: RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])], unzipTimestamp:String): Unit = {}
  }
}

