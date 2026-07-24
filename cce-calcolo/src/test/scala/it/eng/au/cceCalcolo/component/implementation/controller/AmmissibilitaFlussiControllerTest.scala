package it.eng.au.cceCalcolo.component.implementation.controller

import it.eng.au.cceCalcolo.controller.AmmissibilitaCheckController
import it.eng.au.cceCalcolo.dao.ammissibilita.{ReportAmmissibilitaFileDAO, ReportAmmissibilitaPodDAO}
import it.eng.au.cceCalcolo.schema.ammissibilita.{ReportAmmissibilitaFileSchema, ReportAmmissibilitaPodSchema}
import it.eng.au.cceCalcolo.utility.EnvironmentSparkTest
import it.eng.au.cceCalcolo.utility.environment.Environment
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, concat, hash, upper}
import org.apache.spark.sql.types.{StringType, StructType}
import org.junit.Assert

class AmmissibilitaFlussiControllerTest extends EnvironmentSparkTest{

  val ammissibilitaCheckController = new AmmissibilitaCheckController

  val reportFileSchema = new StructType()
    .add(ReportAmmissibilitaFileSchema.nome_file.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.pod.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.flusso.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.codice_inamissibilita.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.anno.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.mese.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.giorno.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.annomese.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaFileSchema.annomesegiornodir.toString, StringType, nullable = true)

  val reportPodSchema = new StructType()
    .add(ReportAmmissibilitaPodSchema.nome_file.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.pod.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.flusso.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.codice_inamissibilita.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.anno.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.mese.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.giorno.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.annomese.toString, StringType, nullable = true)
    .add(ReportAmmissibilitaPodSchema.annomesegiornodir.toString, StringType, nullable = true)


  def testGetReportAmmissibilitaUnion(): Unit = {
    val reportFile = Environment.spark.createDataFrame(
      Environment.spark.sparkContext.parallelize(Seq(
        Row("20240101A1.XML", "", "PDO", "1", "2024", "01", "01", "202401", "20240101")
      )),
      reportFileSchema
    ).withColumn("Key", concat(upper(col(ReportAmmissibilitaPodSchema.nome_file)),col(ReportAmmissibilitaPodSchema.pod), col(ReportAmmissibilitaPodSchema.annomese).cast(StringType), col(ReportAmmissibilitaPodSchema.annomesegiornodir)))


    val reportPod = Environment.spark.createDataFrame(
      Environment.spark.sparkContext.parallelize(Seq(
        Row("20240101A2.XML", "A2A", "RFO", "1", "2024", "01", "01", "202401", "20240101")
      )),
      reportPodSchema
    ).withColumn("Key", concat(upper(col(ReportAmmissibilitaPodSchema.nome_file)),col(ReportAmmissibilitaPodSchema.pod), col(ReportAmmissibilitaPodSchema.annomese).cast(StringType), col(ReportAmmissibilitaPodSchema.annomesegiornodir)))

    val finalReport = ammissibilitaCheckController.getReportAmmissibilitaUnion(reportPod,reportFile)

    reportFile.show
    reportPod.show
    finalReport.show(20, false)

    Assert.assertTrue(true)
  }

}
