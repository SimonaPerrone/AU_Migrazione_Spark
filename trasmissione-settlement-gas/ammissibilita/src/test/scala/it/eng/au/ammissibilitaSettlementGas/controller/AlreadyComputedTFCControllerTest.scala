package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.model.{AlreadyComputedFiles, TFCMetadata}
import it.eng.au.ammissibilitaSettlementGas.utility.Constants.PIVA_SNAM
import it.eng.au.ammissibilitaSettlementGas.utility.{EnvironmentSparkTest, Properties}
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.junit.Assert

import java.io.File

class AlreadyComputedTFCControllerTest extends EnvironmentSparkTest {

  def testFilterAlreadyComputedTFCFiles(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val TFCFilesWithMeta = sc.parallelize(Seq(
      TFCMetadata(file = new File("path/TFCfile3.csv"),
        tipoFile = "csv", lastModified = 123L, yearDir = "2023", monthDir = "03", pivaRdb = Some(PIVA_SNAM),
        annoMese = Some("202302"), progressivo = Some("1"), None, false, true),
      TFCMetadata(file = new File("path/TFCfile2.csv"),
        tipoFile = "csv", lastModified = 123L, yearDir = "2023", monthDir = "02", pivaRdb = Some(PIVA_SNAM),
        annoMese = Some("202301"), progressivo = Some("1"), None, false, true),
      TFCMetadata(file = new File("path/TFCfile1.csv"),
        tipoFile = "csv", lastModified = 123L, yearDir = "2023", monthDir = "03", pivaRdb = Some(PIVA_SNAM),
        annoMese = Some("202302"), progressivo = Some("1"), None, false, true)
    ))

    val alreadyComputedTFCFiles = sc.parallelize(Seq(
      AlreadyComputedFiles("path\\TFCfile1.csv", 123L),
      AlreadyComputedFiles("path\\TFCfile2.csv", 120L),
      AlreadyComputedFiles("path\\TFCfile2.csv", 126L),
      AlreadyComputedFiles("path\\TFCfile4.csv", 123L)
    ))

    val result = AlreadyComputedTFCController.filterAlreadyComputedTFCs(TFCFilesWithMeta, alreadyComputedTFCFiles)
    //result.collect.foreach(println)

    println(Properties.isRuleAlreadyTransmittedTFCEnabled)
    println(Properties.isRuleAlreadyTransmittedVPGEnabled)

    val alreadyTransmittedTFC = sc.parallelize(List("TFCfillkjbue3.csv", "TFCfile6.csv", "TFCfile1", "TFCfile2.csv"))
    alreadyTransmittedTFC.map(TFCFileName => (TFCFileName, "")).foreach(println)
    println()
    println()
    //AlreadyTransmittedTFCController.getAlreadyTransmitted(TFCFilesWithMeta, sc.parallelize(List("TFCfillkjbue3.csv", "TFCfile6.csv"))).
      //foreach(println)
    if (Properties.isRuleAlreadyTransmittedTFCEnabled) {
      val alreadyTransmittedTFCJoin = alreadyTransmittedTFC.map(TFCFileName => (TFCFileName, ""))
      TFCFilesWithMeta
        .keyBy(_.file.getName)
        .leftOuterJoin(alreadyTransmittedTFCJoin)
        .map({ case (tfcName, (tfcMetadata, zipFileName)) =>
          if (zipFileName.isDefined) {
            tfcMetadata.copy(isAlreadyTransmitted = true)
            println("dfgdc")
          } else tfcMetadata
        }).foreach(println)
      println("dsfg")
    }
    else println("Perché???")
    Assert.assertEquals(2,result.count())
    Assert.assertEquals(0,result.filter(tfcMeta => tfcMeta.file.getName == "fileTFC1.csv")count())
  }
}
