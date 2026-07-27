package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.controller.QKRIUDController._
import it.eng.au.ammissibilitaSettlementGas.model.rules.{QKRIUDCsvRule, QKRIUDRecordRule}
import it.eng.au.ammissibilitaSettlementGas.utility.EnvironmentSparkTest
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.junit.Assert._
import org.junit.Test

import java.io.File

class QKRIUDControllerTest extends EnvironmentSparkTest {
  /*
    val path = "src/test/resources/input" //TSG2_10238291008"

    def testReadAndCheckCsvFiles(): Unit = {
      val sc = Environment.sparkContext
      sc.setLogLevel("ERROR")

      Environment.setProperty("csv.input.path", path+"/TSG2/TSG2_10238291008/2025/08")
      Environment.setProperty("current.year", "2025")
      Environment.setProperty("current.month", "8")

      val QKRIUDMetadataRDD = QKRIUDController.readCsvFiles()

      QKRIUDMetadataRDD.foreach(x => println(x.file))

      val QKRIUDCsvRules: List[QKRIUDCsvRule] = List(
        ruleCheckQKRIUDPivaRdb, // 2
        ruleCheckQKRIUDFileExtension, //3
        ruleCheckQKRIUDFileIntegrity, //4
        ruleCheckQKRIUDFileHeader //6
      )

      val QKRIUDRecordRules: List[QKRIUDRecordRule] = List(
        ruleCheckQKRIUDDate, //7
        ruleCheckQKRIUDDateConsistence, //8
        ruleCheckQKRIUDCodRemiMandatory, //9
        ruleCheckQKRIUDfieldMandatory, //10
        ruleCheckQKRIUDDateFormat, //11
        ruleCheckQKRIUDDateExistance, //12
        ruleCheckQKRIUDCodRemiFormat, //13
        ruleCheckQKRIUDfieldFormat, //14
        ruleCheckCodRemiExistanceRcugas(Environment.spark.emptyDataFrame) //15
      )

      val checkedQKRIUDFiles = QKRIUDMetadataRDD.filter(QKRIUDMeta => QKRIUDMeta.isAmmissibile).map(QKRIUDMeta => QKRIUDController.checkQKRIUDCsv(QKRIUDMeta, QKRIUDCsvRules))

      val correctQKRIUDFiles = checkedQKRIUDFiles.filter(_.isAmmissibile)
      val incorrectQKRIUDFiles = checkedQKRIUDFiles.filter(!_.isAmmissibile) union QKRIUDMetadataRDD.filter(QKRIUDMeta => !QKRIUDMeta.isAmmissibile)
      println()
      println("CORRECT QKRIUD FILES")
      correctQKRIUDFiles.foreach(f => println(f.file.getName))
      println("INCORRECT QKRIUD FILES")
      //incorrectQKRIUDFiles.foreach(f => println(f.file.getName))
      incorrectQKRIUDFiles.foreach(f => println(f))
      println()

      val QKRIUDFilesAndRecords = correctQKRIUDFiles.map(QKRIUDMeta => QKRIUDMeta.copy(csv = QKRIUDController.getQKRIUDListFromFile(QKRIUDMeta.file)))
      val QKRIUDRecords = QKRIUDFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))

      val QKRIUDRecordsChecked = QKRIUDRecords.map(QKRIUDRec => QKRIUDController.checkQKRIUDRecord(QKRIUDRec, QKRIUDRecordRules))
      println(QKRIUDFilesAndRecords.first().csv.get(1))
      println()
      println(QKRIUDRecordsChecked.collect()(0))
      println(QKRIUDRecordsChecked.collect()(1))
      println(QKRIUDRecordsChecked.collect()(2))
      println(QKRIUDRecordsChecked.collect()(3))
      println(QKRIUDRecordsChecked.collect()(4))
      println(QKRIUDRecordsChecked.collect()(5))
      println(QKRIUDRecordsChecked.collect()(6))
    }*/
}

