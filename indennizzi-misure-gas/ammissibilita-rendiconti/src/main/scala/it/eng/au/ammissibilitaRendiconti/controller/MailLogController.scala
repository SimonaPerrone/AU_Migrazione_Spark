package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.dao.`trait`.DAO
import it.eng.au.ammissibilitaRendiconti.model.ZipRzg1Metadata
import it.eng.au.ammissibilitaRendiconti.schema.ReportAmmissibilitaRzg1Schema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.storage.StorageLevel

import java.io.File

/** Si occupa del check richiesto dall'ARU-12, ovvero controllo del processo di ammissibilità, e in caso di
 *  - errore nel processo, o
 *  - incongruenza tra il numero di RZG1 in input e numero di RZG1 processati, o
 *  - incongruenza tra il numero di RZG1 in input e numero di file RZG1_AMM creati,
 *
 *  invia una mail di avviso tramite Apache Nifi. */
object MailLogController extends Serializable {
  @transient lazy val mailLogger: Logger = Logger.getLogger("mail")
  val rzg1AmmFileColName = "rzg1_amm_file"

  def check(ZipFilesRdd: RDD[ZipRzg1Metadata], reportAmmissibilitaRendicontiDAO: DAO, yearMonthMin: String, yearMonthMax: String): Unit = {
    val outputDf = reportAmmissibilitaRendicontiDAO
      .readTable
      .where(col(ReportAmmissibilitaRzg1Schema.annomese) >= yearMonthMin)
      .where(col(ReportAmmissibilitaRzg1Schema.annomese) <= yearMonthMax)
      .where(col(ReportAmmissibilitaRzg1Schema.executionid) === lit(Environment.executionId))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val inputCount = ZipFilesRdd.count
    val outputCount = outputDf.count

    mailLogger.warn(s"Controllo uguaglianza numero di RZG1 in input e in output -> $inputCount RZG1 in input, $outputCount RZG1 in output")
    if (!inputCount.equals(outputCount)) throw new Exception("Numero di RZG1 in input diverso dal numero di RZG1 in output.")

    val rzg1AmmFilesCount = outputDf
      .withColumn(rzg1AmmFileColName, concat(col(ReportAmmissibilitaRzg1Schema.cartella_cloud_ammissibilita), col(ReportAmmissibilitaRzg1Schema.ammissibilita_file_name)))
      .select(rzg1AmmFileColName)
      .rdd
      .map(r => r.getAs[String](rzg1AmmFileColName))
      .filter(fileName => {
        val file = new File(fileName)
        file.exists()
      })
      .count

    mailLogger.warn(s"Controllo uguaglianza numero di RZG1 in input e numero di RZG1_AMM in output -> $inputCount RZG1 in input, $rzg1AmmFilesCount RZG1_AMM in output")
    if (!inputCount.equals(rzg1AmmFilesCount)) throw new Exception("Numero di RZG1 in input diverso dal numero di RZG1_AMM in output.")
  }
}
