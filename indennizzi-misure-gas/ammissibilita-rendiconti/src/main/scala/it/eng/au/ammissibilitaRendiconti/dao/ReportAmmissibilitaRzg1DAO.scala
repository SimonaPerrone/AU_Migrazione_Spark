package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaRendiconti.model.{AlreadyComputedZips, ReportAmmissibilitaRzg1}
import it.eng.au.ammissibilitaRendiconti.schema.ReportAmmissibilitaRzg1Schema
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.LongType

/**
 * Tabella di reportistica cig_report_ammissibilita_rzg1, utilizzata per le regole di "già processato" e "già trasmesso".
 *
 * Per scartare i file già trasmessi nei giorni precedenti, leggiamo la report_ammissibilita e scartiamo tutti i file che possiedono lo stesso file e stesso lastModified di record già presenti sulla tabella.
 *
 * Per l'alreadyTransmitted, leggiamo la report_ammissibilita entrando per annomese <= yearMonth e imponiamo il flag ai file che hanno lo stesso nome di un record ammissibile presente nella tabella.
 *
 * Esempi di applicazione:
 *  - se un file viene ri-trasmesso nello stesso anno/mese ma è diverso, allora deve essere riletto
 *  - se un file viene ri-trasmesso nello stesso anno/mese ed è uguale, allora deve essere scartato
 *  - se un file viene trasmesso in un anno/mese diverso ed è diverso, allora deve essere riletto
 *  - se un file viene trasmsso in un annom/mese diverso ma è uguale, allora deve essere scartato
 * */

class ReportAmmissibilitaRzg1DAO extends OutputDAO {
  override val tableName: String = Properties.getReportAmmissibilitaRzg1TableName
  override val columns: List[String] = ReportAmmissibilitaRzg1Schema.getValues
  override val partitionColumn: String = ReportAmmissibilitaRzg1Schema.annomese
  override val partitionByColumns: List[String] = List(ReportAmmissibilitaRzg1Schema.annomese.toString)
  override val parquetPath: String = Properties.getReportAmmissibilitaRzg1Path

  /** Lettura base della report_ammissibilita. Viene poi passata alle due funzioni [[getAlreadyComputedZips]] e [[getAlreadyTransmittedZips]] per estrarre i record
   * su cui verificare i controlli di alreadyComputed e alreadyTransmitted. */
  def getReportAmmissibilita(yearMonthMin: String, yearMonthMax: String): DataFrame = {
    readTable
      .where(
        (col(partitionColumn) >= yearMonthMin && col(partitionColumn) <= yearMonthMax) ||
          col(partitionColumn).isNull
      )
      .select(
        ReportAmmissibilitaRzg1Schema.cartella_cloud,
        ReportAmmissibilitaRzg1Schema.zip_file_name,
        ReportAmmissibilitaRzg1Schema.zip_last_modified_date
      )
  }

  /** Estrae i dati (percorso+nome file e ultima data di modifica) degli ZIP già processati in precedenza. */
  def getAlreadyComputedZips(reportAmmissibilita: DataFrame): RDD[AlreadyComputedZips] = {
    val partitions = Environment.spark.sparkContext.defaultParallelism

    reportAmmissibilita
      .coalesce(partitions)
      .repartition(partitions)
      .rdd
      .map(row =>
        AlreadyComputedZips(
          fileName = row.getAs[String](ReportAmmissibilitaRzg1Schema.cartella_cloud) + "/" + row.getAs[String](ReportAmmissibilitaRzg1Schema.zip_file_name),
          lastModifiedDate = row.getAs[Long](ReportAmmissibilitaRzg1Schema.zip_last_modified_date)
        ))
      .distinct
  }

  /** Estrae i dati (nome file) degli ZIP ammissibili già processati in precedenza. */
  def getAlreadyTransmittedZips(reportAmmissibilita: DataFrame): RDD[String] = {
    val partitions = Environment.spark.sparkContext.defaultParallelism

    reportAmmissibilita
      .where(col(ReportAmmissibilitaRzg1Schema.ammissibilita) === true)
      .select(ReportAmmissibilitaRzg1Schema.zip_file_name)
      .coalesce(partitions)
      .repartition(partitions)
      .rdd
      .map(_.getAs[String](ReportAmmissibilitaRzg1Schema.zip_file_name))
      .distinct
  }

  def getReportAmmissibilitaOutput(ammissibilita: RDD[ReportAmmissibilitaRzg1]): DataFrame = {
    Environment.spark.sqlContext.createDataFrame(ammissibilita)
      .withColumn(ReportAmmissibilitaRzg1Schema.executionid, lit(Environment.executionId).cast(LongType))
      .selectExpr(ReportAmmissibilitaRzg1Schema.getValues: _*)
  }
}
