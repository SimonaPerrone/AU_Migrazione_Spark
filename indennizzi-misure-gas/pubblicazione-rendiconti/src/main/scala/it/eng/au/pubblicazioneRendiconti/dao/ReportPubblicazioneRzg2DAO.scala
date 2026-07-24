package it.eng.au.pubblicazioneRendiconti.dao

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneRendiconti.dao.`trait`.OutputDAO
import it.eng.au.pubblicazioneRendiconti.model.ReportPubblicazioneRzg2
import it.eng.au.pubblicazioneRendiconti.schema.ReportPubblicazioneRzg2Schema
import it.eng.au.pubblicazioneRendiconti.utility.properties.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class ReportPubblicazioneRzg2DAO extends OutputDAO {
  override val tableName: String = Properties.getReportPubblicazioneRzg2TableName
  override val parquetPath: String = Properties.getReportPubblicazioneRzg2Path
  override val columns: List[String] = ReportPubblicazioneRzg2Schema.getValues
  override val partitionColumn: String = ReportPubblicazioneRzg2Schema.executionid.toString
  override val partitionByColumns: List[String] = List(ReportPubblicazioneRzg2Schema.executionid.toString)

  def get(rdd: RDD[ReportPubblicazioneRzg2]): DataFrame = {
    Environment.spark.createDataFrame(rdd)
      .withColumn(ReportPubblicazioneRzg2Schema.executionid, lit(Environment.executionId))
      .selectExpr(columns: _*)
  }
}

