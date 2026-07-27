package it.eng.au.ccgPubblicazione.dao.agg

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionAggEsclusiSchema
import it.eng.au.ccgPubblicazione.utility.Constants.CCG
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col


object DailyConsumptionAggEsclusiDao extends SessionDao {
  override val tableName: String = Environment.getAggEsclusiConsumptionTableName
  override val partitionField: String = DailyConsumptionAggEsclusiSchema.executionid.toString
  override val fields: List[String] = DailyConsumptionAggEsclusiSchema.getValues

  override def specificDfFilterPartition: Column = {
    col(DailyConsumptionAggEsclusiSchema.session) === CCG
  }

  override def specificFilterPartition(listPartitions: List[String]): List[String] = {
    listPartitions
      .filter(value => value.contains(s"${DailyConsumptionAggEsclusiSchema.session.toString}=$CCG"))
  }
}
