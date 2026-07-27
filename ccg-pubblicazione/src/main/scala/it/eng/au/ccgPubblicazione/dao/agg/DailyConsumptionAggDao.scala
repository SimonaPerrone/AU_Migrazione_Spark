package it.eng.au.ccgPubblicazione.dao.agg

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionAggSchema
import it.eng.au.ccgPubblicazione.utility.Constants.CCG
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/** Tabella dei consumi giornalieri AGG. */
object DailyConsumptionAggDao extends SessionDao {
  override val tableName: String = Environment.getAggConsumptionTableName
  override val partitionField: String = DailyConsumptionAggSchema.executionid.toString
  override val fields: List[String] = DailyConsumptionAggSchema.getValues

  override def specificDfFilterPartition: Column = {
    col(DailyConsumptionAggSchema.session) === CCG
  }

  override def specificFilterPartition(listPartitions: List[String]): List[String] = {
    listPartitions
      .filter(value => value.contains(s"${DailyConsumptionAggSchema.session.toString}=$CCG"))
  }
}
