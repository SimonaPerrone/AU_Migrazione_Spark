package it.eng.au.ccgPubblicazione.dao.agg

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionAggIncoerentiSchema
import it.eng.au.ccgPubblicazione.utility.Constants.CCG
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col


object DailyConsumptionAggIncoerentiDao extends SessionDao {
  override val tableName: String = Environment.getAggIncoerentiConsumptionTableName
  override val partitionField: String = DailyConsumptionAggIncoerentiSchema.executionid.toString
  override val fields: List[String] = DailyConsumptionAggIncoerentiSchema.getValues

  override def specificDfFilterPartition: Column = {
    col(DailyConsumptionAggIncoerentiSchema.session) === CCG
  }

  override def specificFilterPartition(listPartitions: List[String]): List[String] = {
    listPartitions
      .filter(value => value.contains(s"${DailyConsumptionAggIncoerentiSchema.session.toString}=$CCG"))
  }
}
