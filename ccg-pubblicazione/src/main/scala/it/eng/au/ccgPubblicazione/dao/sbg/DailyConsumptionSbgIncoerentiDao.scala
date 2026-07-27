package it.eng.au.ccgPubblicazione.dao.sbg

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionSbgIncoerentiSchema
import it.eng.au.ccgPubblicazione.utility.Constants.CCG
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object DailyConsumptionSbgIncoerentiDao extends SessionDao {
  override val tableName: String = Environment.getSbgIncoerentiConsumptionTableName
  override val partitionField: String = DailyConsumptionSbgIncoerentiSchema.executionid.toString
  override val fields: List[String] = DailyConsumptionSbgIncoerentiSchema.getValues

  override def specificDfFilterPartition: Column = {
    col(DailyConsumptionSbgIncoerentiSchema.session) === CCG
  }

  override def specificFilterPartition(listPartitions: List[String]): List[String] = {
    listPartitions
      .filter(value => value.contains(s"${DailyConsumptionSbgIncoerentiSchema.session.toString}=$CCG"))
  }
}
