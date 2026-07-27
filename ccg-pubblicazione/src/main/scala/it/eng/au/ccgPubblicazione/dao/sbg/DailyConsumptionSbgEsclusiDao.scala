package it.eng.au.ccgPubblicazione.dao.sbg

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionSbgEsclusiSchema
import it.eng.au.ccgPubblicazione.utility.Constants.CCG
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object DailyConsumptionSbgEsclusiDao extends SessionDao {
  override val tableName: String = Environment.getSbgEsclusiConsumptionTableName
  override val partitionField: String = DailyConsumptionSbgEsclusiSchema.executionid.toString
  override val fields: List[String] = DailyConsumptionSbgEsclusiSchema.getValues

  override def specificDfFilterPartition: Column = {
    col(DailyConsumptionSbgEsclusiSchema.session) === CCG
  }

  override def specificFilterPartition(listPartitions: List[String]): List[String] = {
    listPartitions
      .filter(value => value.contains(s"${DailyConsumptionSbgEsclusiSchema.session.toString}=$CCG"))
  }

}
