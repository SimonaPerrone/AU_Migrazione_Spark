package it.eng.au.ccgPubblicazione.dao.sbg

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionAggSchema
import it.eng.au.ccgPubblicazione.utility.Constants.CCG
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/** Tabella dei consumi giornalieri SBG. */
object DailyConsumptionSbgDao extends SessionDao {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  override val tableName: String = Environment.getSbgConsumptionTableName
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
