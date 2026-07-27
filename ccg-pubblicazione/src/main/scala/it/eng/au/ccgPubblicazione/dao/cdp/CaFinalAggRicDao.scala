package it.eng.au.ccgPubblicazione.dao.cdp

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.cdp.CaFinalCdpSchema
import it.eng.au.ccgPubblicazione.utility.Constants.{RIC, CCG, FIN}
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/** Tabella dei consumi annui per il processo CDP RIC. */
object CaFinalAggRicDao extends SessionDao {
  override val tableName: String = Environment.getCdpConsumptionTableName
  override val partitionField: String = CaFinalCdpSchema.executionid.toString
  override val fields: List[String] = CaFinalCdpSchema.getValues

  override def specificDfFilterPartition: Column = {
    col(CaFinalCdpSchema.tipo_trasmissione) === RIC && col(CaFinalCdpSchema.session) === CCG
  }

  override def specificFilterPartition(listPartitions: List[String]): List[String] = {
    listPartitions
      .filter(value => value.contains(s"${CaFinalCdpSchema.tipo_trasmissione.toString}=$RIC") && value.contains(s"${CaFinalCdpSchema.session.toString}=$CCG"))
  }
}
