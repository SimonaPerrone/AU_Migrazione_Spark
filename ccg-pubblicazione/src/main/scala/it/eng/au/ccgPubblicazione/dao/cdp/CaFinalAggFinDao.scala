package it.eng.au.ccgPubblicazione.dao.cdp

import it.eng.au.ccgPubblicazione.dao.SessionDao
import it.eng.au.ccgPubblicazione.schema.cdp.CaFinalCdpSchema
import it.eng.au.ccgPubblicazione.utility.Constants.{CCG, FIN}
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

/** Tabella dei consumi annuo per il processo CDP FIN. */
object CaFinalAggFinDao extends SessionDao {
  override val tableName: String = Environment.getCdpConsumptionTableName
  override val partitionField: String = CaFinalCdpSchema.executionid.toString
  override val fields: List[String] = CaFinalCdpSchema.getValues

  override def specificDfFilterPartition: Column = {
    col(CaFinalCdpSchema.tipo_trasmissione) === FIN && col(CaFinalCdpSchema.session) === CCG
  }

  override def specificFilterPartition(listPartitions: List[String]): List[String] = {
    listPartitions
      .filter(value => value.contains(s"${CaFinalCdpSchema.tipo_trasmissione.toString}=$FIN") && value.contains(s"${CaFinalCdpSchema.session.toString}=$CCG"))
  }
}
