package it.eng.au.scambioDatiGasivori.controller

import it.eng.au.scambioDatiGasivori.controller.traits.AggregatoreGasivori
import it.eng.au.scambioDatiGasivori.dao.GasivoriFilieraUdbDao
import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraUdbSchema
import it.eng.au.scambioDatiGasivori.schema.output.FilieraUdbOutputSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

import scala.collection.immutable.ListMap

object UdbAggregator extends AggregatoreGasivori {
  override val baseNumber: String = "3"
  override val destName: String = "UDB"
  override val keyField: String = FilieraUdbOutputSchema.UDB_DEST
  override def inputDao: Dao = new GasivoriFilieraUdbDao()
  override def getInputTableFiltering: String = Properties.getGasivoriFilieraUdbExecutionId

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    GasivoriFilieraUdbSchema.udb_dest.toString -> FilieraUdbOutputSchema.UDB_DEST.toString,
    GasivoriFilieraUdbSchema.t_ragione_sociale_cliente.toString -> FilieraUdbOutputSchema.RAGIONE_SOCIALE_CLIENTE.toString,
    GasivoriFilieraUdbSchema.t_piva_cliente.toString -> FilieraUdbOutputSchema.PIVA_CLIENTE.toString,
    GasivoriFilieraUdbSchema.t_cf_cliente.toString -> FilieraUdbOutputSchema.CF_CLIENTE.toString,
    GasivoriFilieraUdbSchema.t_codice_pdr.toString -> FilieraUdbOutputSchema.COD_PDR.toString,
    GasivoriFilieraUdbSchema.t_classe_agevolazione.toString -> FilieraUdbOutputSchema.CLASSE_AGEVOLAZIONE.toString,
    GasivoriFilieraUdbSchema.d_data_inizio.toString -> FilieraUdbOutputSchema.DATA_INIZIO.toString,
    GasivoriFilieraUdbSchema.d_data_fine.toString -> FilieraUdbOutputSchema.DATA_FINE.toString
  )
}
