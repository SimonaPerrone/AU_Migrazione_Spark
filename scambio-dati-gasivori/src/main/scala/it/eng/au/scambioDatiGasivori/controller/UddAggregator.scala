package it.eng.au.scambioDatiGasivori.controller

import it.eng.au.scambioDatiGasivori.controller.traits.AggregatoreGasivori
import it.eng.au.scambioDatiGasivori.dao.GasivoriFilieraUddDao
import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraUddSchema
import it.eng.au.scambioDatiGasivori.schema.output.FilieraUddOutputSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

import scala.collection.immutable.ListMap

object UddAggregator extends AggregatoreGasivori {
  override val baseNumber: String = "2"
  override val destName: String = "UDD"
  override val keyField: String = FilieraUddOutputSchema.UDD_DEST
  override def inputDao: Dao = new GasivoriFilieraUddDao()
  override def getInputTableFiltering: String = Properties.getGasivoriFilieraUddExecutionId

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    GasivoriFilieraUddSchema.udd_dest.toString -> FilieraUddOutputSchema.UDD_DEST.toString,
    GasivoriFilieraUddSchema.t_ragione_sociale_cliente.toString -> FilieraUddOutputSchema.RAGIONE_SOCIALE_CLIENTE.toString,
    GasivoriFilieraUddSchema.t_piva_cliente.toString -> FilieraUddOutputSchema.PIVA_CLIENTE.toString,
    GasivoriFilieraUddSchema.t_cf_cliente.toString -> FilieraUddOutputSchema.CF_CLIENTE.toString,
    GasivoriFilieraUddSchema.t_codice_pdr.toString -> FilieraUddOutputSchema.COD_PDR.toString,
    GasivoriFilieraUddSchema.t_classe_agevolazione.toString -> FilieraUddOutputSchema.CLASSE_AGEVOLAZIONE.toString,
    GasivoriFilieraUddSchema.d_data_inizio.toString -> FilieraUddOutputSchema.DATA_INIZIO.toString,
    GasivoriFilieraUddSchema.d_data_fine.toString -> FilieraUddOutputSchema.DATA_FINE.toString
  )
}
