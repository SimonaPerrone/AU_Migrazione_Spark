package it.eng.au.scambioDatiGasivori.controller

import it.eng.au.scambioDatiGasivori.controller.traits.AggregatoreGasivori
import it.eng.au.scambioDatiGasivori.dao.GasivoriFilieraIdDao
import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraIdSchema
import it.eng.au.scambioDatiGasivori.schema.output.FilieraIdOutputSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

import scala.collection.immutable.ListMap

object IdAggregator extends AggregatoreGasivori {
  override val baseNumber: String = "4"
  override val destName: String = "ID"
  override val keyField: String = FilieraIdOutputSchema.ID_DEST
  override def inputDao: Dao = new GasivoriFilieraIdDao()
  override def getInputTableFiltering: String = Properties.getGasivoriFilieraIdExecutionId

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    GasivoriFilieraIdSchema.id_dest.toString -> FilieraIdOutputSchema.ID_DEST.toString,
    GasivoriFilieraIdSchema.t_ragione_sociale_cliente.toString -> FilieraIdOutputSchema.RAGIONE_SOCIALE_CLIENTE.toString,
    GasivoriFilieraIdSchema.t_piva_cliente.toString -> FilieraIdOutputSchema.PIVA_CLIENTE.toString,
    GasivoriFilieraIdSchema.t_cf_cliente.toString -> FilieraIdOutputSchema.CF_CLIENTE.toString,
    GasivoriFilieraIdSchema.t_codice_pdr.toString -> FilieraIdOutputSchema.COD_PDR.toString,
    GasivoriFilieraIdSchema.t_classe_agevolazione.toString -> FilieraIdOutputSchema.CLASSE_AGEVOLAZIONE.toString,
    GasivoriFilieraIdSchema.d_data_inizio.toString -> FilieraIdOutputSchema.DATA_INIZIO.toString,
    GasivoriFilieraIdSchema.d_data_fine.toString -> FilieraIdOutputSchema.DATA_FINE.toString
  )
}
