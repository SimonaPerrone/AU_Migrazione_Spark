package it.eng.au.scambioDatiGasivori.controller

import it.eng.au.scambioDatiGasivori.controller.traits.AggregatoreGasivori
import it.eng.au.scambioDatiGasivori.dao.GasivoriFilieraCcDao
import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraCcSchema
import it.eng.au.scambioDatiGasivori.schema.output.FilieraCcOutputSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

import scala.collection.immutable.ListMap

object CcAggregator extends AggregatoreGasivori {
  override val baseNumber: String = "1"
  override val destName: String = "CC"
  override val keyField: String = FilieraCcOutputSchema.CC_DEST
  override def inputDao: Dao = new GasivoriFilieraCcDao()
  override def getInputTableFiltering: String = Properties.getGasivoriFilieraCcExecutionId

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    GasivoriFilieraCcSchema.cc_dest.toString -> FilieraCcOutputSchema.CC_DEST.toString,
    GasivoriFilieraCcSchema.t_ragione_sociale_cliente.toString -> FilieraCcOutputSchema.RAGIONE_SOCIALE_CLIENTE.toString,
    GasivoriFilieraCcSchema.t_piva_cliente.toString -> FilieraCcOutputSchema.PIVA_CLIENTE.toString,
    GasivoriFilieraCcSchema.t_cf_cliente.toString -> FilieraCcOutputSchema.CF_CLIENTE.toString,
    GasivoriFilieraCcSchema.t_codice_pdr.toString -> FilieraCcOutputSchema.COD_PDR.toString,
    GasivoriFilieraCcSchema.t_classe_agevolazione.toString -> FilieraCcOutputSchema.CLASSE_AGEVOLAZIONE.toString,
    GasivoriFilieraCcSchema.d_data_inizio.toString -> FilieraCcOutputSchema.DATA_INIZIO.toString,
    GasivoriFilieraCcSchema.d_data_fine.toString -> FilieraCcOutputSchema.DATA_FINE.toString
  )
}
