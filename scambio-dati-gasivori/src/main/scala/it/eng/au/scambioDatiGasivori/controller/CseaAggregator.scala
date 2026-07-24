package it.eng.au.scambioDatiGasivori.controller

import it.eng.au.scambioDatiGasivori.controller.traits.AggregatoreGasivori
import it.eng.au.scambioDatiGasivori.dao.GasivoriFilieraCseaDao
import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraCseaSchema
import it.eng.au.scambioDatiGasivori.schema.output.FilieraCseaOutputSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

import scala.collection.immutable.ListMap

object CseaAggregator extends AggregatoreGasivori {
  override val baseNumber: String = "5"
  override val destName: String = "CSEA"
  override val keyField: String = FilieraCseaOutputSchema.CSEA_DEST
  override def inputDao: Dao = new GasivoriFilieraCseaDao()
  override def getInputTableFiltering: String = Properties.getGasivoriFilieraCseaExecutionId

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    GasivoriFilieraCseaSchema.csea_dest.toString -> FilieraCseaOutputSchema.CSEA_DEST.toString,
    GasivoriFilieraCseaSchema.t_ragione_sociale_cliente.toString -> FilieraCseaOutputSchema.RAGIONE_SOCIALE_CLIENTE.toString,
    GasivoriFilieraCseaSchema.t_piva_cliente.toString -> FilieraCseaOutputSchema.PIVA_CLIENTE.toString,
    GasivoriFilieraCseaSchema.t_cf_cliente.toString -> FilieraCseaOutputSchema.CF_CLIENTE.toString,
    GasivoriFilieraCseaSchema.t_codice_pdr.toString -> FilieraCseaOutputSchema.COD_PDR.toString,
    GasivoriFilieraCseaSchema.t_classe_agevolazione.toString -> FilieraCseaOutputSchema.CLASSE_AGEVOLAZIONE.toString,
    GasivoriFilieraCseaSchema.d_data_inizio.toString -> FilieraCseaOutputSchema.DATA_INIZIO.toString,
    GasivoriFilieraCseaSchema.d_data_fine.toString -> FilieraCseaOutputSchema.DATA_FINE.toString
  )
}
