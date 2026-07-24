package it.eng.au.scambioDatiGasivori.controller

import it.eng.au.scambioDatiGasivori.controller.traits.AmmissibilitaGasivori
import it.eng.au.scambioDatiGasivori.dao.GasivoriPerimetroAmmDao
import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriPerimetroAmmSchema
import it.eng.au.scambioDatiGasivori.schema.output.PerimetroAmmOutputSchema

import scala.collection.immutable.ListMap

object AmmAggregator extends AmmissibilitaGasivori {
  override val baseNumber: String = "5"
  override val destName: String = "AMM"
  override val keyField: String = cseaDest
  override def inputDao: Dao = new GasivoriPerimetroAmmDao()

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    GasivoriPerimetroAmmSchema.n_id_gasivori_file.toString -> PerimetroAmmOutputSchema.N_ID_GASIVORI_FILE.toString,
    GasivoriPerimetroAmmSchema.t_nome_file_in.toString -> PerimetroAmmOutputSchema.T_NOME_FILE_IN,
    GasivoriPerimetroAmmSchema.piva_cliente.toString -> PerimetroAmmOutputSchema.PIVA_CLIENTE.toString,
    GasivoriPerimetroAmmSchema.cf_cliente.toString -> PerimetroAmmOutputSchema.CF_CLIENTE.toString,
    GasivoriPerimetroAmmSchema.prestazione.toString -> PerimetroAmmOutputSchema.PRESTAZIONE.toString,
    GasivoriPerimetroAmmSchema.classe_agevolazione.toString -> PerimetroAmmOutputSchema.CLASSE_AGEVOLAZIONE.toString,
    GasivoriPerimetroAmmSchema.data_inizio.toString -> PerimetroAmmOutputSchema.DATA_INIZIO.toString,
    GasivoriPerimetroAmmSchema.verifica_amm.toString -> PerimetroAmmOutputSchema.VERIFICA_AMM.toString,
    GasivoriPerimetroAmmSchema.cod_causale.toString -> PerimetroAmmOutputSchema.COD_CAUSALE.toString,
    GasivoriPerimetroAmmSchema.motivazione.toString -> PerimetroAmmOutputSchema.MOTIVAZIONE.toString,
    cseaDest -> PerimetroAmmOutputSchema.CSEA_DEST.toString
  )
}