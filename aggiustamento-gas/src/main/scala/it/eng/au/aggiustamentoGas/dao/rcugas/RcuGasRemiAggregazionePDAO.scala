package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasRemiAggregazionePSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

class RcuGasRemiAggregazionePDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasRemiAggregazionePath
  override val columns: List[String] = List(
    RcuGasRemiAggregazionePSchema.n_id_remi_anagrafica_fisico,
    RcuGasRemiAggregazionePSchema.n_id_remi_anagrafica_pool,
    RcuGasRemiAggregazionePSchema.d_data_inizio,
    RcuGasRemiAggregazionePSchema.d_data_fine
  )
}
