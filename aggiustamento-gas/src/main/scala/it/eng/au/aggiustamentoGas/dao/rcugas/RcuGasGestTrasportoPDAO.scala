package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasGestTrasportoPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

class RcuGasGestTrasportoPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasGestTrasportPath
  override val columns: List[String] = List(
    RcuGasGestTrasportoPSchema.n_id_it,
    RcuGasGestTrasportoPSchema.n_id_remi_anagrafica
  )
}
