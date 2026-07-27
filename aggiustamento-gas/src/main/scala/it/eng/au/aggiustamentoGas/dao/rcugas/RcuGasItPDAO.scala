package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasItPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

class RcuGasItPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasGasltPath
  override val columns: List[String] = List(
    RcuGasItPSchema.n_id_it,
    RcuGasItPSchema.n_id_azienda
  )
}
