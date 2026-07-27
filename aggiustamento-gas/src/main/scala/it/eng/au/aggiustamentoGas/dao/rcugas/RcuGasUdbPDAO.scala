package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasUdbPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

class RcuGasUdbPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasUdbPath
  override val columns: List[String] = List(
    RcuGasUdbPSchema.n_id_azienda,
    RcuGasUdbPSchema.n_id_udb
  )
}
