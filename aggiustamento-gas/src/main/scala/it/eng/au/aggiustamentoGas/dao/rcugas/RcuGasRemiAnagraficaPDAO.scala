package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasRemiAnagraficaPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

class RcuGasRemiAnagraficaPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasRemiAnagraficaPath
  override val columns: List[String] = List(
    RcuGasRemiAnagraficaPSchema.t_remi,
    RcuGasRemiAnagraficaPSchema.n_id_remi_anagrafica
  )
}
