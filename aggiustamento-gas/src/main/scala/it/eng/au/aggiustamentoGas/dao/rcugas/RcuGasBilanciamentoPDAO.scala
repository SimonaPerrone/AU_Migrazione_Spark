package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasBilanciamentoPSchema, RcuGasConnessioniDistr2Schema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment

/** Contiene le informazioni del responsabile del bilanciamento per un certo PdR */
class RcuGasBilanciamentoPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasBilanciamentoPath
  override val columns: List[String] = List(
    RcuGasBilanciamentoPSchema.n_id_udb,
    RcuGasBilanciamentoPSchema.n_id_pdr,
    RcuGasBilanciamentoPSchema.d_data_inizio,
    RcuGasBilanciamentoPSchema.d_data_fine
  )
}
