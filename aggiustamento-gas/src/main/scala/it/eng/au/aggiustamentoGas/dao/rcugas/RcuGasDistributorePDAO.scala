package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasDistributorePSchema, RcuGasItPSchema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment

/** Lega l'id del distributore alla rispettiva partita iva */
class RcuGasDistributorePDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasDistributorePath
  override val columns: List[String] = List(
    RcuGasDistributorePSchema.n_id_distributore,
    RcuGasDistributorePSchema.t_piva
  )
}
