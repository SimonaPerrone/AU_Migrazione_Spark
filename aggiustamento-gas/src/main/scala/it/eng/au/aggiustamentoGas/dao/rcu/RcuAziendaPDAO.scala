package it.eng.au.aggiustamentoGas.dao.rcu

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcu.RcuAziendaPSchema
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasItPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

/** Tabella da rcu utilizzata per l'estrazione della piva udb */
class RcuAziendaPDAO extends Dao {
  override val parquetPath: String = Environment.getRcuAziendaPath
  override val columns: List[String] = List(
    RcuAziendaPSchema.n_id_azienda,
    RcuAziendaPSchema.t_piva
  )
}
