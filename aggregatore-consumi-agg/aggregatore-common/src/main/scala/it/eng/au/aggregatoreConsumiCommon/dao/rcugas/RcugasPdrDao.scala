package it.eng.au.aggregatoreConsumiCommon.dao.rcugas

import it.eng.au.aggregatoreConsumiCommon.dao.Dao
import it.eng.au.aggregatoreConsumiCommon.schema.{RcugasPdrSchema, RcugasVarMisuratoreSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment

/** Data Access Object per la tabella su rcugas contenente la relazione t_codice_pdr <-> n_id_pdr */
class RcugasPdrDao extends Dao {
  override val tableName: String = Environment.getRcugasPdrTableName
  override val columns: List[String] = RcugasPdrSchema.getValues
}
