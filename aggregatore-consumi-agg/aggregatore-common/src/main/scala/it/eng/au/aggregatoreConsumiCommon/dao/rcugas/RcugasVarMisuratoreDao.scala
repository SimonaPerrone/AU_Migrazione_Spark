package it.eng.au.aggregatoreConsumiCommon.dao.rcugas

import it.eng.au.aggregatoreConsumiCommon.dao.Dao
import it.eng.au.aggregatoreConsumiCommon.schema.RcugasVarMisuratoreSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment

/** Data Access Object per la tabella su rcugas contenente il coefficiente di correzione aggiornato */
class RcugasVarMisuratoreDao extends Dao {
  override val tableName: String = Environment.getRcugasVarMisuratoreTableName
  override val columns: List[String] = RcugasVarMisuratoreSchema.getValues
}
