package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.RcugasAziendaSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment

class RcuAziendaDao extends Dao {
  override val tableName: String = Environment.getRcuAziendaTableName
  override val columns: List[String] = RcugasAziendaSchema.getValues
}
