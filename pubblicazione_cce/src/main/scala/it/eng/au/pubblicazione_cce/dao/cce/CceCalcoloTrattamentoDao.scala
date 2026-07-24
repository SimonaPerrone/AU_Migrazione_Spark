package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloTrattamentoModel
import it.eng.au.pubblicazione_cce.schema.cce.CceCalcoloTrattamentoSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceCalcoloTrattamentoDao extends HiveDao[CceCalcoloTrattamentoModel] {
  override val tableName: String = Environment.getCceCalcoloTrattamentoTableName
  override val columns: List[String] = CceCalcoloTrattamentoSchema.getValues

}
