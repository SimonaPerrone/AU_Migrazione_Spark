package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceEsitoModel
import it.eng.au.pubblicazione_cce.schema.cce.CceRichiestaPodSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceEsitoDao extends HiveDao[CceEsitoModel] {
  override val tableName: String = Environment.getCceEsitoTableName
  override val columns: List[String] = CceRichiestaPodSchema.getValues

}
