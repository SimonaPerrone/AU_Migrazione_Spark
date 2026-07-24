package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.{CceEsitoModel, CceEsitoViewModel}
import it.eng.au.pubblicazione_cce.schema.cce.CceRichiestaPodSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceEsitoExportDao extends HiveDao[CceEsitoViewModel] {
  override val tableName: String = Environment.getCceEsitoExportTableName
  override val columns: List[String] = CceRichiestaPodSchema.getValues

}
