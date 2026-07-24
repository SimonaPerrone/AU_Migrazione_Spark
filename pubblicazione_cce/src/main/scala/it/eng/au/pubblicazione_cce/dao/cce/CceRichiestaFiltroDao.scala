package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceRichiestaFiltroModel
import it.eng.au.pubblicazione_cce.schema.cce.CceRichiestaFiltroSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceRichiestaFiltroDao extends HiveDao[CceRichiestaFiltroModel] {
  override val tableName: String = Environment.getCceRichiestaFiltroTableName
  override val columns: List[String] = CceRichiestaFiltroSchema.getValues

}
