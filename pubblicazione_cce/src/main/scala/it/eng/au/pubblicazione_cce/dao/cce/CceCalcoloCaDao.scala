package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloCaModel
import it.eng.au.pubblicazione_cce.schema.file.FileConsumiCaSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceCalcoloCaDao extends HiveDao[CceCalcoloCaModel] {
  override val tableName: String = Environment.getCceCalcoloCaTableName

  override def columns: List[String] = FileConsumiCaSchema.getValues
}
