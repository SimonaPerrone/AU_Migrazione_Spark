package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloCaFlussiModel
import it.eng.au.pubblicazione_cce.schema.file.FileElencoFlussiCaSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceCalcoloCaFlussiDao extends HiveDao[CceCalcoloCaFlussiModel] {
  override val tableName: String = Environment.getCceCalcoloCaFlussiTableName

  override def columns: List[String] = FileElencoFlussiCaSchema.getValues

}
