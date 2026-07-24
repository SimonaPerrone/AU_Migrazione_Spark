package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloAnagraficaModel
import it.eng.au.pubblicazione_cce.schema.cce.CceCalcoloAnagraficaSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import it.eng.au.pubblicazione_cce.utility.property.Properties

class CceCalcoloAnagraficaDao extends HiveDao[CceCalcoloAnagraficaModel] {
  override val tableName: String = Environment.getCceCalcoloAnagraficaTableName
  override val columns: List[String] = CceCalcoloAnagraficaSchema.getValues

}
