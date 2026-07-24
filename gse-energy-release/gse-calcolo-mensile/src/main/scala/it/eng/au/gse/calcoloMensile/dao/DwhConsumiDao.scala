package it.eng.au.gse.calcoloMensile.dao

import it.eng.au.gse.common.dao.Dao
import it.eng.au.gse.common.schema.dwh.DwhConsumiSchema
import it.eng.au.gse.common.utility.Properties

class DwhConsumiDao extends Dao {
  override val tableName: String = Properties.dwhConsumiMensileTableName
  override val columns: List[String] = DwhConsumiSchema.getValues
}
