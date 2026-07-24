package it.eng.au.gse.calcoloAnnuale.dao

import it.eng.au.gse.common.dao.Dao
import it.eng.au.gse.common.schema.dwh.DwhConsumiSchema
import it.eng.au.gse.common.utility.Properties

class DwhConsumiDao extends Dao {
  override val tableName: String = Properties.dwhConsumiAnnualeTableName
  override val columns: List[String] = DwhConsumiSchema.getValues
}
