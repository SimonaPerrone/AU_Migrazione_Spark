package it.eng.au.gse.calcoloAnnuale.dao

import it.eng.au.gse.calcoloAnnuale.schema.GseRichiestaSchema
import it.eng.au.gse.common.dao.Dao
import it.eng.au.gse.common.utility.Properties

class GseRichiesteAnnualiDao extends Dao {
  override val tableName: String = Properties.gseRichiesteAnnualiTableName
  override val columns: List[String] = GseRichiestaSchema.getValues
}
