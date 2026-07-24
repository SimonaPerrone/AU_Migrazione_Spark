package it.eng.au.gse.calcoloMensile.dao

import it.eng.au.gse.calcoloMensile.schema.gse.GseRichiestaSchema
import it.eng.au.gse.common.dao.Dao
import it.eng.au.gse.common.utility.Properties

class GseRichiesteMensiliDao extends Dao {
  override val tableName: String = Properties.gseRichiesteMensiliTableName
  override val columns: List[String] = GseRichiestaSchema.getValues
}
