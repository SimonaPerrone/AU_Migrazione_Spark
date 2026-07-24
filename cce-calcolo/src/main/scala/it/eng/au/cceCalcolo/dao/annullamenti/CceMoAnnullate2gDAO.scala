package it.eng.au.cceCalcolo.dao.annullamenti

import it.eng.au.cceCalcolo.schema.annullamenti.CceMoAnnullate2gSchema
import it.eng.au.cceCalcolo.utility.property.Properties

class CceMoAnnullate2gDAO extends CceMoAnnullateDAO {
  override val tablePath: String = Properties.getCceMoAnnullate2gTablePath
  override val tableName: String = Properties.getCceMoAnnullate2gTableName
  override val columns: List[String] = CceMoAnnullate2gSchema.getValues
}
