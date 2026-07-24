package it.eng.au.cceCalcolo.dao.annullamenti

import it.eng.au.cceCalcolo.schema.annullamenti.CceMoAnnullate1gSchema
import it.eng.au.cceCalcolo.utility.property.Properties

class CceMoAnnullate1gDAO extends CceMoAnnullateDAO {
  override val tablePath: String = Properties.getCceMoAnnullate1gTablePath
  override val tableName: String = Properties.getCceMoAnnullate1gTableName
  override val columns: List[String] = CceMoAnnullate1gSchema.getValues
}
