package it.eng.au.cceCalcolo.dao.annullamenti

import it.eng.au.cceCalcolo.schema.annullamenti.CceMoAnnullate1gEinSchema
import it.eng.au.cceCalcolo.utility.property.Properties

class CceMoAnnullate1gEinDAO extends CceMoAnnullateDAO {
  override val tablePath: String = Properties.getCceMoAnnullate1gEinTablePath
  override val tableName: String = Properties.getCceMoAnnullate1gEinTableName
  override val columns: List[String] = CceMoAnnullate1gEinSchema.getValues
}