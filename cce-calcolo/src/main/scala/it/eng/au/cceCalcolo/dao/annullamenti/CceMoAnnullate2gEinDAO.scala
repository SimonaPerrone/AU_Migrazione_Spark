package it.eng.au.cceCalcolo.dao.annullamenti

import it.eng.au.cceCalcolo.schema.annullamenti.CceMoAnnullate2gEinSchema
import it.eng.au.cceCalcolo.utility.property.Properties

class CceMoAnnullate2gEinDAO extends CceMoAnnullateDAO {
  override val tablePath: String = Properties.getCceMoAnnullate2gEinTablePath
  override val tableName: String = Properties.getCceMoAnnullate2gEinTableName
  override val columns: List[String] = CceMoAnnullate2gEinSchema.getValues
}