package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.aggregazioniMisureQuartorarieSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class AggregazioniMisureQuartorarieDao extends HDao{

  override val tableName: String = PropertyUtility.aggregazioniMisureQuartorarieTable
  override val schema: SchemaEnum = aggregazioniMisureQuartorarieSchema
  override val writeEnabled: Boolean = false

}
