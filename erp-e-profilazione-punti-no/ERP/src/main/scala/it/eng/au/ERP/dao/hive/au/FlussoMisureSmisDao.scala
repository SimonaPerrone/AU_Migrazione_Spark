package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.{aggregazioniMisureQuartorarieSchema, flussoMisureSmisSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class FlussoMisureSmisDao extends HDao{

  override val tableName: String = PropertyUtility.auFlussoMisureSmis
  override val schema: SchemaEnum = flussoMisureSmisSchema
  override val writeEnabled: Boolean = false

}
