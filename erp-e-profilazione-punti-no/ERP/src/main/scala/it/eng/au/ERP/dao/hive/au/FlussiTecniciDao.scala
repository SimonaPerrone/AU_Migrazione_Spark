package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.{flussiTeniciSchema, flussoMisureSmisSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class FlussiTecniciDao extends HDao{

  override val tableName: String = PropertyUtility.auFlussitecnici
  override val schema: SchemaEnum = flussiTeniciSchema
  override val writeEnabled: Boolean = false

}
