package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.flussoMisureInterconnessioneSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class flussoMisureInterconnessioneDao extends HDao{

  override val tableName: String = PropertyUtility.flussoMisureInterconnessioneTable
  override val schema: SchemaEnum = flussoMisureInterconnessioneSchema
  override val writeEnabled: Boolean = false

}
