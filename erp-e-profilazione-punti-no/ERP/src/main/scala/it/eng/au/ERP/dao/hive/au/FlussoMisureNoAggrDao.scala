package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.{flussoMisureNoAggrSchema, flussoMisureSmisSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class FlussoMisureNoAggrDao extends HDao{

  override val tableName: String = PropertyUtility.auFlussoMisureNoAggr
  override val schema: SchemaEnum = flussoMisureNoAggrSchema
  override val writeEnabled: Boolean = false

}
