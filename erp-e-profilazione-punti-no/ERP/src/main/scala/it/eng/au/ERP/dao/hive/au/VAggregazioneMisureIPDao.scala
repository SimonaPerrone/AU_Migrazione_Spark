package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.vAggreagazioneMisureIPSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class VAggregazioneMisureIPDao extends HDao{

  override val tableName: String = PropertyUtility.aggregazioneMisureIPTable
  override val schema: SchemaEnum = vAggreagazioneMisureIPSchema
  override val writeEnabled: Boolean = false

}
