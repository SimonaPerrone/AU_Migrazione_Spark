package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HiveDao
import it.eng.au.ERP.model.au.podConnessioneModel
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.podConnessioneSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class PodConnessioneDao extends HiveDao[podConnessioneModel]{

  override val tableName: String = PropertyUtility.podConnessioneTable
  override val schema: SchemaEnum = podConnessioneSchema

}
