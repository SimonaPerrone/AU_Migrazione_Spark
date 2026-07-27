package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.autolettureModel
import it.eng.au.portale_consumi_ee.schema.misure.autolettureSchema
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class autolettureDao extends HiveMisureDao[autolettureModel]{

  override val tableName: String = PropertyUtility.autolettureTable
  override val schema: SchemaEnum = autolettureSchema
}
