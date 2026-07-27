package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.misureMensiliCModel
import it.eng.au.portale_consumi_ee.schema.misure.misureMensiliCSchema
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class misureMensiliCDao extends HiveMisureDao[misureMensiliCModel]{

  override val tableName: String = PropertyUtility.misureMensiliCTable
  override val schema: SchemaEnum = misureMensiliCSchema
}
