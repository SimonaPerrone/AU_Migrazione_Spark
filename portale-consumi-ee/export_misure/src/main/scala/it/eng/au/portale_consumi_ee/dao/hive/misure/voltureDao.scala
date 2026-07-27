package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{misureOrarieCModel, voltureModel}
import it.eng.au.portale_consumi_ee.schema.misure.{misureOrarieCSchema, voltureSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class voltureDao extends HiveMisureDao[voltureModel]{

  override val tableName: String = PropertyUtility.voltureTable
  override val schema: SchemaEnum = voltureSchema
}
