package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{misureNonOrarieCModel, misureOrarieCModel}
import it.eng.au.portale_consumi_ee.schema.misure.{misureNonOrarieCSchema, misureOrarieCSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class misureOrarieCDao extends HiveMisureDao[misureOrarieCModel]{

  override val tableName: String = PropertyUtility.misureOrarieCTable
  override val schema: SchemaEnum = misureOrarieCSchema
}
