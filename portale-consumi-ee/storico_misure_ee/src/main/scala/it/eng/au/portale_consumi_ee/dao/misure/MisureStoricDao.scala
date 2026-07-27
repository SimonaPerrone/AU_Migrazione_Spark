package it.eng.au.portale_consumi_ee.dao.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.MisureStoricModel
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.schema.misure.MisureStoricSchema
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureElettricheTmpSchema
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class MisureStoricDao extends HiveMisureDao[MisureStoricModel]{
  override val tableName: String = PropertyUtility.misureStoric
  override val schema: SchemaEnum = MisureStoricSchema

}