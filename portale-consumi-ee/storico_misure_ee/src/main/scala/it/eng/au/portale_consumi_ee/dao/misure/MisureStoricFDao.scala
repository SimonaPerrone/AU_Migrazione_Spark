package it.eng.au.portale_consumi_ee.dao.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.model.misure.{MisureStoricFModel, MisureStoricModel}
import it.eng.au.portale_consumi_ee.schema.misure.{MisureStoricFSchema, MisureStoricSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class MisureStoricFDao extends HiveDao[MisureStoricFModel]{
  override val tableName: String = PropertyUtility.misureStoricF
  override val schema: SchemaEnum = MisureStoricFSchema

}