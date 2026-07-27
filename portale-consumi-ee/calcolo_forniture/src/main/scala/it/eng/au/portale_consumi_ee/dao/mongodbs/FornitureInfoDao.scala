package it.eng.au.portale_consumi_ee.dao.mongodbs

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.{FornitureInfoModel, FornitureModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.{FornitureInfoSchema, FornitureSchema}

case class FornitureInfoDao() extends HiveDao[FornitureInfoModel]{

  override val tableName: String = Environment.getProperty("hive.table.mongodbs.forniture_info")
  override val schema: SchemaEnum = FornitureInfoSchema

}
