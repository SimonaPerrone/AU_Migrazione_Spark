package it.eng.au.portale_consumi_ee.common.dao.mongodbs

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.model.mongodbs.fornitureElettricheModel
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.schema.mongodbs.fornitureElettricheSchema
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment

case class fornitureElettricheDao() extends HiveDao[fornitureElettricheModel] {

  override val tableName: String = Environment.getProperty("hive.table.mongodbs.forniture_elettriche")
  override val schema: SchemaEnum = fornitureElettricheSchema

}
