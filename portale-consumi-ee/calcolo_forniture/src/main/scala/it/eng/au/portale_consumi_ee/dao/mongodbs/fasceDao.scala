package it.eng.au.portale_consumi_ee.dao.mongodbs

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.{ fasceModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.{ fasceSchema}

case class fasceDao() extends HiveDao[fasceModel]{

  override val tableName: String = Environment.getProperty("hive.table.mongodbs.fasce")
  override val schema: SchemaEnum = fasceSchema

}
