package it.eng.au.portale_consumi_ee.dao.mongodbs

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.{GdmModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.{GdmSchema}

case class GdmDao() extends HiveDao[GdmModel]{

  override val tableName: String = Environment.getProperty("hive.table.mongodbs.gdm")
  override val schema: SchemaEnum = GdmSchema

}
