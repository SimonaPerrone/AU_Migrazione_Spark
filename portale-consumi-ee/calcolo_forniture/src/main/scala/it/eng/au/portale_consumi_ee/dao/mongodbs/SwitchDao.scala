package it.eng.au.portale_consumi_ee.dao.mongodbs

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.{ SwitchModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.{ SwitchSchema}

case class SwitchDao() extends HiveDao[SwitchModel]{

  override val tableName: String = Environment.getProperty("hive.table.mongodbs.switch")
  override val schema: SchemaEnum = SwitchSchema

}
