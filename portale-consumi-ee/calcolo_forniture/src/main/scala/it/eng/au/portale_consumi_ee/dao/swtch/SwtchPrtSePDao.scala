package it.eng.au.portale_consumi_ee.dao.swtch

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.swtch.SwtchPrtSePModel
import it.eng.au.portale_consumi_ee.schema.swtch.SwtchPrtSePSchema

case class SwtchPrtSePDao() extends HiveDao[SwtchPrtSePModel]{

  override val tableName: String = Environment.getProperty("hive.table.swtch.prt_se_p")
  override val schema: SchemaEnum = SwtchPrtSePSchema

}
