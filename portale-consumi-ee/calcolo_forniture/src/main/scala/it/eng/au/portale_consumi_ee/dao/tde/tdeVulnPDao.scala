package it.eng.au.portale_consumi_ee.dao.tde

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.swtch.SwtchPrtSePModel
import it.eng.au.portale_consumi_ee.model.tde.tdeVulnPModel
import it.eng.au.portale_consumi_ee.schema.swtch.SwtchPrtSePSchema
import it.eng.au.portale_consumi_ee.schema.tde.tdeVulnPSchema

case class tdeVulnPDao() extends HiveDao[tdeVulnPModel]{

  override val tableName: String = Environment.getProperty("hive.table.tde.tde_vuln_p")
  override val schema: SchemaEnum = tdeVulnPSchema

}
