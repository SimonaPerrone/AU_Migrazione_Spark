package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuFasceMisuratore2gPModel}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuFasceMisuratore2gPSchema}

case class RcuFasceMisuratore2gPDao() extends HiveDao[RcuFasceMisuratore2gPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_fasce_misuratore_2g_p")
  override val schema: SchemaEnum = RcuFasceMisuratore2gPSchema

}
