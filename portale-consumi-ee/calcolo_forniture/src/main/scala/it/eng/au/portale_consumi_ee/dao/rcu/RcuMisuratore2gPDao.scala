package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuFornituraPModel, RcuMisuratore2gPModel}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuFornituraPSchema, RcuMisuratore2gPSchema}

case class RcuMisuratore2gPDao()  extends HiveDao[RcuMisuratore2gPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_misuratore_2g_p")
  override val schema: SchemaEnum = RcuMisuratore2gPSchema

}
