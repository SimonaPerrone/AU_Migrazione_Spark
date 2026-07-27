package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuClienteFinalePModel}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuClienteFinalePSchema}

case class RcuClienteFinalePDao() extends HiveDao[RcuClienteFinalePModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_clientefinale_p")
  override val schema: SchemaEnum = RcuClienteFinalePSchema

}
