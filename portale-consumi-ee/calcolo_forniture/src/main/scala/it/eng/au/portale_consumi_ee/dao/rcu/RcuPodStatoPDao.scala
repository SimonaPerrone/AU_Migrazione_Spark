package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.RcuPodStatoPModel
import it.eng.au.portale_consumi_ee.schema.rcu.RcuPodStatoPSchema

case class RcuPodStatoPDao() extends HiveDao[RcuPodStatoPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_pod_stato_p")
  override val schema: SchemaEnum = RcuPodStatoPSchema

}
