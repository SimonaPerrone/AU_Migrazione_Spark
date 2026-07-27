package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{ RcuPodMisurePModel}
import it.eng.au.portale_consumi_ee.schema.rcu.{ RcuPodMisurePSchema}

case class RcuPodMisurePDao()  extends HiveDao[RcuPodMisurePModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_pod_misure_p")
  override val schema: SchemaEnum = RcuPodMisurePSchema

}
