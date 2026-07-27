package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.RcuTariffaPModel
import it.eng.au.portale_consumi_ee.schema.rcu.RcuTariffaPSchema

case class RcuTariffaPDao() extends HiveDao[RcuTariffaPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_tariffa_p")
  override val schema: SchemaEnum = RcuTariffaPSchema

}
