package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.RcuAziendaPModel
import it.eng.au.portale_consumi_ee.schema.rcu.RcuAziendaPSchema

case class RcuAziendaPDao() extends HiveDao[RcuAziendaPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_azienda_p")
  override val schema: SchemaEnum = RcuAziendaPSchema

}
