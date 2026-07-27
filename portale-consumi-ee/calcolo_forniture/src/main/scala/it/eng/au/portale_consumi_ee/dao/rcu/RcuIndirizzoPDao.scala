package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuIndirizzoPModel}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuIndirizzoPSchema}

case class RcuIndirizzoPDao() extends HiveDao[RcuIndirizzoPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_indirizzo_p")
  override val schema: SchemaEnum = RcuIndirizzoPSchema

}
