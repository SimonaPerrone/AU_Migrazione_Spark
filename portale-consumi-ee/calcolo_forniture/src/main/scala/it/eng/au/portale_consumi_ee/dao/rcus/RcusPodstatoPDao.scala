package it.eng.au.portale_consumi_ee.dao.rcus

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcus.RcusPodstatoPModel
import it.eng.au.portale_consumi_ee.schema.rcus.RcusPodstatoPSchema

case class RcusPodstatoPDao() extends HiveDao[RcusPodstatoPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcus.rcus_podstato_p")
  override val schema: SchemaEnum = RcusPodstatoPSchema

}
