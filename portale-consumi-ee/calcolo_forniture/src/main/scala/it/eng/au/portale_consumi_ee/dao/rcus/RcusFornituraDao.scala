package it.eng.au.portale_consumi_ee.dao.rcus

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcus.RcusFornituraPModel
import it.eng.au.portale_consumi_ee.schema.rcus.RcusFornituraPSchema

case class RcusFornituraDao() extends HiveDao[RcusFornituraPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcus.rcus_fornitura_p")
  override val schema: SchemaEnum = RcusFornituraPSchema

}
