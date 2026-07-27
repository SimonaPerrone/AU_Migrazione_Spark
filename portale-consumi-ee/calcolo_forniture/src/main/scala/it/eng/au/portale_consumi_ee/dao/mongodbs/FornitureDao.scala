package it.eng.au.portale_consumi_ee.dao.mongodbs

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureModel
import it.eng.au.portale_consumi_ee.model.rcu.RcuAziendaPModel
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureSchema
import it.eng.au.portale_consumi_ee.schema.rcu.RcuAziendaPSchema

case class FornitureDao() extends HiveDao[FornitureModel]{

  override val tableName: String = Environment.getProperty("hive.table.mongodbs.forniture")
  override val schema: SchemaEnum = FornitureSchema

}
