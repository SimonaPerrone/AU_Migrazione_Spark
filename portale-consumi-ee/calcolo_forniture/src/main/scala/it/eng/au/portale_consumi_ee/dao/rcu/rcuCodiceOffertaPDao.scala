package it.eng.au.portale_consumi_ee.dao.rcu

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuClienteFinalePModel, rcuCodiceOffertaPModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.rcuCodiceOffertaPSchema
import it.eng.au.portale_consumi_ee.schema.rcu.RcuClienteFinalePSchema

case class rcuCodiceOffertaPDao() extends HiveDao[rcuCodiceOffertaPModel]{

  override val tableName: String = Environment.getProperty("hive.table.rcu.rcu_codice_offerta_p")
  override val schema: SchemaEnum = rcuCodiceOffertaPSchema

}
