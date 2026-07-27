package it.eng.au.portaleConsumi.dao.hive.rcu

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcu.VRcuAziendaPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcu.VRcuAziendaPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment

class VRcuAziendaPDao() extends HiveDao[VRcuAziendaPModel]{
  override val tableName: String = Environment.getProperty("hive.table.v_rcu_azienda_p")
  override val schema: SchemaEnum = VRcuAziendaPSchema

}
