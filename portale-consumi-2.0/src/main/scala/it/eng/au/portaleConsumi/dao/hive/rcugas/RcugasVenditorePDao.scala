package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasVenditorePModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasVenditorePSchema
import it.eng.au.portaleConsumi.utility.environment.Environment

class RcugasVenditorePDao extends HiveDao[RcugasVenditorePModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_venditore_p")
  override val schema: SchemaEnum = RcugasVenditorePSchema

}
