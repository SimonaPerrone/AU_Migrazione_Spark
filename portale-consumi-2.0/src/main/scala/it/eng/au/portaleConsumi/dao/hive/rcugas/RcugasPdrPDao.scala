package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasPdrPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasPdrPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment

class RcugasPdrPDao extends HiveDao[RcugasPdrPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_pdr_p")
  override val schema: SchemaEnum = RcugasPdrPSchema

}
