package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasIndirizziPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasIndirizziPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment

class RcugasIndirizziPDao extends HiveDao[RcugasIndirizziPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_indirizzi_p")
  override val schema: SchemaEnum = RcugasIndirizziPSchema

}
