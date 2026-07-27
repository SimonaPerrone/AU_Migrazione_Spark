package it.eng.au.portaleConsumi.dao.hive.tdg

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.tdg.TdgVulnPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.tdg.TdgVulnPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class TdgVulnPDao extends HiveDao[TdgVulnPModel] {
  override val tableName: String = Environment.getProperty("hive.table.tdg_vuln_p")
  override val schema: SchemaEnum = TdgVulnPSchema

  def readAttivi(): Dataset[TdgVulnPModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    read()
      .where(col(TdgVulnPSchema.d_data_fine).isNull)
      .selectExpr(columns: _*)
      .as[TdgVulnPModel]
  }

}
