package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.VRcugasDistributorePModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.VRcugasDistributorePSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class VRcugasDistributorePDao extends HiveDao[VRcugasDistributorePModel] {
  override val tableName: String = Environment.getProperty("hive.table.v_rcugas_distributore_p")
  override val schema: SchemaEnum = VRcugasDistributorePSchema

  def readAttivi(): Dataset[VRcugasDistributorePModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    read()
      .where(col(VRcugasDistributorePSchema.t_rag_soc).isNotNull)
      .where(col(VRcugasDistributorePSchema.d_data_fine).isNull)
      .selectExpr(this.columns: _*)
      .as[VRcugasDistributorePModel]
  }
}
