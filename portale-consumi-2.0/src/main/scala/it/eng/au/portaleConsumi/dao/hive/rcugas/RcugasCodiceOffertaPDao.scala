package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasCodiceOffertaPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasCodiceOffertaPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class RcugasCodiceOffertaPDao extends HiveDao[RcugasCodiceOffertaPModel] {

  override val tableName: String = Environment.getProperty("hive.table.rcugas_codice_offerta_p")
  override val schema: SchemaEnum = RcugasCodiceOffertaPSchema

  def readAttivi(): Dataset[RcugasCodiceOffertaPModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    read()
      .where(col(RcugasCodiceOffertaPSchema.d_data_fine).isNull)
      .selectExpr(columns: _*)
      .as[RcugasCodiceOffertaPModel]
  }
}
