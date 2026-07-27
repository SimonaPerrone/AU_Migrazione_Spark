package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasConnessioniDistrPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasConnessioniDistrPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

class RcugasConnessioniDistrPDao() extends HiveDao[RcugasConnessioniDistrPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_connessioni_distr_p")
  override val schema: SchemaEnum = RcugasConnessioniDistrPSchema

  def readUltimoAggiornamento(): Dataset[RcugasConnessioniDistrPModel] = {
    val spark = Environment.getSpark
    import spark.implicits._
    val columnName = "_tmp"
    val window = Window.partitionBy(RcugasConnessioniDistrPSchema.n_id_pdr)
      .orderBy(col(RcugasConnessioniDistrPSchema.d_data_inizio_conn).desc)
    read()
      .withColumn(columnName, row_number() over window)
      .where(col(columnName) === 1)
      .selectExpr(this.columns: _*)
      .as[RcugasConnessioniDistrPModel]
  }
}
