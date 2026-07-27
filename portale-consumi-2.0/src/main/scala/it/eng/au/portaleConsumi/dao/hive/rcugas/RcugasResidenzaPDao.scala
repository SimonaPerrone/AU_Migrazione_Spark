package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasResidenzaPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasResidenzaPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

class RcugasResidenzaPDao extends HiveDao[RcugasResidenzaPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_residenza_p")
  override val schema: SchemaEnum = RcugasResidenzaPSchema

  def readResidenzeForniture(): DataFrame = {
    val colName = "_rownumber"
    val window = Window.partitionBy(RcugasResidenzaPSchema.n_id_fornitura)
      .orderBy(
        col(RcugasResidenzaPSchema.d_aggiornamento).desc,
        col(RcugasResidenzaPSchema.d_data_fine).desc_nulls_first
      )
    read()
      .withColumn(colName, row_number() over window)
      .where(col(colName) === 1)
      .select(RcugasResidenzaPSchema.n_id_fornitura, RcugasResidenzaPSchema.t_residenza)
  }
}
