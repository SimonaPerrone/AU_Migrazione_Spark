package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasClientefinalePModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasClientefinalePSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, when}

class RcugasClientefinalePDao() extends HiveDao[RcugasClientefinalePModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_clientefinale_p")
  override val schema: SchemaEnum = RcugasClientefinalePSchema

  def readValidati(): Dataset[RcugasClientefinalePModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    val tmpColumn = "_tmpColumn"
    read()
      .where(col(RcugasClientefinalePSchema.t_codice_fiscale).isNotNull or col(RcugasClientefinalePSchema.t_partita_iva).isNotNull)
      .withColumn(tmpColumn, when(col(RcugasClientefinalePSchema.t_codice_fiscale).isNotNull, col(RcugasClientefinalePSchema.t_codice_fiscale))
        .otherwise(col(RcugasClientefinalePSchema.t_partita_iva)))
      .drop(RcugasClientefinalePSchema.t_codice_fiscale)
      .withColumnRenamed(tmpColumn, RcugasClientefinalePSchema.t_codice_fiscale)
      .selectExpr(columns: _*)
      .as[RcugasClientefinalePModel]
  }

}
