package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.{RcugasFornituraPModel, RcugasPdrMisuratorePModel}
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.{RcugasFornituraPSchema, RcugasPdrMisuratorePSchema}
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, column, to_date}

import java.sql.Timestamp

class RcugasPdrMisuratorePDao extends HiveDao[RcugasPdrMisuratorePModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_pdr_misuratore_p")
  override val schema: SchemaEnum = RcugasPdrMisuratorePSchema

  override def read(columns: List[String] = columns): Dataset[RcugasPdrMisuratorePModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    val tmpName = "tmp_data_inst_misuratore"
    spark.sqlContext.read
      .table(tableName)
      .withColumnRenamed(RcugasPdrMisuratorePSchema.t_data_inst_misuratore, tmpName)
      .withColumn(RcugasPdrMisuratorePSchema.t_data_inst_misuratore, to_date(col(tmpName), "dd/MM/yyyy"))
      .selectExpr(columns: _*)
      .as[RcugasPdrMisuratorePModel]
  }

  def readMisuratoriInstallati(): DataFrame = {
    read()
      .select(
        RcugasPdrMisuratorePSchema.n_id_pdr,
        RcugasPdrMisuratorePSchema.t_matricola_misuratore,
        RcugasPdrMisuratorePSchema.t_classe_misuratore,
        RcugasPdrMisuratorePSchema.n_coeff_correzione,
        RcugasPdrMisuratorePSchema.t_data_inst_misuratore
      )
      .distinct()
  }
}
