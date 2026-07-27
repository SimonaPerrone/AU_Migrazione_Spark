package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasPdrDatiprelievoPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasPdrDatiprelievoPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

class RcugasPdrDatiprelievoPDao extends HiveDao[RcugasPdrDatiprelievoPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_pdr_datiprelievo_p")
  override val schema: SchemaEnum = RcugasPdrDatiprelievoPSchema

  def readPdrCategoria(): DataFrame = {
    val colName = "_rownumber"
    val window = Window.partitionBy(RcugasPdrDatiprelievoPSchema.n_id_pdr)
      .orderBy(col(RcugasPdrDatiprelievoPSchema.t_anno).desc)
    read()
      .withColumn(colName, row_number() over window)
      .where(col(colName) === 1)
      .select(RcugasPdrDatiprelievoPSchema.n_id_pdr, RcugasPdrDatiprelievoPSchema.t_cod_cat_uso)
  }
}
