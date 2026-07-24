package it.eng.au.gsvAggregatoreConsumi.dao.gsv

import it.eng.au.gsvAggregatoreConsumi.dao.Dao
import it.eng.au.gsvAggregatoreConsumi.schema.gsv.GsvConsFornitureSchema
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class GsvConsFornitureDAO extends Dao {
  override val tablePath: String = Environment.getGsvConsForniturePath
  override val tableName: String = Environment.getGsvConsFornitureTable
  override val columns: List[String] = List(
    GsvConsFornitureSchema.n_id_gsv5_cons_forniture,
    GsvConsFornitureSchema.n_id_gsv5_cons_richiesta,
    GsvConsFornitureSchema.n_id_pdr,
    GsvConsFornitureSchema.t_codice_pdr,
    GsvConsFornitureSchema.t_cod_tipo_pdr,
    GsvConsFornitureSchema.t_cf_cliente,
    GsvConsFornitureSchema.t_piva_cliente,
    GsvConsFornitureSchema.t_rag_soc_cliente,
    GsvConsFornitureSchema.n_id_cliente,
    GsvConsFornitureSchema.t_piva_dd,
    GsvConsFornitureSchema.t_anno,
    GsvConsFornitureSchema.d_data_inizio,
    GsvConsFornitureSchema.d_data_fine,
    GsvConsFornitureSchema.forn_continue,
    GsvConsFornitureSchema.t_execution_id,
    GsvConsFornitureSchema.d_data_inserimento
  )
  private val gsvConsFornitureSchema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))

  def get(): DataFrame = {
    val df = Environment.getSpark.sqlContext.read.format("csv").option("sep", ";").option("nullValue", "\\N").schema(gsvConsFornitureSchema).load(tablePath)
      .filter(col(GsvConsFornitureSchema.t_cod_tipo_pdr) =!= "0")
      .drop(GsvConsFornitureSchema.t_anno)

    df

  }

}
