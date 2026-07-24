package it.eng.au.gsvAggregatoreConsumi.dao.gsv

import it.eng.au.gsvAggregatoreConsumi.dao.Dao
import it.eng.au.gsvAggregatoreConsumi.schema.gsv.GsvConsRichiestaSchema
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class GsvConsRichiestaDAO extends Dao {
  override val tablePath: String = Environment.getGsvConsRichiestaPath
  override val tableName: String = Environment.getGsvConsRichiestaTable
  override val columns: List[String] = List(
    GsvConsRichiestaSchema.n_id_gsv5_cons_richiesta,
    GsvConsRichiestaSchema.t_tipo,
    GsvConsRichiestaSchema.t_anno,
    GsvConsRichiestaSchema.d_data_richiesta,
    GsvConsRichiestaSchema.t_stato,
    GsvConsRichiestaSchema.d_data_inserimento,
    GsvConsRichiestaSchema.d_data_modifica
  )
  private val gsvConsRichiestaSchema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))

  def get(): DataFrame = {
    val df = Environment.getSpark.sqlContext.read.format("csv").option("sep", ";").option("nullValue", "\\N").schema(gsvConsRichiestaSchema).load(tablePath)
      .filter(col(GsvConsRichiestaSchema.t_stato)==="N")

    df
  }

}
