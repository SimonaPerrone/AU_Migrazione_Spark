package it.eng.au.sgsFlussoStoricoGas.dao.perimetro.raw

import it.eng.au.sgsFlussoStoricoGas.dao.perimetro.PerimetroDao
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.raw.SgsPerimetroARawSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType, StructField}

class SgsPerimetroUigARawDao extends PerimetroDao {
  override val tableName: String = Environment.getSgsDBName ++ "." ++ Environment.getPerimetroUigARawTableName
  override val tablePath: String = Environment.getPerimetroUigARawPath
  override val columns: List[String] = List(
    SgsPerimetroARawSchema.n_id_pratica,
    SgsPerimetroARawSchema.d_data_decorrenza,
    SgsPerimetroARawSchema.t_codice_pdr,
    SgsPerimetroARawSchema.n_id_pdr,
    SgsPerimetroARawSchema.piva_udb_entrante,
    SgsPerimetroARawSchema.data_estrazione
  )
  private val perimetroARawSchema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))

  override def readTable: DataFrame = {
    val df = Environment.getSpark.sqlContext.read.format("csv").option("sep", ";").option("nullValue", "\\N").schema(perimetroARawSchema).load(tablePath)
      .selectExpr(columns: _*)

    df
  }

}
