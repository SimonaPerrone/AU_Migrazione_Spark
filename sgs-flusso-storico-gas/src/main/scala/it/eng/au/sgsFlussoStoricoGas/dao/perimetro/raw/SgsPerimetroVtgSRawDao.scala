package it.eng.au.sgsFlussoStoricoGas.dao.perimetro.raw

import it.eng.au.sgsFlussoStoricoGas.dao.perimetro.PerimetroDao
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.raw.SgsPerimetroSRawSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType, StructField}

class SgsPerimetroVtgSRawDao extends PerimetroDao {
  override val tableName: String = Environment.getSgsDBName++"."++Environment.getPerimetroVtgSRawTableName
  override val tablePath: String = Environment.getPerimetroVtgSRawPath
  override val columns: List[String] = List(
    SgsPerimetroSRawSchema.n_id_pratica,
    SgsPerimetroSRawSchema.t_stato_pratica,
    SgsPerimetroSRawSchema.b_ammissibile,
    SgsPerimetroSRawSchema.t_stato,
    SgsPerimetroSRawSchema.d_data_decorrenza,
    SgsPerimetroSRawSchema.t_codice_pdr,
    SgsPerimetroSRawSchema.n_id_pdr,
    SgsPerimetroSRawSchema.piva_udd_entrante,
    SgsPerimetroSRawSchema.piva_udb_entrante,
    SgsPerimetroSRawSchema.piva_udb_uscente,
    SgsPerimetroSRawSchema.data_estrazione
  )

  private val perimetroSRawSchema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))

  override def readTable: DataFrame = {
    val df = Environment.getSpark.sqlContext.read.format("csv").option("sep", ";").option("nullValue", "\\N").schema(perimetroSRawSchema).load(tablePath)
      .selectExpr(columns: _*)

    df
  }
}
