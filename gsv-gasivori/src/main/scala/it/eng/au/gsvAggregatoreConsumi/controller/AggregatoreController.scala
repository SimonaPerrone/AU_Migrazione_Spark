package it.eng.au.gsvAggregatoreConsumi.controller

import it.eng.au.gsvAggregatoreConsumi.schema.agg.DailyConsumptionSchema
import it.eng.au.gsvAggregatoreConsumi.schema.gsv.{GsvConsAggrSchema, GsvConsFornitureSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array_contains, col, count, lit, split}

class AggregatoreController {

  private val aggregationColumns = List(
    col(GsvConsAggrSchema.n_id_gsv5_cons_aggr),
    col(GsvConsAggrSchema.n_id_gsv5_cons_richiesta),
    col(GsvConsAggrSchema.t_anno),
    col(GsvConsAggrSchema.t_ragione_sociale_cliente),
    col(GsvConsAggrSchema.t_cf_cliente),
    col(GsvConsAggrSchema.t_piva_cliente),
    col(GsvConsAggrSchema.t_codice_pdr),
    col(GsvConsAggrSchema.t_anno_mese),
    col(GsvConsAggrSchema.t_giorni_mese),
    col(GsvConsAggrSchema.t_piva_dd),
    col(GsvConsAggrSchema.t_stato)
  )

  def aggregationFunction(consumptionDF: DataFrame, fornitureDF: DataFrame): DataFrame = {

    val windowByPdrInMonth = Window.partitionBy(col(DailyConsumptionSchema.pdr), col(DailyConsumptionSchema.annoMese))
    val period = "period"

    consumptionDF
      .join(fornitureDF, col(DailyConsumptionSchema.pdr).equalTo(col(GsvConsFornitureSchema.t_codice_pdr)), "inner")
      .filter(col(DailyConsumptionSchema.date).between(col(GsvConsFornitureSchema.d_data_inizio), col(GsvConsFornitureSchema.d_data_fine)))
      .filter(array_contains(split(col(period), ","), col(DailyConsumptionSchema.annoMese).substr(0, 4)))
      .withColumn(GsvConsAggrSchema.t_giorni_mese, count("*").over(windowByPdrInMonth))
      .withColumn(GsvConsAggrSchema.n_id_gsv5_cons_aggr, lit(null))
      .withColumnRenamed(DailyConsumptionSchema.annoMese, GsvConsAggrSchema.t_anno_mese)
      .withColumnRenamed(GsvConsFornitureSchema.t_rag_soc_cliente, GsvConsAggrSchema.t_ragione_sociale_cliente)
      .groupBy(aggregationColumns:_*)
      .sum(DailyConsumptionSchema.value)
      .withColumnRenamed("sum(value)", GsvConsAggrSchema.n_consumo_mese)

  }

}
