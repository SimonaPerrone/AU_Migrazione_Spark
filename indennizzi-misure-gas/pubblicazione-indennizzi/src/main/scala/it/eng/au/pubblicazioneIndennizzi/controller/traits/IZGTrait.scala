package it.eng.au.pubblicazioneIndennizzi.controller.traits

import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import it.eng.au.pubblicazioneIndennizzi.dao.Dao
import it.eng.au.pubblicazioneIndennizzi.dao.cig.AggregatoTotaleDao
import it.eng.au.pubblicazioneIndennizzi.schema.IZGOutputSchema
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.immutable.ListMap

trait IZGTrait extends RunnableAggregator {
  override def daoTableName: Dao = new AggregatoTotaleDao()
  override val operationName: String = "IZG_AGGREGATOR"
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    AggregatoTotaleSchema.id_indennizzo.toString -> IZGOutputSchema.ID_INDENNIZZO.toString,
    AggregatoTotaleSchema.piva_distr.toString -> IZGOutputSchema.PIVA_ID.toString,
    AggregatoTotaleSchema.rag_soc_distr.toString -> IZGOutputSchema.RAG_SOC_ID.toString,
    AggregatoTotaleSchema.piva_udd.toString -> IZGOutputSchema.PIVA_UDD.toString,
    AggregatoTotaleSchema.rag_soc_udd.toString -> IZGOutputSchema.RAG_SOC_UDD.toString,
    AggregatoTotaleSchema.annomese.toString -> IZGOutputSchema.AAAAMM.toString,
    AggregatoTotaleSchema.pdr_g.toString -> IZGOutputSchema.PDR_G.toString,
    AggregatoTotaleSchema.pdr_g_om1.toString -> IZGOutputSchema.PDR_G_SETTIMO.toString,
    AggregatoTotaleSchema.pdr_g_om2.toString -> IZGOutputSchema.PDR_G_SETTIMO_100_PERCENT_SYMBOL_EFF.toString,
    AggregatoTotaleSchema.pdr_g_om3.toString -> IZGOutputSchema.PDR_G_SETTIMO_30_PERCENT_SYMBOL_EFF.toString,
    AggregatoTotaleSchema.achieved_percentage_om1.toString -> IZGOutputSchema._PERCENT_SYMBOL_OM1.toString,
    AggregatoTotaleSchema.achieved_percentage_om2.toString -> IZGOutputSchema._PERCENT_SYMBOL_OM2.toString,
    AggregatoTotaleSchema.achieved_percentage_om3.toString -> IZGOutputSchema._PERCENT_SYMBOL_OM3.toString,
    AggregatoTotaleSchema.pdr_target_om1.toString -> IZGOutputSchema.PdR_TARGET_OM1.toString,
    AggregatoTotaleSchema.pdr_target_om2.toString -> IZGOutputSchema.PdR_TARGET_OM2.toString,
    AggregatoTotaleSchema.pdr_target_om3.toString -> IZGOutputSchema.PdR_TARGET_OM3.toString,
    AggregatoTotaleSchema.delta_pdr_om1.toString -> IZGOutputSchema.DELTA_PdR_OM1.toString,
    AggregatoTotaleSchema.delta_pdr_om2.toString -> IZGOutputSchema.DELTA_PdR_OM2.toString,
    AggregatoTotaleSchema.delta_pdr_om3.toString -> IZGOutputSchema.DELTA_PdR_OM3.toString,
    AggregatoTotaleSchema.indennizzo_om1.toString -> IZGOutputSchema._EURO_SYMBOL_OM1_SII.toString,
    AggregatoTotaleSchema.indennizzo_om2.toString -> IZGOutputSchema._EURO_SYMBOL_OM2_SII.toString,
    AggregatoTotaleSchema.indennizzo_om3.toString -> IZGOutputSchema._EURO_SYMBOL_OM3_SII.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList

  override def getAggregato(df: DataFrame): DataFrame = {
    val orderedSelectList = aggregatoColumns.values.toList

    var aggDF = df.filter(
      ((col(AggregatoTotaleSchema.indennizzo_om1).isNotNull && col(AggregatoTotaleSchema.indennizzo_om1) > 0.0) ||
        (col(AggregatoTotaleSchema.indennizzo_om2).isNotNull && col(AggregatoTotaleSchema.indennizzo_om2) > 0.0) ||
        (col(AggregatoTotaleSchema.indennizzo_om3).isNotNull && col(AggregatoTotaleSchema.indennizzo_om3) > 0.0))
        && fileSpecificFilterExpression)
      .withColumn(AggregatoTotaleSchema.achieved_percentage_om1, round(col(AggregatoTotaleSchema.achieved_percentage_om1)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.achieved_percentage_om2, round(col(AggregatoTotaleSchema.achieved_percentage_om2)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.achieved_percentage_om3, round(col(AggregatoTotaleSchema.achieved_percentage_om3)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.pdr_target_om1, round(col(AggregatoTotaleSchema.pdr_target_om1)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.pdr_target_om2, round(col(AggregatoTotaleSchema.pdr_target_om2)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.pdr_target_om3, round(col(AggregatoTotaleSchema.pdr_target_om3)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.delta_pdr_om1, round(col(AggregatoTotaleSchema.delta_pdr_om1)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.delta_pdr_om2, round(col(AggregatoTotaleSchema.delta_pdr_om2)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.delta_pdr_om3, round(col(AggregatoTotaleSchema.delta_pdr_om3)).cast(IntegerType))
      .withColumn(AggregatoTotaleSchema.indennizzo_om1, round(col(AggregatoTotaleSchema.indennizzo_om1), 2).cast(DecimalType(11, 2)))
      .withColumn(AggregatoTotaleSchema.indennizzo_om2, round(col(AggregatoTotaleSchema.indennizzo_om2), 2).cast(DecimalType(11, 2)))
      .withColumn(AggregatoTotaleSchema.indennizzo_om3, round(col(AggregatoTotaleSchema.indennizzo_om3), 2).cast(DecimalType(11, 2)))

    aggregatoColumns.foreach({ case (tableName, fileName) =>
      aggDF = aggDF.withColumnRenamed(tableName, fileName)
    })

    aggDF.selectExpr(orderedSelectList: _*) // TODO: filters still to be implemented..
  }

  override def fileSpecificFilterExpression: Column = (col(AggregatoTotaleSchema.piva_distr).isNotNull
    and col(AggregatoTotaleSchema.piva_udd).isNotNull
    and col(AggregatoTotaleSchema.annomese).isNotNull
    and col(AggregatoTotaleSchema.piva_distr) =!= ""
    and col(AggregatoTotaleSchema.piva_udd) =!= ""
    and col(AggregatoTotaleSchema.annomese) =!= "")
}