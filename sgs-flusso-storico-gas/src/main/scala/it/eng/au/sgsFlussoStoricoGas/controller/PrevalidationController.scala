package it.eng.au.sgsFlussoStoricoGas.controller

import it.eng.au.sgsFlussoStoricoGas.schema.aggregazione.AggregatoreInfoDettSchema
import it.eng.au.sgsFlussoStoricoGas.utility.constants.StatoAggregazione
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat, length, lit, trim, udf, when}
import org.apache.spark.sql.types.DoubleType

class PrevalidationController {

  def executePrevalidation(aggregatoreInfoDett: DataFrame): DataFrame = {

    // UDF per controllare se un anno è bisestile
    val isLeapYear: UserDefinedFunction = udf((year: String) => {
      val intYear = year.toInt
      (intYear % 4 == 0 && intYear % 100 != 0) || (intYear % 400 == 0)
    })

    val prelievoCols = AggregatoreInfoDettSchema.getValues.filter(v => "prelievo_\\d+".r.findFirstIn(v).isDefined)
    val prelievoNullCol = "null_concat"

    aggregatoreInfoDett
      .withColumn(
        "tutti_prelievi_null",
        when((1 to 31).map(i => col(s"prelievo_$i").isNull).reduce(_ && _), lit(true)).otherwise(lit(false))
      )
      .withColumn("t_stato_dett_copy", col(AggregatoreInfoDettSchema.t_stato_dett))
      .withColumn(AggregatoreInfoDettSchema.t_stato_dett,
        when(col(AggregatoreInfoDettSchema.trattamento).isNull, lit(StatoAggregazione.KO_T.toString))
          .when(
            length(col(AggregatoreInfoDettSchema.matr_mis)) > 20 ||
              !col(AggregatoreInfoDettSchema.data_inst_mis).rlike("^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[012])/(19|20)\\d\\d$") ||
              !col(AggregatoreInfoDettSchema.telegestione).isin("SI", "NO") ||
              !col(AggregatoreInfoDettSchema.tipo_mis).isin("01", "02") ||
              !trim(col(AggregatoreInfoDettSchema.coeff_corr)).rlike("^-?\\d+(\\.\\d+)?$") ||
              !col(AggregatoreInfoDettSchema.data_inst_conv).rlike("^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[012])/(19|20)\\d\\d$") ||
              length(col(AggregatoreInfoDettSchema.cod_remi)) > 14 ||
              !col(AggregatoreInfoDettSchema.id_reg_clim).rlike("^\\d{2}$") ||
              length(col(AggregatoreInfoDettSchema.cod_pdr)) =!= 14 ||
              !col(AggregatoreInfoDettSchema.classe_gruppo_mis).isin(
                "G1,6", "G2,5", "G4", "G6", "G10", "G16", "G25", "G40", "G65", "G100", "G160", "G250", "G400", "G650",
                "G1000", "G1600", "G2500", "G4000", "G6500") ||
              col(AggregatoreInfoDettSchema.classe_gruppo_mis).isNull
            , lit(StatoAggregazione.KO_F.toString))
          .when(col("tutti_prelievi_null"), lit(StatoAggregazione.KO_M.toString))
          .otherwise(lit(StatoAggregazione.OK.toString))
      )
      .withColumn(AggregatoreInfoDettSchema.t_stato_dett,
        when(col("t_stato_dett_copy")===StatoAggregazione.KO_A.toString, lit(StatoAggregazione.KO_A.toString))
          .otherwise(col(AggregatoreInfoDettSchema.t_stato_dett)))
      .withColumn(AggregatoreInfoDettSchema.coeff_corr, col(AggregatoreInfoDettSchema.coeff_corr).cast(DoubleType))

  }

}
