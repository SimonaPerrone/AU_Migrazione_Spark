package it.eng.au.sgsFlussoStoricoGas.controller.perimetro

import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.RcuGasVarTrattamentoPSchema
import it.eng.au.sgsFlussoStoricoGas.utility.constants.StatoPerimetro
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, lpad, when}
import org.apache.spark.sql.types.DateType

trait PerimetroControllerTrait {

  val tipoPratica = ""

  def getPerimetroS(perimetroSRaw: DataFrame, rcuVarTrattamento: DataFrame, annoEx: String, meseEx: String, giornoEx: String): DataFrame = {
    perimetroSRaw.join(rcuVarTrattamento, Seq(SgsPerimetroSchema.n_id_pdr.toString), "left")
      .withColumnRenamed(RcuGasVarTrattamentoPSchema.t_trattamento_settlement, SgsPerimetroSchema.t_trattamento)
      .withColumn(SgsPerimetroSchema.t_tipo_pratica, lit(tipoPratica))
      .withColumn(SgsPerimetroSchema.t_stato_perimetro,
        when(col(SgsPerimetroSchema.t_trattamento) === "Y", lit(StatoPerimetro.T_KO.toString))
          .when(col(SgsPerimetroSchema.t_trattamento).isNull, lit(StatoPerimetro.T_KO.toString))
          .otherwise(lit(StatoPerimetro.OK.toString))
      )
      .withColumn(SgsPerimetroSchema.d_data_decorrenza, col(SgsPerimetroSchema.d_data_decorrenza).cast(DateType))
      .withColumn(SgsPerimetroSchema.data_estrazione, col(SgsPerimetroSchema.data_estrazione).cast(DateType))
      .withColumn(SgsPerimetroSchema.anno_mese_calcolo_perimetro, concat(lit(annoEx), lpad(lit(meseEx), 2, "0")))
      .withColumn(SgsPerimetroSchema.giorno_calcolo_perimetro, lpad(lit(giornoEx), 2, "0"))
  }


  def getPerimetroA(perimetroARaw: DataFrame, rcuVarTrattamento: DataFrame, sgsPerimetroSLast: DataFrame, annoEx: String, meseEx: String, giornoEx: String): DataFrame = {
    sgsPerimetroSLast
      .withColumnRenamed(SgsPerimetroSchema.d_data_decorrenza, "d_data_decorrenza_s")
      .withColumnRenamed(SgsPerimetroSchema.data_estrazione, "data_estrazione_s")
      .withColumnRenamed(SgsPerimetroSchema.t_codice_pdr, "t_codice_pdr_s")
      .drop(SgsPerimetroSchema.t_trattamento)
      .withColumn("flag_storico", lit(true))
      .join(perimetroARaw.withColumn("flag_actual", lit(true))
        , Seq(SgsPerimetroSchema.n_id_pratica.toString, SgsPerimetroSchema.n_id_pdr.toString, SgsPerimetroSchema.piva_udb_entrante.toString)
        , "full_outer")
      .join(rcuVarTrattamento, Seq(SgsPerimetroSchema.n_id_pdr.toString), "left")
      .withColumn(SgsPerimetroSchema.d_data_decorrenza, when(col("flag_actual").isNull, col("d_data_decorrenza_s")).otherwise(col(SgsPerimetroSchema.d_data_decorrenza)))
      .withColumn(SgsPerimetroSchema.data_estrazione, when(col("flag_actual").isNull, col("data_estrazione_s")).otherwise(col(SgsPerimetroSchema.data_estrazione)))
      .withColumn(SgsPerimetroSchema.t_codice_pdr, when(col("flag_actual").isNull, col("t_codice_pdr_s")).otherwise(col(SgsPerimetroSchema.t_codice_pdr)))
      .withColumnRenamed(RcuGasVarTrattamentoPSchema.t_trattamento_settlement, SgsPerimetroSchema.t_trattamento)
      .withColumn(SgsPerimetroSchema.t_tipo_pratica, lit(tipoPratica))
      .withColumn(SgsPerimetroSchema.t_stato_perimetro, when(col("flag_actual").isNull || col("flag_storico").isNull, lit(StatoPerimetro.P_KO.toString)))
      .withColumn(
        SgsPerimetroSchema.t_stato_perimetro,
        when(
          (col(SgsPerimetroSchema.t_trattamento) === "Y" || col(SgsPerimetroSchema.t_trattamento).isNull)
            && col(SgsPerimetroSchema.t_stato_perimetro).isNull
          , lit(StatoPerimetro.T_KO.toString)
        )
          .when(
            col(SgsPerimetroSchema.t_trattamento).isin("G", "M")
              && col(SgsPerimetroSchema.t_stato_perimetro).isNull
            , lit(StatoPerimetro.OK.toString))
          .otherwise(col(SgsPerimetroSchema.t_stato_perimetro))
      )
      .withColumn(SgsPerimetroSchema.t_trattamento, when(col(SgsPerimetroSchema.t_stato_perimetro)===StatoPerimetro.P_KO.toString, lit(null)).otherwise(col(SgsPerimetroSchema.t_trattamento)))
      .withColumn(SgsPerimetroSchema.d_data_decorrenza, col(SgsPerimetroSchema.d_data_decorrenza).cast(DateType))
      .withColumn(SgsPerimetroSchema.data_estrazione, col(SgsPerimetroSchema.data_estrazione).cast(DateType))
      .withColumn(SgsPerimetroSchema.anno_mese_calcolo_perimetro, concat(lit(annoEx), lpad(lit(meseEx), 2, "0")))
      .withColumn(SgsPerimetroSchema.giorno_calcolo_perimetro, lpad(lit(giornoEx), 2, "0"))
  }

}
