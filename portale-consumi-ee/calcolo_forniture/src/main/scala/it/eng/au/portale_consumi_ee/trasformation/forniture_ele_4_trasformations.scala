package it.eng.au.portale_consumi_ee.trasformation

import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.common.utility.functions.{costants, sqlToSparkUtilitties}
import it.eng.au.portale_consumi_ee.model.mongodbs.{RcuPodDistrModel, fasceModel}
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuFasceMisuratore2gPModel, RcuMisuratore2gPModel, RcuPodDistrPModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.{RcuPodDistrSchema, fasceSchema}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuFasceMisuratore2gPSchema, RcuMisuratore2gPSchema, RcuPodDistrPSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Dataset, SparkSession}

// spark implementation of hql_forniture_ele_r_human_readble.sql
object forniture_ele_4_trasformations {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  val max_aggiornamento = "max_aggiornamento"
  val n_id_misuratore_numeric = "n_id_misuratore_numeric"
  val n_id_misuratore_2g_numeric = "n_id_misuratore_2g_numeric"
  val n_id_pod_numeric = "n_id_pod_numeric"
  val fasce = "fasce"

  // see hql_forniture_ele_4_human_readble.sql
  def calcolo_fasce(
                           dsRcuFasceMisuratore2gP : Dataset[RcuFasceMisuratore2gPModel],
                         dsRcuMisuratore2gP: Dataset[RcuMisuratore2gPModel]
                       ): Dataset[fasceModel] = {
    val fasce2 =
      dsRcuFasceMisuratore2gP
        .withColumn(n_id_misuratore_numeric,sqlToSparkUtilitties.isNumericUDF(col(RcuFasceMisuratore2gPSchema.n_id_misuratore)))
        .filter(col(n_id_misuratore_numeric) === true)
        .select(
          when(col(RcuFasceMisuratore2gPSchema.n_fascia_1) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fascia_1)).alias(RcuFasceMisuratore2gPSchema.n_fascia_1),
          when(col(RcuFasceMisuratore2gPSchema.n_fascia_2) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fascia_2)).alias(RcuFasceMisuratore2gPSchema.n_fascia_2),
          when(col(RcuFasceMisuratore2gPSchema.n_fascia_3) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fascia_3)).alias(RcuFasceMisuratore2gPSchema.n_fascia_3),
          when(col(RcuFasceMisuratore2gPSchema.n_fascia_4) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fascia_4)).alias(RcuFasceMisuratore2gPSchema.n_fascia_4),
          when(col(RcuFasceMisuratore2gPSchema.n_fascia_5) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fascia_5)).alias(RcuFasceMisuratore2gPSchema.n_fascia_5),
          when(col(RcuFasceMisuratore2gPSchema.n_fascia_6) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fascia_6)).alias(RcuFasceMisuratore2gPSchema.n_fascia_6),
          when(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_1) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_1)).alias(RcuFasceMisuratore2gPSchema.n_fine_fascia_1),
          when(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_2) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_2)).alias(RcuFasceMisuratore2gPSchema.n_fine_fascia_2),
          when(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_3) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_3)).alias(RcuFasceMisuratore2gPSchema.n_fine_fascia_3),
          when(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_4) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_4)).alias(RcuFasceMisuratore2gPSchema.n_fine_fascia_4),
          when(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_5) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_5)).alias(RcuFasceMisuratore2gPSchema.n_fine_fascia_5),
          when(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_6) === "", null).otherwise(col(RcuFasceMisuratore2gPSchema.n_fine_fascia_6)).alias(RcuFasceMisuratore2gPSchema.n_fine_fascia_6),
          col(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g),
          col(RcuFasceMisuratore2gPSchema.n_id_misuratore),
          col(RcuFasceMisuratore2gPSchema.d_aggiornamento),
          max(col(RcuFasceMisuratore2gPSchema.d_aggiornamento))
            .over(Window.partitionBy(RcuFasceMisuratore2gPSchema.n_id_misuratore, RcuFasceMisuratore2gPSchema.n_cod_giorno_2g))
            .alias(max_aggiornamento)
        )
        .filter(col(RcuFasceMisuratore2gPSchema.d_aggiornamento)===col(max_aggiornamento))
        .persist()

    val misuratore =   dsRcuMisuratore2gP
      .withColumn(n_id_misuratore_2g_numeric,sqlToSparkUtilitties.isNumericUDF(col(RcuMisuratore2gPSchema.n_id_misuratore_2g)))
      .withColumn(n_id_pod_numeric,sqlToSparkUtilitties.isNumericUDF(col(RcuMisuratore2gPSchema.n_id_pod)))
      .filter(col(n_id_misuratore_2g_numeric) === true && col(n_id_pod_numeric) === true)
      .select(
      col(RcuMisuratore2gPSchema.n_id_pod),
      col(RcuMisuratore2gPSchema.d_inizio_validita),
      col(RcuMisuratore2gPSchema.d_fine_validita),
      col(RcuMisuratore2gPSchema.d_data_iniziofreezing),
      col(RcuMisuratore2gPSchema.n_id_misuratore_2g)
    )
      .filter(col(RcuMisuratore2gPSchema.n_id_pod).isNotNull)
      .persist()

    val ttx = fasce2.as("fasce").join(misuratore.as("misure"),
      substring(misuratore(RcuMisuratore2gPSchema.n_id_misuratore_2g), 1, 18) === substring(fasce2(RcuFasceMisuratore2gPSchema.n_id_misuratore), 1, 18)
      ,"left")
      .select(
        misuratore(RcuMisuratore2gPSchema.n_id_pod),
        fasce2(RcuFasceMisuratore2gPSchema.n_id_misuratore),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "1", "1").otherwise("0").as(fasceSchema.f_lunedi),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "2", "1").otherwise("0").as(fasceSchema.f_martedi),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "3", "1").otherwise("0").as(fasceSchema.f_mercoledi),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "4", "1").otherwise("0").as(fasceSchema.f_giovedi),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "5", "1").otherwise("0").as(fasceSchema.f_venerdi),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "6", "1").otherwise("0").as(fasceSchema.f_sabato),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "7", "1").otherwise("0").as(fasceSchema.f_domenica),
        when(fasce2(RcuFasceMisuratore2gPSchema.n_cod_giorno_2g) === "8", "1").otherwise("0").as(fasceSchema.f_festivo),
        concat_ws(",",
          concat(coalesce(col("fasce." + RcuFasceMisuratore2gPSchema.n_fine_fascia_1), lit("")), coalesce(concat(lit("-"), col("fasce." + RcuFasceMisuratore2gPSchema.n_fascia_1)), lit(""))),
          concat(coalesce(col("fasce." + RcuFasceMisuratore2gPSchema.n_fine_fascia_2), lit("")), coalesce(concat(lit("-"), col("fasce." + RcuFasceMisuratore2gPSchema.n_fascia_2)), lit(""))),
          concat(coalesce(col("fasce." + RcuFasceMisuratore2gPSchema.n_fine_fascia_3), lit("")), coalesce(concat(lit("-"), col("fasce." + RcuFasceMisuratore2gPSchema.n_fascia_3)), lit(""))),
          concat(coalesce(col("fasce." + RcuFasceMisuratore2gPSchema.n_fine_fascia_4), lit("")), coalesce(concat(lit("-"), col("fasce." + RcuFasceMisuratore2gPSchema.n_fascia_4)), lit(""))),
          concat(coalesce(col("fasce." + RcuFasceMisuratore2gPSchema.n_fine_fascia_5), lit("")), coalesce(concat(lit("-"), col("fasce." + RcuFasceMisuratore2gPSchema.n_fascia_5)), lit(""))),
          concat(coalesce(col("fasce." + RcuFasceMisuratore2gPSchema.n_fine_fascia_6), lit("")), coalesce(concat(lit("-"), col("fasce." + RcuFasceMisuratore2gPSchema.n_fascia_6)), lit("")))
        ).as(fasce),
        misuratore(RcuMisuratore2gPSchema.d_inizio_validita),
        misuratore(RcuMisuratore2gPSchema.d_fine_validita),
        misuratore(RcuMisuratore2gPSchema.d_data_iniziofreezing)
      )

    fasce2.unpersist()
    misuratore.unpersist()

    val ffx = ttx.select(
      col(fasceSchema.n_id_pod),
      col(fasceSchema.n_id_misuratore),
      when(col(fasceSchema.f_lunedi) === "1", col(fasce)).otherwise("").as(fasceSchema.f_lunedi),
      when(col(fasceSchema.f_martedi) === "1", col(fasce)).otherwise("").as(fasceSchema.f_martedi),
      when(col(fasceSchema.f_mercoledi) === "1", col(fasce)).otherwise("").as(fasceSchema.f_mercoledi),
      when(col(fasceSchema.f_giovedi) === "1", col(fasce)).otherwise("").as(fasceSchema.f_giovedi),
      when(col(fasceSchema.f_venerdi) === "1", col(fasce)).otherwise("").as(fasceSchema.f_venerdi),
      when(col(fasceSchema.f_sabato) === "1", col(fasce)).otherwise("").as(fasceSchema.f_sabato),
      when(col(fasceSchema.f_domenica) === "1", col(fasce)).otherwise("").as(fasceSchema.f_domenica),
      when(col(fasceSchema.f_festivo) === "1", col(fasce)).otherwise("").as(fasceSchema.f_festivo),

      when(col(RcuMisuratore2gPSchema.d_inizio_validita).isNull || col(RcuMisuratore2gPSchema.d_inizio_validita) === "",
        concat(
          year(costants.dateMinus1126).cast(StringType),
          lpad(month(costants.dateMinus1126).cast(StringType), 2, "0"),
          lit("01")
        ).cast(LongType)
      ).otherwise(
        concat(
          substring(col(RcuMisuratore2gPSchema.d_inizio_validita), 1, 4),
          substring(col(RcuMisuratore2gPSchema.d_inizio_validita), 6, 2),
          substring(col(RcuMisuratore2gPSchema.d_inizio_validita), 9, 2)
        ).cast(LongType)
      ).as(RcuMisuratore2gPSchema.d_inizio_validita),

      when(col(RcuMisuratore2gPSchema.d_fine_validita).isNull || col(RcuMisuratore2gPSchema.d_fine_validita) === "",
        concat(
          year(costants.currentDate).cast(StringType),
          lpad(month(costants.currentDate).cast(StringType), 2, "0"),
          lpad(dayofmonth(costants.currentDate).cast(StringType), 2, "0")
        ).cast(LongType)
      ).otherwise(
        concat(
          substring(col(RcuMisuratore2gPSchema.d_fine_validita), 1, 4),
          substring(col(RcuMisuratore2gPSchema.d_fine_validita), 6, 2),
          substring(col(RcuMisuratore2gPSchema.d_fine_validita), 9, 2)
        ).cast(LongType)
      ).as(RcuMisuratore2gPSchema.d_fine_validita),

      when(col(RcuMisuratore2gPSchema.d_fine_validita).isNotNull && col(RcuMisuratore2gPSchema.d_fine_validita) =!= "",
        concat(
          substring(col(RcuMisuratore2gPSchema.d_fine_validita), 1, 4),
          substring(col(RcuMisuratore2gPSchema.d_fine_validita), 6, 2),
          substring(col(RcuMisuratore2gPSchema.d_fine_validita), 9, 2)
        )
      ).otherwise("").as(fasceSchema.d_fine_validita_str),

      when(col(RcuMisuratore2gPSchema.d_data_iniziofreezing).isNotNull && col(RcuMisuratore2gPSchema.d_data_iniziofreezing) =!= "",
        concat(
          substring(col(RcuMisuratore2gPSchema.d_data_iniziofreezing), 1, 4),
          substring(col(RcuMisuratore2gPSchema.d_data_iniziofreezing), 6, 2),
          substring(col(RcuMisuratore2gPSchema.d_data_iniziofreezing), 9, 2)
        )
      ).otherwise(col(RcuMisuratore2gPSchema.d_data_iniziofreezing)).as(RcuMisuratore2gPSchema.d_data_iniziofreezing)
    )

    val fasceDf = ffx
      .groupBy(fasceSchema.n_id_pod, fasceSchema.n_id_misuratore)
      .agg(
        max(fasceSchema.f_lunedi).as(fasceSchema.f_lunedi),
        max(fasceSchema.f_martedi).as(fasceSchema.f_martedi),
        max(fasceSchema.f_mercoledi).as(fasceSchema.f_mercoledi),
        max(fasceSchema.f_giovedi).as(fasceSchema.f_giovedi),
        max(fasceSchema.f_venerdi).as(fasceSchema.f_venerdi),
        max(fasceSchema.f_sabato).as(fasceSchema.f_sabato),
        max(fasceSchema.f_domenica).as(fasceSchema.f_domenica),
        max(fasceSchema.f_festivo).as(fasceSchema.f_festivo),
        max(fasceSchema.d_inizio_validita).as(fasceSchema.d_inizio_validita),
        max(fasceSchema.d_fine_validita).as(fasceSchema.d_fine_validita),
        max(fasceSchema.d_fine_validita_str).as(fasceSchema.d_fine_validita_str),
        max(fasceSchema.d_data_iniziofreezing).as(fasceSchema.d_data_iniziofreezing)
      ).selectExpr(fasceSchema.getValues:_*)
      .as[fasceModel]

    fasceDf
  }

}
