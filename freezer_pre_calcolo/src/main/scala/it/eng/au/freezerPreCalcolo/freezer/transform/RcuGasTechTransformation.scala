package it.eng.au.freezerPreCalcolo.freezer.transform

import it.eng.au.freezerPreCalcolo.schema.{RcuGasMassivoFrozenSchema, RcuGasTechFrozenSchema, RcuGasVarConvertitoreSchema, RcuGasVarMisuratoreSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, coalesce, col, count, lit, row_number, when}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}


object RcuGasTechTransformation {

  /** Contains the operations needed to get final dataframe (RcugasTechFrozen) from processed input dataframes.
   *
   * @param rcuGasMassivoPdr RcugasMassivo "frozen" dataframe
   * @param rcuGasVarMisuratoreMis RcugasVarMisuratore processed dataframe
   * @param rcuGasVarConvertitoreConv RcugasVarConvertitore processed dataframe
   * @param freezeDateColumn column containing the freezing date
   * @param executionId current run timestamp
   * @return final dataframe RcugasTechFrozen
   */
  def transform(rcuGasMassivoPdr: DataFrame,
                rcuGasVarMisuratoreMis: DataFrame,
                rcuGasVarConvertitoreConv: DataFrame,
                freezeDateColumn: Column,
                executionId: String): DataFrame = {

    val rcuGasVar = rcuGasVarConvertitoreConv.withColumn(RcuGasTechFrozenSchema.t_pre_conv, lit("SI"))
    joinRcuGasMassivoAndVar(rcuGasMassivoPdr, rcuGasVarMisuratoreMis, rcuGasVar, freezeDateColumn, executionId)
  }

  /** Performs the join between [[rcuGasVarConvertitoreMis]] and [[rcuGasVarConvertitoreConv]]. */
    @deprecated
  def joinRcuGasVar(rcuGasVarMisuratoreMis: DataFrame, rcuGasVarConvertitoreConv: DataFrame): DataFrame = {
    rcuGasVarMisuratoreMis.join(broadcast(rcuGasVarConvertitoreConv),
      rcuGasVarMisuratoreMis(RcuGasVarMisuratoreSchema.n_id_pdr) === rcuGasVarConvertitoreConv(RcuGasVarConvertitoreSchema.n_id_pdr), "left_outer")
      .drop(rcuGasVarConvertitoreConv(RcuGasVarConvertitoreSchema.n_id_pdr))
  }

  /**
   * @param rcuGasMassivoPdr frozen RcugasMassivo dataframe
 */
  def joinRcuGasMassivoAndVar(
                               rcuGasMassivoPdr: DataFrame,
                               rcuGasVarMisuratore: DataFrame,
                               rcuGasVarConvertitore: DataFrame,
                               freezeDateColumn: Column,
                               executionId: String
                             ): DataFrame = {

    val joinCol = Seq(RcuGasMassivoFrozenSchema.n_id_pdr.toString)

    val joinMisConv =
      rcuGasVarMisuratore(RcuGasVarMisuratoreSchema.n_id_pdr) ===
        rcuGasVarConvertitore(RcuGasVarConvertitoreSchema.n_id_pdr)

    val infTs = lit("2900-01-01 00:00:00").cast(TimestampType)

    val convStart = col(RcuGasVarConvertitoreSchema.d_data_inizio).cast(TimestampType)
    val convEnd = coalesce(col(RcuGasVarConvertitoreSchema.d_data_fine).cast(TimestampType), infTs)

    val misStart = col(RcuGasTechFrozenSchema.data_inizio_tech).cast(TimestampType)
    val misEnd = coalesce(col(RcuGasTechFrozenSchema.data_fine_tech).cast(TimestampType), infTs)

    // overlap: [start,end) overlaps [start,end)
    val overlapExpr = convStart < misEnd && misStart < convEnd

    val varMisEConv = rcuGasVarMisuratore
      .join(rcuGasVarConvertitore, joinMisConv && overlapExpr, "left")
      .drop(rcuGasVarConvertitore(RcuGasVarConvertitoreSchema.n_id_pdr))
      .distinct

    // --- chiavi null-safe per group/join/window (NO NULL in join keys) ---
    val kMis = "k_t_matricola_mis"
    val kStart = "k_data_inizio_tech"
    val kEnd = "k_data_fine_tech"

    val varMisEConvKeyed = varMisEConv
      .withColumn(kMis, col(RcuGasTechFrozenSchema.t_matricola_misuratore))
      .withColumn(kStart, col(RcuGasTechFrozenSchema.data_inizio_tech).cast(TimestampType))
      .withColumn(kEnd, coalesce(col(RcuGasTechFrozenSchema.data_fine_tech).cast(TimestampType), infTs))

    val varMisEConvGrouped = varMisEConvKeyed
      .groupBy(col(kMis), col(kStart), col(kEnd))
      .agg(count(lit(1)).as("n_righe"))
      .withColumn("flag_multi", col("n_righe") > 1)
      .select(kMis, kStart, kEnd, "flag_multi")

    val varMisEConvWithFlag = varMisEConvKeyed
      .join(varMisEConvGrouped, Seq(kMis, kStart, kEnd), "left")
      .withColumn("flag_multi", coalesce(col("flag_multi"), lit(false)))

    val window = Window
      .partitionBy(col(kMis), col(kStart), col(kEnd), col("flag_multi"))
      .orderBy(col(RcuGasVarConvertitoreSchema.d_data_inizio).asc_nulls_last)

    val varMisEConvUpdated =
      varMisEConvWithFlag
        .withColumn("row_number", row_number().over(window))
        .withColumn(
          RcuGasTechFrozenSchema.data_inizio_tech,
          when(col("flag_multi") && col("row_number") > 1, col(RcuGasVarConvertitoreSchema.d_data_inizio))
            .otherwise(col(RcuGasTechFrozenSchema.data_inizio_tech))
        )
        .withColumn(
          RcuGasTechFrozenSchema.data_fine_tech,
          when(col("flag_multi"), col(RcuGasVarConvertitoreSchema.d_data_fine))
            .otherwise(col(RcuGasTechFrozenSchema.data_fine_tech))
        )
        .drop(kMis, kStart, kEnd)

    rcuGasMassivoPdr
      .join(varMisEConvUpdated, joinCol, "left")
      .withColumn(
        RcuGasTechFrozenSchema.t_pre_conv,
        when(col(RcuGasTechFrozenSchema.t_pre_conv).isNotNull, col(RcuGasTechFrozenSchema.t_pre_conv))
          .otherwise(lit("NO"))
      )
      .withColumn(RcuGasTechFrozenSchema.freeze_date, freezeDateColumn.cast(TimestampType))
      .withColumn(RcuGasTechFrozenSchema.execution_id, lit(executionId).cast(LongType))
  }

}
