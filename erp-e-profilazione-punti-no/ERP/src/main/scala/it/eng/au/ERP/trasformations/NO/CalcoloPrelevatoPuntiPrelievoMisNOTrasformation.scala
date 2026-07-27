package it.eng.au.ERP.trasformations.NO

import it.eng.au.ERP.model.tratt_pod.TrattPodAnnomesePartitionedModel
import it.eng.au.ERP.schema.au.{flussiTeniciSchema, flussoMisureNoAggrSchema, flussoMisureSmisSchema}
import it.eng.au.ERP.schema.erp.erpValidatedMisNoSchema
import it.eng.au.ERP.schema.tratt_pod.trattPodAllAnnomesePartitionedSchema
import it.eng.au.ERP.utility.functions.Constants
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object CalcoloPrelevatoPuntiPrelievoMisNOTrasformation {

  val d_data_timestamp = "d_data_timestamp"

  val start_in_column = "start_in_column"
  val stop_in_column = "stop_in_column"
  val rank = "rank"
  val windowsSpecFlussiPeriodiciERettifica: WindowSpec = Window.partitionBy(flussoMisureNoAggrSchema.pod, flussoMisureNoAggrSchema.data_misura)
    .orderBy(col(flussoMisureNoAggrSchema.time_stamp).desc_nulls_last, col(flussoMisureNoAggrSchema.dataelaborazione).desc_nulls_last)

  val windowsSpecFlussiTecnici: WindowSpec = Window.partitionBy(flussiTeniciSchema.pod, flussiTeniciSchema.data_misura)
    .orderBy(col(flussiTeniciSchema.time_stamp).desc_nulls_last, col(flussiTeniciSchema.dataelaborazione).desc_nulls_last)

  val windowsSpecFlussiTecniciSmisSmontaggio: WindowSpec = Window.partitionBy(flussoMisureSmisSchema.pod, flussoMisureSmisSchema.data_misura_smn)
    .orderBy(col(flussoMisureSmisSchema.time_stamp).desc_nulls_last, col(flussoMisureSmisSchema.dataelaborazione).asc_nulls_last)

  val windowsSpecFlussiTecniciSmisMontaggio: WindowSpec = Window.partitionBy(flussoMisureSmisSchema.pod, flussoMisureSmisSchema.data_misura_mn)
    .orderBy(col(flussoMisureSmisSchema.time_stamp).desc_nulls_last, col(flussoMisureSmisSchema.dataelaborazione).asc_nulls_last)


  val anno_tratt = "anno_tratt"
  val mese_tratt = "mese_tratt"

  def prepareTrattPodForJoin(trattPodDf: Dataset[TrattPodAnnomesePartitionedModel]): DataFrame = {
    trattPodDf
      .withColumnRenamed(trattPodAllAnnomesePartitionedSchema.anno, anno_tratt)
      .withColumnRenamed(trattPodAllAnnomesePartitionedSchema.mese, mese_tratt)
      .withColumn(mese_tratt, lpad(col(mese_tratt).cast("string"), 2, "0"))
      .select(
        col(trattPodAllAnnomesePartitionedSchema.pod14),
        col(anno_tratt),
        col(mese_tratt),
        col(trattPodAllAnnomesePartitionedSchema.is_t_trattamento)
      )
  }

  // flusso periodico segmento sinistra (anno, mese -1) mese e anno correnti
  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciSpondaSinistraSegmentoFlusso1(
                                                                                            dfFlussoMisureNOAggrSpondaSinistra: DataFrame,
                                                                                            trattPodllAnnomesePartitioned: DataFrame,
                                                                                            start_in: String,
                                                                                            stop_in: String,
                                                                                            timestamp: Long
                                                                                          )
                                                                                          (implicit spark: SparkSession): DataFrame = {

    val df = dfFlussoMisureNOAggrSpondaSinistra
      .withColumn(start_in_column, lit(start_in))
      .withColumn(stop_in_column, lit(stop_in))
      .filter(col(flussoMisureNoAggrSchema.validato) === Constants.flussiPeriodiciMisuraValidataS)
      .filter(col(flussoMisureNoAggrSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE1)

    val dfAfterFilters = df
      .select(
        col(flussoMisureNoAggrSchema.pod).alias("pod"),
        col(flussoMisureNoAggrSchema.data_misura).alias("data_misura"),
        col("data_misura_yyyymmdd")
      )

    // df.select("start_in_column", "data_misura").show()  // Commentato per performance

    val dfSpondaSinistraSegmento_1 = df
      .filter(upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin(Constants.flussiPeriodiciTipoFlusso_1: _*))
      .filter(col("data_misura_yyyymmdd") === col(start_in_column))
      .withColumn("start_in_date_parsed", to_date(col(start_in_column), "yyyyMMdd"))
      .withColumn(
        erpValidatedMisNoSchema.data_misura,
        when(
          upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin("PNO", "PNO2G"),
          date_format(col("start_in_date_parsed"), "yyyy-MM-dd")
        ).otherwise(col(erpValidatedMisNoSchema.data_misura))
      )

    // Debug heavy operations removed (count/show)

    // dfSpondaSinistraSegmento_1.show()  // Commentato per performance

    val dfSpondaSinistraSegmentoLastTimeStamp = dfSpondaSinistraSegmento_1
      .withColumn(rank,
        row_number().over(windowsSpecFlussiPeriodiciERettifica).as(rank))
      .filter(col(rank) === 1)

      val dfFinal =
        dfSpondaSinistraSegmentoLastTimeStamp
          .withColumnRenamed(flussoMisureNoAggrSchema.perdita, erpValidatedMisNoSchema.coeff_perdita)
          .withColumnRenamed(flussoMisureNoAggrSchema.ka, erpValidatedMisNoSchema.k)
          .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_s, erpValidatedMisNoSchema.tipo_dato_s)
          .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_e, erpValidatedMisNoSchema.tipo_dato_e)
          // RIMOSSA: .withColumnRenamed(flussoMisureNoAggrSchema.data_misura, erpValidatedMisNoSchema.data_misura)
          .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))

      val parsedDataMisura = coalesce(
        to_date(col(erpValidatedMisNoSchema.data_misura), "yyyy-MM-dd"),
        to_date(col(erpValidatedMisNoSchema.data_misura), "dd/MM/yyyy")
      )

      val dfFinalWithAnnoMese = dfFinal
        .withColumn("data_misura_date", parsedDataMisura)
        .withColumn(erpValidatedMisNoSchema.data_misura, date_format(col("data_misura_date"), "yyyy-MM-dd"))
        .withColumn(flussoMisureNoAggrSchema.anno, year(col("data_misura_date")))
        .withColumn(flussoMisureNoAggrSchema.mese, lpad(month(col("data_misura_date")).cast("string"), 2, "0"))
        .withColumn("start_in_date_parsed", to_date(col(start_in_column), "yyyyMMdd"))
        .withColumn(
          "join_anno",
          col(flussoMisureNoAggrSchema.anno)  // anno è sempre quello da data_misura (2025)
        )
        .withColumn(
          "join_mese",
          // FIX: Sponda SINISTRA - PNO2G usa mese da start_in (agosto) per JOIN con tratt_pod
          when(
            upper(col(flussoMisureNoAggrSchema.tipo_flusso)) === lit("PNO2G"),
            lpad(month(col("start_in_date_parsed")).cast("string"), 2, "0")  // PNO2G: usa mese da start_in (es. '08')
          ).otherwise(
            lpad(col(flussoMisureNoAggrSchema.mese).cast("string"), 2, "0")  // PNO e altri: usa mese da data_misura
          )
        )
        .drop("data_misura_date", "start_in_date_parsed")

    val dfJoin = dfFinalWithAnnoMese.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussoMisureNoAggrSchema.pod) &&
        col(anno_tratt) === col("join_anno") &&
        col(mese_tratt).cast("int") === col("join_mese").cast("int")
      , "inner"

    )
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin
  }

  // flusso periodico segmento destra (anno, mese) corrente
  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciSpondaDestraSegmentoFlusso1(
                                                                                          dfFlussoMisureNOAggrSpondaDestra: DataFrame,
                                                                                          trattPodllAnnomesePartitioned: DataFrame,
                                                                                          start_in: String,
                                                                                          stop_in: String,
                                                                                          timestamp: Long
                                                                                        )
                                                                                        (implicit spark: SparkSession): DataFrame = {

    val df = dfFlussoMisureNOAggrSpondaDestra
      .withColumn(start_in_column, lit(start_in))
      .withColumn(stop_in_column, lit(stop_in))
      .withColumn("start_in_date_parsed", to_date(col(start_in_column), "yyyyMMdd"))
      .filter(col(flussoMisureNoAggrSchema.validato) === Constants.flussiPeriodiciMisuraValidataS)
      .filter(col(flussoMisureNoAggrSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE1)

    // Debug heavy operations removed (count/show)

    val dfSpondaDestraSegmento = df
      .filter(upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin(Constants.flussiPeriodiciTipoFlusso_1: _*))
      .filter(col("data_misura_yyyymmdd") === col(stop_in_column))
      .withColumn("stop_in_date_parsed", to_date(col(stop_in_column), "yyyyMMdd"))
      .withColumn(
        erpValidatedMisNoSchema.data_misura,
        when(
          upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin("PNO", "PNO2G"),
          date_format(col("stop_in_date_parsed"), "yyyy-MM-dd")  // FIX: PNO/PNO2G in sponda DESTRA usano stop_in
        ).otherwise(col(flussoMisureNoAggrSchema.data_misura))  // FIX: Usa nome colonna originale dalla tabella input
      )

    // Debug heavy operations removed (count/show)

    val dfSpondaDestraSegmentoLastTimeStamp = dfSpondaDestraSegmento
      .withColumn(rank,
        row_number().over(windowsSpecFlussiPeriodiciERettifica).as(rank))
      .filter(col(rank) === 1)

    val dfFinal =
      dfSpondaDestraSegmentoLastTimeStamp
        .withColumnRenamed(flussoMisureNoAggrSchema.perdita, erpValidatedMisNoSchema.coeff_perdita)
        .withColumnRenamed(flussoMisureNoAggrSchema.ka, erpValidatedMisNoSchema.k)
        .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_s, erpValidatedMisNoSchema.tipo_dato_s)
        .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_e, erpValidatedMisNoSchema.tipo_dato_e)
        // RIMOSSA: .withColumnRenamed(flussoMisureNoAggrSchema.data_misura, erpValidatedMisNoSchema.data_misura)
        // Motivo: sovrascriverebbe il valore corretto di data_misura impostato sopra per PNO2G
        .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))
        .withColumn(start_in_column, lit(start_in))
        .withColumn(stop_in_column, lit(stop_in))

    // Create anno and mese columns from data_misura (formato dd/MM/yyyy)
    val parsedDataMisura = coalesce(
      to_date(col(erpValidatedMisNoSchema.data_misura), "yyyy-MM-dd"),
      to_date(col(erpValidatedMisNoSchema.data_misura), "dd/MM/yyyy")
    )

    val dfFinalWithAnnoMese = dfFinal
      .withColumn("data_misura_date", parsedDataMisura)
      .withColumn(erpValidatedMisNoSchema.data_misura, date_format(col("data_misura_date"), "yyyy-MM-dd"))
      .withColumn(flussoMisureNoAggrSchema.anno, year(col("data_misura_date")))
      .withColumn(flussoMisureNoAggrSchema.mese, lpad(month(col("data_misura_date")).cast("string"), 2, "0"))
      .withColumn("start_in_date_parsed", to_date(col(start_in_column), "yyyyMMdd"))
      .withColumn("stop_in_date_parsed", to_date(col(stop_in_column), "yyyyMMdd"))
      .withColumn(
        "join_anno",
        when(
          upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin("PNO", "PNO2G"),
          year(col("stop_in_date_parsed"))  // SPONDA DESTRA: usa stop_in (anno corrente)
        ).otherwise(col(flussoMisureNoAggrSchema.anno))
      )
      .withColumn(
        "join_mese",
        when(
          upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin("PNO", "PNO2G"),
          lpad(month(col("stop_in_date_parsed")).cast("string"), 2, "0")  // SPONDA DESTRA: usa stop_in (mese corrente '09')
        ).otherwise(lpad(col(flussoMisureNoAggrSchema.mese).cast("string"), 2, "0"))  // FIX: lpad anche per altri flussi
      )
      .drop("data_misura_date", "start_in_date_parsed", "stop_in_date_parsed")

    val dfJoin = dfFinalWithAnnoMese.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussoMisureNoAggrSchema.pod) &&
      col(anno_tratt) === col("join_anno") &&
        col(mese_tratt).cast("int") === col("join_mese").cast("int")
      , "inner"

    )
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin
  }


  // flussi periodici anno, mese corrente e anno, mese -1
  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciFlusso2(
                                                                      dfFlussoMisureNOAggrSegmentoDestraESinistra: DataFrame,
                                                                      trattPodllAnnomesePartitioned: DataFrame,
                                                                      start_in: String,
                                                                      stop_in: String,
                                                                      timestamp: Long
                                                                    )
                                                                    (implicit spark: SparkSession): DataFrame = {

    val df = dfFlussoMisureNOAggrSegmentoDestraESinistra
      .withColumn(start_in_column, lit(start_in))
      .withColumn(stop_in_column, lit(stop_in))
      .filter(col(flussoMisureNoAggrSchema.validato) === Constants.flussiPeriodiciMisuraValidataS)
      .filter(col(flussoMisureNoAggrSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE1)

    val df_2 = df
      .filter(upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin(Constants.flussiPeriodiciTipoFlusso_2: _*))
      .filter(col("data_misura_yyyymmdd").between(col(start_in_column), col(stop_in_column)))

    val df_2LastTimeStamp = df_2
      .withColumn(rank,
        row_number().over(windowsSpecFlussiPeriodiciERettifica).as(rank))
      .filter(col(rank) === 1)

    val dfFinal =
      df_2LastTimeStamp
        .withColumnRenamed(flussoMisureNoAggrSchema.perdita, erpValidatedMisNoSchema.coeff_perdita)
        .withColumnRenamed(flussoMisureNoAggrSchema.ka, erpValidatedMisNoSchema.k)
        .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_s, erpValidatedMisNoSchema.tipo_dato_s)
        .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_e, erpValidatedMisNoSchema.tipo_dato_e)
        .withColumnRenamed(flussoMisureNoAggrSchema.data_misura, erpValidatedMisNoSchema.data_misura)
        .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))

    val parsedDataMisura = coalesce(
      to_date(col(erpValidatedMisNoSchema.data_misura), "yyyy-MM-dd"),
      to_date(col(erpValidatedMisNoSchema.data_misura), "dd/MM/yyyy")
    )

    val dfFinalWithAnnoMese = dfFinal
      .withColumn("data_misura_date", parsedDataMisura)
      .withColumn(erpValidatedMisNoSchema.data_misura, date_format(col("data_misura_date"), "yyyy-MM-dd"))
      .withColumn(flussoMisureNoAggrSchema.anno, year(col("data_misura_date")))
      .withColumn(flussoMisureNoAggrSchema.mese, lpad(month(col("data_misura_date")).cast("string"), 2, "0"))
      .drop("data_misura_date")

    val dfJoin = dfFinalWithAnnoMese.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussoMisureNoAggrSchema.pod) &&
        col(anno_tratt) === col(flussoMisureNoAggrSchema.anno) &&
        col(mese_tratt).cast("int") === col(flussoMisureNoAggrSchema.mese).cast("int")
      , "inner"

    )
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin
  }


  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiRettificaSegmentoSinistro(
                                                                               dfFlussoMisureNOAggrSegmentoSinistro: DataFrame,
                                                                               trattPodllAnnomesePartitioned: DataFrame,
                                                                               start_in: String,
                                                                               timestamp: Long
                                                                             )
                                                                             (implicit spark: SparkSession): DataFrame = {


    val df = dfFlussoMisureNOAggrSegmentoSinistro
      .withColumn(start_in_column, lit(start_in))
      .filter(col(flussoMisureNoAggrSchema.validato) === Constants.flussiPeriodiciMisuraValidataS)
      .filter(col(flussoMisureNoAggrSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE0 &&
        col(flussoMisureNoAggrSchema.tipodato_s) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoS0)
      .filter(upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin(Constants.flussiRettifica: _*))
      .filter(col(flussoMisureNoAggrSchema.motivazione) =!= "3")


    val dfSpondaSinistraSegmento = df
      .filter(upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin(Constants.flussiRettifica: _*))
      .filter(col(start_in_column) === col("data_misura_yyyymmdd"))

    val dfSpondaSinistraSegmentoLastTimeStamp = dfSpondaSinistraSegmento
      .withColumn(rank,
        row_number().over(windowsSpecFlussiPeriodiciERettifica).as(rank))
      .filter(col(rank) === 1)

    val dfFinal = dfSpondaSinistraSegmentoLastTimeStamp
      .withColumnRenamed(flussoMisureNoAggrSchema.perdita, erpValidatedMisNoSchema.coeff_perdita)
      .withColumnRenamed(flussoMisureNoAggrSchema.ka, erpValidatedMisNoSchema.k)
      .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_s, erpValidatedMisNoSchema.tipo_dato_s)
      .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_e, erpValidatedMisNoSchema.tipo_dato_e)
      .withColumnRenamed(flussoMisureNoAggrSchema.data_misura, erpValidatedMisNoSchema.data_misura)
      .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))

    // Create anno and mese columns from data_misura (formato dd/MM/yyyy)
    val parsedDataMisura = coalesce(
      to_date(col(erpValidatedMisNoSchema.data_misura), "yyyy-MM-dd"),
      to_date(col(erpValidatedMisNoSchema.data_misura), "dd/MM/yyyy")
    )

    val dfFinalWithAnnoMese = dfFinal
      .withColumn("data_misura_date", parsedDataMisura)
      .withColumn(erpValidatedMisNoSchema.data_misura, date_format(col("data_misura_date"), "yyyy-MM-dd"))
      .withColumn(flussoMisureNoAggrSchema.anno, year(col("data_misura_date")))
      .withColumn(flussoMisureNoAggrSchema.mese, lpad(month(col("data_misura_date")).cast("string"), 2, "0"))
      .drop("data_misura_date")

    val dfJoin = dfFinalWithAnnoMese.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussoMisureNoAggrSchema.pod) &&
        col(anno_tratt) === col(flussoMisureNoAggrSchema.anno) &&
        col(mese_tratt).cast("int") === col(flussoMisureNoAggrSchema.mese).cast("int")
      , "inner"

    )
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin
  }


  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiRettificaSegmentoDestro(
                                                                             dfFlussoMisureNOAggrSpondaDestra: DataFrame,
                                                                             trattPodllAnnomesePartitioned: DataFrame,
                                                                             stop_in: String,
                                                                             timestamp: Long
                                                                           )
                                                                           (implicit spark: SparkSession): DataFrame = {


    val df = dfFlussoMisureNOAggrSpondaDestra
      .withColumn(stop_in_column, lit(stop_in))
      .filter(col(flussoMisureNoAggrSchema.validato) === Constants.flussiPeriodiciMisuraValidataS)
      .filter(col(flussoMisureNoAggrSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE0 &&
        col(flussoMisureNoAggrSchema.tipodato_s) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoS0)
      .filter(upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin(Constants.flussiRettifica: _*))
      .filter(col(flussoMisureNoAggrSchema.motivazione) =!= "3")


    val dfSpondaDestraSegmento = df
      .filter(upper(col(flussoMisureNoAggrSchema.tipo_flusso)).isin(Constants.flussiRettifica: _*))
      .filter(col(stop_in_column) === col("data_misura_yyyymmdd"))

    val dfSpondaDestraSegmentoLastTimeStamp = dfSpondaDestraSegmento
      .withColumn(rank,
        row_number().over(windowsSpecFlussiPeriodiciERettifica).as(rank))
      .filter(col(rank) === 1)

    val dfFinal = dfSpondaDestraSegmentoLastTimeStamp
      .withColumnRenamed(flussoMisureNoAggrSchema.perdita, erpValidatedMisNoSchema.coeff_perdita)
      .withColumnRenamed(flussoMisureNoAggrSchema.ka, erpValidatedMisNoSchema.k)
      .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_s, erpValidatedMisNoSchema.tipo_dato_s)
      .withColumnRenamed(flussoMisureNoAggrSchema.tipodato_e, erpValidatedMisNoSchema.tipo_dato_e)
      .withColumnRenamed(flussoMisureNoAggrSchema.data_misura, erpValidatedMisNoSchema.data_misura)
      .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))

    // Create anno and mese columns from data_misura (formato dd/MM/yyyy)
    val parsedDataMisura = coalesce(
      to_date(col(erpValidatedMisNoSchema.data_misura), "yyyy-MM-dd"),
      to_date(col(erpValidatedMisNoSchema.data_misura), "dd/MM/yyyy")
    )

    val dfFinalWithAnnoMese = dfFinal
      .withColumn("data_misura_date", parsedDataMisura)
      .withColumn(erpValidatedMisNoSchema.data_misura, date_format(col("data_misura_date"), "yyyy-MM-dd"))
      .withColumn(flussoMisureNoAggrSchema.anno, year(col("data_misura_date")))
      .withColumn(flussoMisureNoAggrSchema.mese, lpad(month(col("data_misura_date")).cast("string"), 2, "0"))
      .drop("data_misura_date")

    val dfJoin = dfFinalWithAnnoMese.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussoMisureNoAggrSchema.pod) &&
        col(anno_tratt) === col(flussoMisureNoAggrSchema.anno) &&
        col(mese_tratt).cast("int") === col(flussoMisureNoAggrSchema.mese).cast("int")
      , "inner"

    )
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin
  }


  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiTecnici(
                                                             dfFlussiTecnici: DataFrame,
                                                             trattPodllAnnomesePartitioned: DataFrame,
                                                             start_in: String,
                                                             stop_in: String,
                                                             timestamp: Long
                                                           )
                                                           (implicit spark: SparkSession): DataFrame = {

    val df = dfFlussiTecnici
      .withColumn(start_in_column, lit(start_in))
      .withColumn(stop_in_column, lit(stop_in))
      .filter(col(flussiTeniciSchema.validato) === Constants.flussiPeriodiciMisuraValidataS)
      .filter(col(flussiTeniciSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE1)
      .filter(upper(col(flussiTeniciSchema.tipo_flusso)).isin(Constants.flussiTecnici: _*))


    val dfFinestraTemporale = df
      .filter(col("data_misura_yyyymmdd").between(col(start_in_column), col(stop_in_column)))

    val dfFinestraTemporaleLastTimeStamp = dfFinestraTemporale
      .withColumn(rank,
        row_number().over(windowsSpecFlussiTecnici).as(rank))
      .filter(col(rank) === 1)

    val dfFinal = dfFinestraTemporaleLastTimeStamp
      .withColumnRenamed(flussiTeniciSchema.perdita, erpValidatedMisNoSchema.coeff_perdita)
      .withColumnRenamed(flussiTeniciSchema.ka, erpValidatedMisNoSchema.k)
      .withColumnRenamed(flussiTeniciSchema.tipodato_s, erpValidatedMisNoSchema.tipo_dato_s)
      .withColumnRenamed(flussiTeniciSchema.tipodato_e, erpValidatedMisNoSchema.tipo_dato_e)
      .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))

    val dfJoin = dfFinal.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussiTeniciSchema.pod) &&
        col(anno_tratt) === col(flussiTeniciSchema.anno) &&
        col(mese_tratt) === col(flussiTeniciSchema.mese)  // 🔧 FIX: confronto STRING-to-STRING (entrambi hanno lpad)
      , "inner"

    )
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin

  }

  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiTecniciMisuraSmontaggio(
                                                                             dfFlussoMisureSmis: DataFrame,
                                                                             trattPodllAnnomesePartitioned: DataFrame,
                                                                             start_in: String,
                                                                             stop_in: String,
                                                                             timestamp: Long
                                                                           )
                                                                           (implicit spark: SparkSession): DataFrame = {

    val df = dfFlussoMisureSmis
      .withColumn(start_in_column, lit(start_in))
      .withColumn(stop_in_column, lit(stop_in))


    val dfFinestraTemporale = df
      .filter(col("data_misura_smn_yyyymmdd").between(col(start_in_column), col(stop_in_column)))

    val dfFinestraTemporaleLastTimeStamp = dfFinestraTemporale
      .withColumn(rank,
        row_number().over(windowsSpecFlussiTecniciSmisSmontaggio).as(rank))
      .filter(col(rank) === 1)

    val dfFinal = dfFinestraTemporaleLastTimeStamp
      .withColumn(erpValidatedMisNoSchema.anno, col(flussoMisureSmisSchema.anno_dtms).cast("string"))
      .withColumn(erpValidatedMisNoSchema.mese, lpad(col(flussoMisureSmisSchema.mese_dtms).cast("string"), 2, "0"))
      // Enermia Residuale Parziale: shift di +1 giorno per tutte le misure SMIS_SMN
      .withColumn("data_misura_smn_date", to_date(col(flussoMisureSmisSchema.data_misura_smn), "yyyy-MM-dd"))
      .withColumn("stop_in_date", to_date(col(stop_in_column), "yyyyMMdd"))
      .withColumn("data_misura_shifted_date", date_add(col("data_misura_smn_date"), 1))
      .withColumn(
        erpValidatedMisNoSchema.data_misura,
        date_format(col("data_misura_shifted_date"), "yyyy-MM-dd")
      )
      .withColumn(erpValidatedMisNoSchema.tipo_dato_s, when(col(flussoMisureSmisSchema.tipo_dato_smn) === Constants.tipo_dato_smnS, 1).otherwise(0))
      .withColumn(erpValidatedMisNoSchema.tipo_dato_e, when(col(flussoMisureSmisSchema.tipo_dato_smn) === Constants.tipo_dato_smnE, 1).otherwise(0))
      .withColumnRenamed(flussoMisureSmisSchema.eam_smn, erpValidatedMisNoSchema.eam)
      .withColumnRenamed(flussoMisureSmisSchema.eaf1_smn, erpValidatedMisNoSchema.eaf1)
      .withColumnRenamed(flussoMisureSmisSchema.eaf2_smn, erpValidatedMisNoSchema.eaf2)
      .withColumnRenamed(flussoMisureSmisSchema.eaf3_smn, erpValidatedMisNoSchema.eaf3)
      .withColumn(erpValidatedMisNoSchema.tipo_flusso, lit(Constants.tipo_flussoA))
      .withColumnRenamed(flussoMisureSmisSchema.perditatens_mn, erpValidatedMisNoSchema.coeff_perdita)
      .withColumnRenamed(flussoMisureSmisSchema.ka_mn, erpValidatedMisNoSchema.k)
      .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))
      .filter(col("data_misura_shifted_date") <= col("stop_in_date"))
      .drop("data_misura_smn_date", "data_misura_shifted_date", "stop_in_date")

    val dfJoin = dfFinal.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussoMisureSmisSchema.pod) &&
        col(anno_tratt) === col(erpValidatedMisNoSchema.anno) &&
        col(mese_tratt) === col(erpValidatedMisNoSchema.mese)  // 🔧 FIX SMIS-SMONTAGGIO: confronto STRING-to-STRING
      , "inner"

    )
      .withColumnRenamed(trattPodAllAnnomesePartitionedSchema.is_t_trattamento.toString, erpValidatedMisNoSchema.trattamento.toString)
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin

  }

  def calcoloPrelevatoPuntiPrelievoNOIngestionFlussiTecniciMisuraMontaggio(
                                                                            dfFlussoMisureSmis: DataFrame,
                                                                            trattPodllAnnomesePartitioned: DataFrame,
                                                                            start_in: String,
                                                                            stop_in: String,
                                                                            timestamp: Long
                                                                          )
                                                                          (implicit spark: SparkSession): DataFrame = {


    val df = dfFlussoMisureSmis
      .withColumn(start_in_column, lit(start_in))
      .withColumn(stop_in_column, lit(stop_in))

    val dfFinestraTemporale = df
      .filter(col("data_misura_mn_yyyymmdd").between(col(start_in_column), col(stop_in_column)))

    val dfFinestraTemporaleLastTimeStamp = dfFinestraTemporale
      .withColumn(rank,
        row_number().over(windowsSpecFlussiTecniciSmisMontaggio).as(rank))
      .filter(col(rank) === 1)

    val dfFinal = dfFinestraTemporaleLastTimeStamp
      .withColumn(erpValidatedMisNoSchema.anno, col(flussoMisureSmisSchema.anno_dtms).cast("string"))
      .withColumn(erpValidatedMisNoSchema.mese, lpad(col(flussoMisureSmisSchema.mese_dtms).cast("string"), 2, "0"))
      .withColumnRenamed(flussoMisureSmisSchema.data_misura_mn, erpValidatedMisNoSchema.data_misura)
      .withColumn(erpValidatedMisNoSchema.tipo_dato_s, when(col(flussoMisureSmisSchema.tipo_dato_smn) === Constants.tipo_dato_smnS, 1).otherwise(0))
      .withColumn(erpValidatedMisNoSchema.tipo_dato_e, when(col(flussoMisureSmisSchema.tipo_dato_smn) === Constants.tipo_dato_smnE, 1).otherwise(0))
      .withColumnRenamed(flussoMisureSmisSchema.eam_mn, erpValidatedMisNoSchema.eam)
      .withColumnRenamed(flussoMisureSmisSchema.eaf1_mn, erpValidatedMisNoSchema.eaf1)
      .withColumnRenamed(flussoMisureSmisSchema.eaf2_mn, erpValidatedMisNoSchema.eaf2)
      .withColumnRenamed(flussoMisureSmisSchema.eaf3_mn, erpValidatedMisNoSchema.eaf3)
      .withColumn(erpValidatedMisNoSchema.tipo_flusso, lit(Constants.tipo_flussoB))
      .withColumnRenamed(flussoMisureSmisSchema.perditatens_mn, erpValidatedMisNoSchema.coeff_perdita)
      .withColumnRenamed(flussoMisureSmisSchema.ka_mn, erpValidatedMisNoSchema.k)
      .withColumn(erpValidatedMisNoSchema.executionid, lit(timestamp))

    val dfJoin = dfFinal.join(
      trattPodllAnnomesePartitioned,
      col(trattPodAllAnnomesePartitionedSchema.pod14) === col(flussoMisureSmisSchema.pod) &&
        col(anno_tratt) === col(erpValidatedMisNoSchema.anno) &&
        col(mese_tratt) === col(erpValidatedMisNoSchema.mese)  // 🔧 FIX SMIS-MONTAGGIO: confronto STRING-to-STRING
      , "inner"

    )
      .withColumnRenamed(trattPodAllAnnomesePartitionedSchema.is_t_trattamento.toString, erpValidatedMisNoSchema.trattamento.toString)
      .selectExpr(erpValidatedMisNoSchema.getValues: _*)

    dfJoin

  }

}



