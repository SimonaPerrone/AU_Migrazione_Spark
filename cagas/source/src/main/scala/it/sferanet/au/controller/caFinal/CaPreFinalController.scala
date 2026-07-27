package it.sferanet.au.controller.caFinal

import it.sferanet.au.model.Tds
import it.sferanet.au.schema.{CaPreFinalSchema, CaSchema, PdrMassivoSchema, SettleGasGasTdsSchema}
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

/**
 * Prima parte di exportCA.sql
 * PdrMassivo = censimento tutti PDR
 *
 * @param executionId execution id from App.run, the same for all the entities
 */
class CaPreFinalController(session: String, executionId: Long) {
  private val columnCAName = "ca"
  private val zonaClimaticaTmp = "zona_climatica_tmp"
  private val catUsoTmp = "cat_uso_tmp"
  private val classePrelievoTmp = "classe_prelievo_tmp"

  /**
   * Crea la tabella ca_pre_final a partire dalla massivo e dalla ca. In particolare,
   *  1. seleziona dalla ca i PdR con idCaErrorCode 0 (ovvero i PdR calcolati)
   *  1. tramite le informazioni contenute nella ca e nella massivo, popola vari campi, tra cui
   *  `cat_uso`, `classe_prelievo`, `zona_climatica`, `cod_prof_prel_std` e `prelievo_annuo_prev`
   *  1. per i PdR dedotti, popola i campi `pres_tds`, `classe_prelievo_forced` e `cat_uso_forced` utilizzando le informazioni dalla tds
   *  1. se presenti, applica le forzature per tutti i PdR e alla fine della procedura valida il codice profilo standard creato.
   *  Se ammissibile, la forzatura viene mantentura; in caso contrario viene scartata.
   * @param pdrMassivoDF dataframe from rcuGasMassivo
   * @param ca           the ca DataFrame
   * @return Dataframe con struttura Ca + info aggiuntive + righe con pdr su cui
   *         non è stato calcolato la CA (prese dalla massivo)
   */
  def get(pdrMassivoDF: DataFrame, ca: DataFrame, tdsForPresTds: DataFrame, tdsForDedotti: DataFrame): DataFrame = {
    //defining window function to get minimum error code received
    val minErrorColName = "min_error"
    val minErrorWindowSpec = Window.partitionBy(CaSchema.pdr)
    val filterMode = Environment.getFilterPdrMode
    val forcedMode = Environment.isForcingEnabled
    val exclusionMode = Environment.isExclusionFilterEnabled
    val massivoExecutionId = if (Environment.getRcugasMassivoPath.contains("freeze")) Environment.getMassivoExecutionId else "0"

    val caFiltered = ca.select(
      col(CaSchema.pdr)
      , col(CaSchema.next_cod_profilo)
      , col(CaSchema.id_regClim)
      , col(CaSchema.t_comune_istat_pdr).alias(CaPreFinalSchema.codistat)
      , col(CaSchema.ca_sum).as(columnCAName).cast(LongType)
      , col(CaSchema.start_local_file)
      , col(CaSchema.end_local_file)
      , col(CaSchema.start_t_misuratore_integrato)
      , col(CaSchema.end_t_misuratore_integrato)
      , col(CaSchema.start_t_pre_conv)
      , col(CaSchema.end_t_pre_conv)
      , col(CaSchema.n_coeff_correzione)
      , col(CaSchema.idCaErrorCode)
      , col(CaSchema.pres_tds)
      , col(CaSchema.tipologia_uso)
      , col(CaSchema.comp_termica)
      , col(CaSchema.cat_uso_tds)
      , col(CaSchema.classe_prelievo_tds)
      , col(CaSchema.cod_istat_last_rcu)
      , col(CaSchema.zona_climatica_lookup)
      , col(CaSchema.startSegment)
      , col(CaSchema.endSegment)
      , min(CaSchema.idCaErrorCode).over(minErrorWindowSpec).as(minErrorColName)
    ).where(col(CaSchema.idCaErrorCode) === lit(0))
      .distinct()

    //Defining recurring expressions
    val caNextCodProfiloIsValued = not(col(CaSchema.next_cod_profilo).isNull).and(col(CaSchema.next_cod_profilo) =!= "")
    val caIdRegClimIsGTZero = not(col(CaSchema.id_regClim).isNull).and(col(CaSchema.id_regClim) > 0)
    val getCodProfilo = when(caNextCodProfiloIsValued, col(CaSchema.next_cod_profilo))
      .otherwise(col(PdrMassivoSchema.t_cod_profilo))
    val caIsNull = col(columnCAName).isNull.or(col(columnCAName) <=> "")

    val forcedModeExpression = if (forcedMode.equals("true")) " con forzatura" else ""
    val exclusionModeExpression = if (exclusionMode.equals("true")) " con esclusione" else ""
    //This works also for the case where there is no match between pdrMassivo and ca: since we are doing a left join
    // either ca_sum is null because it has not been computed or because the record has not a match in the left-join
    val calModeExpression = concat_ws("", when(lit(filterMode != null && filterMode.equals(Constants.FILTER_MODE_FORZATURA)), lit(Constants.CALCMOD_FORZATO))
      .when((col(minErrorColName) <=> lit(0)) and col(columnCAName).isNotNull, lit(Constants.CALCMOD_PROCEDURA))
      .otherwise(lit(Constants.CALCMOD_DEDOTTO))
      , lit(forcedModeExpression), lit(exclusionModeExpression), col(PdrMassivoSchema.calcmode))

    val caFinalDF = pdrMassivoDF.join(caFiltered, pdrMassivoDF(PdrMassivoSchema.codice_pdr) === ca(CaSchema.pdr), "left")
      .withColumn(CaPreFinalSchema.cat_uso, substring(getCodProfilo, 1, 2))
      .withColumn(CaPreFinalSchema.classe_prelievo, substring(getCodProfilo, 4, 1))
      .withColumn(CaPreFinalSchema.zona_climatica,
        when(substring(getCodProfilo, 3, 1) === "X", col(PdrMassivoSchema.t_regione_climatica))
          .otherwise(substring(getCodProfilo, 3, 1))
      )
      .withColumn(CaPreFinalSchema.id_reg_clim,
        when(caIdRegClimIsGTZero, col(CaSchema.id_regClim).cast(StringType))
          .otherwise(col(PdrMassivoSchema.id_regione_climatica))
      )
      .withColumn(CaPreFinalSchema.cod_prof_prel_std,
        when(caNextCodProfiloIsValued, col(CaSchema.next_cod_profilo)).otherwise(col(PdrMassivoSchema.t_cod_profilo))
      )
      .withColumn(CaPreFinalSchema.prelievo_annuo_prev,
        when(not(caIsNull), col(columnCAName).cast(LongType))
          .otherwise(col(PdrMassivoSchema.n_prelievo_annuo).cast(LongType)).cast(StringType))
      .withColumn(CaPreFinalSchema.d_ricezione, date_format(lit(current_date()), "yyyy-MM-dd").cast(StringType))
      .withColumn(CaPreFinalSchema.is_ca_calculated, caIsNull)
      .withColumn(CaPreFinalSchema.massivo_freeze_executionid, lit(massivoExecutionId).cast(LongType))
      .withColumnRenamed(CaSchema.idCaErrorCode, CaPreFinalSchema.id_ca_error_code)
      .withColumn(CaPreFinalSchema.session, lit(session))
      .withColumn(CaPreFinalSchema.executionid, lit(executionId))
      .withColumn(CaPreFinalSchema.calcmode, calModeExpression)

    //We use the information from tdsForPresTds to include pres_tds for dedotti as well
    val caFinalWithPresTds = caFinalDF
      .join(broadcast(tdsForPresTds), caFinalDF(PdrMassivoSchema.codice_pdr) === tdsForPresTds(SettleGasGasTdsSchema.cod_pdr), "left")
      .withColumn(CaPreFinalSchema.pres_tds, when(col(CaPreFinalSchema.calcmode).contains(Constants.CALCMOD_DEDOTTO), tdsForPresTds(SettleGasGasTdsSchema.cod_pdr).isNotNull).otherwise(caFinalDF(CaPreFinalSchema.pres_tds)))

    //We use the latest information of tdsForDedotti to update classe_prelievo_forced and cat_uso_forced in case the PdR is "dedotto"
    //The information from file forcing should have an higher priority than info from tds (hence the coalesce) (AU-590)
    val caFinalWithTdsDedotti = caFinalWithPresTds
      .join(broadcast(tdsForDedotti), caFinalWithPresTds(PdrMassivoSchema.codice_pdr) === tdsForDedotti(SettleGasGasTdsSchema.cod_pdr), "left")
      .withColumn(CaPreFinalSchema.classe_prelievo_forced,
        when(col(CaPreFinalSchema.calcmode).contains(Constants.CALCMOD_DEDOTTO), coalesce(caFinalWithPresTds(CaPreFinalSchema.classe_prelievo_forced), tdsForDedotti(SettleGasGasTdsSchema.classe_prelievo)))
        .otherwise(caFinalWithPresTds(CaPreFinalSchema.classe_prelievo_forced)))
      .withColumn(CaPreFinalSchema.cat_uso_forced,
        when(col(CaPreFinalSchema.calcmode).contains(Constants.CALCMOD_DEDOTTO), coalesce(caFinalWithPresTds(CaPreFinalSchema.cat_uso_forced), tdsForDedotti(SettleGasGasTdsSchema.cat_uso)))
          .otherwise(caFinalWithPresTds(CaPreFinalSchema.cat_uso_forced)))
      .drop(tdsForDedotti(SettleGasGasTdsSchema.cod_pdr))
      .drop(tdsForDedotti(SettleGasGasTdsSchema.classe_prelievo))
      .drop(tdsForDedotti(SettleGasGasTdsSchema.cat_uso))
      .selectExpr(CaPreFinalSchema.getValues: _*)

    val caFinalWithForcedValues = if (forcedMode.equals("true") || filterMode.equals(Constants.FILTER_MODE_FORZATURA)) addForcedColumnsLogic(caFinalWithTdsDedotti) else caFinalWithTdsDedotti
    caFinalWithForcedValues
  }

  def write(caPreFinal: DataFrame, mode: String = "append"): Unit = {
    caPreFinal
      .selectExpr(CaPreFinalSchema.getValues: _*)
      .write
      .mode(mode)
      .partitionBy(CaPreFinalSchema.session, CaPreFinalSchema.tipo_trasmissione, CaPreFinalSchema.anno_competenza, CaPreFinalSchema.executionid)
      .parquet(Environment.getCaPreFinalPath)

    if (!Environment.isLocalMode) Environment.getSpark.sql(s"MSCK REPAIR TABLE ${Environment.getCaPreFinalTableName}")
  }

  @deprecated("Now it is exported later", "CaFinalController")
  def writeToExport(caFinal: DataFrame): Unit = {
    caFinal.write
      .mode("overwrite")
      .parquet(Environment.getCaFinalToExportPath)
  }

  /**
   * Scenarios: if an input file with pdr to force was specified for this execution then the pdrMassivoDF used to
   * compute CaFinalDF has at least one of the fields  prelievo_annuo_prev_forced, cod_prof_prel_std_forced, cat_uso_forced,
   * zona_climatica_forced, classe_prelievo_forced, trattamento_forced valued. Otherwise this method does nothing.
   *
   * <ul>
   * <li>if cod_prof_prel_std_forced is not null we derive  cat_uso_forced, zona_climatica_forced,
   * classe_prelievo_forced from it</li>
   * <li>if cod_prof_prel_std_forced is null as all the other fields to force (prelievo_annuo_prev_forced might be
   * null or not, we do not care), the method does nothing</li>
   * <li>if cod_prof_prel_std_forced is null but all the other fields to force are not, the method do derive
   * cod_prof_prel_std_forced from cat_uso_forced, zona_climatica_forced, classe_prelievo_forced and returns</li>
   * </ul>
   * Whatever case listed above we fall into, if cod_prof_prel_std_forced is not valid we set cod_prof_prel_std_forced,
   * cat_uso_forced, zona_climatica_forced, classe_prelievo_forced to null before returning.
   *
   * @param caFinal without fields ending with _forced following AU business logic
   * @return caFinal, the Same DataFrame in input but with  fields ending with _forced following AU business logic
   */
  def addForcedColumnsLogic(caFinal: DataFrame): DataFrame = {
    val codPrelForcedColumn = col(CaPreFinalSchema.cod_prof_prel_std_forced)
    caFinal.persist(StorageLevel.MEMORY_AND_DISK)

    val dfWithCodProfValued = caFinal.where(codPrelForcedColumn.isNotNull)
    /*
    Quando in input non é specificato ne cod_prof_prel_std_forced ne i tre campi che lo compongono (cat_uso_forced,
     classe_prelievo_forced, zona_climatica_forced) forzarne comunque il valore sarebbe un errore. Questo filtro serve
    ad ignorare tutte le righe per cui sarebbe un errore forzare cod_prof_prel_std_forced.
    */
    val dfRowsToIgnore = caFinal.where(col(CaPreFinalSchema.cod_prof_prel_std_forced).isNull)
      .where(col(CaPreFinalSchema.cat_uso_forced).isNull)
      .where(col(CaPreFinalSchema.classe_prelievo_forced).isNull)
      .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)

    val dfWithCodProfNotValued = caFinal.where(codPrelForcedColumn.isNull).exceptAll(dfRowsToIgnore)

    // se cod_prof_prel_std_forced non é nullo :
    // 1. set cat_uso_forced come i primi due caratteri di cod_prof_prel_std_forced
    // 2. set classe_prelievo_forced come l'ultimo carattere di cod_prof_prel_std_forced
    // 3. set zona_climatica_forced come:
    //       - se cod_prof_prel_std_forced[3] == X    =>  zona_climatica_forced
    //       - se cod_prof_prel_std_forced[3] != X    =>  cod_prof_prel_std_forced[3]
    val zonaClimFromCodPrelStd = "zona_clim_tmp"
    val zonaClimFromFile = "zona_clim_from_file"
    val df1 = dfWithCodProfValued
      .drop(CaPreFinalSchema.cat_uso_forced) //cat_uso_forced overwrote with first two char of cod_prof_prel_std_forced
      .withColumn(CaPreFinalSchema.cat_uso_forced, substring(codPrelForcedColumn, 0, 2))
      .drop(CaPreFinalSchema.classe_prelievo_forced) //classe_prelievo_forced overwrote with last char of cod_prof_prel_std_forced
      .withColumn(CaPreFinalSchema.classe_prelievo_forced, substring(codPrelForcedColumn, 4, 1))
      .withColumnRenamed(CaPreFinalSchema.zona_climatica_forced, zonaClimFromFile) //zona_climatica_forced overwrote with 3rd char of cod_prof_prel_std_forced if not X
      .withColumn(zonaClimFromCodPrelStd, substring(codPrelForcedColumn, 3, 1))
      .withColumn(CaPreFinalSchema.zona_climatica_forced, when(col(zonaClimFromCodPrelStd) === lit("X"), col(zonaClimFromFile)).otherwise(col(zonaClimFromCodPrelStd)))
      .drop(col(zonaClimFromFile))
      .drop(col(zonaClimFromCodPrelStd))

    // se cod_prof_prel_std_forced é nullo lo compongo concatenando cat_uso_forced, zona_climatica_forced, classe_prelievo_forced
    val df2 = dfWithCodProfNotValued //use forced values to build cod_prof_prel_std_forced
      .withColumn(catUsoTmp, when(col(CaPreFinalSchema.cat_uso_forced).isNull, col(CaPreFinalSchema.cat_uso)).otherwise(col(CaPreFinalSchema.cat_uso_forced)))
      .withColumn(classePrelievoTmp, when(col(CaPreFinalSchema.classe_prelievo_forced).isNull, col(CaPreFinalSchema.classe_prelievo)).otherwise(col(CaPreFinalSchema.classe_prelievo_forced)))
      .withColumn(zonaClimaticaTmp, getZonaClimaticaExpression)
      .drop(CaPreFinalSchema.cod_prof_prel_std_forced)
      .withColumn(CaPreFinalSchema.cod_prof_prel_std_forced, concat(col(catUsoTmp), col(zonaClimaticaTmp), col(classePrelievoTmp)))
      .drop(catUsoTmp)
      .drop(classePrelievoTmp)
      .drop(zonaClimaticaTmp)
    // We must select since in this version of spark union is positional not nominal on columns
    val caFinalWithForcedFieldsDF = df1.selectExpr(CaPreFinalSchema.getValues: _*)
      .union(df2.selectExpr(CaPreFinalSchema.getValues: _*)).coalesce(caFinal.rdd.getNumPartitions)
      .union(dfRowsToIgnore.selectExpr(CaPreFinalSchema.getValues: _*)).coalesce(caFinal.rdd.getNumPartitions)

    validateForcedFields(caFinalWithForcedFieldsDF)
      .selectExpr(CaPreFinalSchema.getValues: _*)
  }

  private def getZonaClimaticaExpression: Column = {
    val catUsoTmpIsC2 = col(catUsoTmp) === lit("C2")
    val catUsoTmpIsC4 = col(catUsoTmp) === lit("C4")
    val catUsoTmpIsT1 = col(catUsoTmp) === lit("T1")

    when(catUsoTmpIsC2.or(catUsoTmpIsC4).or(catUsoTmpIsT1), lit("X"))
      .otherwise(coalesce(col(CaPreFinalSchema.zona_climatica_forced), col(CaPreFinalSchema.zona_climatica)))
  }

  /**
   * if cod_prof_prel_std_forced is not valid we set cod_prof_prel_std_forced, cat_uso_forced, zona_climatica_forced,
   * classe_prelievo_forced to null before returning, otherwise the method does nothing.
   *
   * @param caFinalWithForcedFileds , the ca final after business logic for forced fields is applied (NB. valid in case
   *                                of filterPdr.mode=forzatura)
   * @return the caFinal where  if cod_prof_prel_std_forced is not valid we set cod_prof_prel_std_forced,
   *         cat_uso_forced, zona_climatica_forced, classe_prelievo_forced to null before returning.
   */
  private def validateForcedFields(caFinalWithForcedFileds: DataFrame): DataFrame = {
    val isCodProfForcedValidCol = "cod_prof_forced_is_valid"
    caFinalWithForcedFileds
      .withColumn(isCodProfForcedValidCol, col(CaPreFinalSchema.cod_prof_prel_std_forced).isin(Constants.codProfPrelStdWitheList: _*))
      .withColumn(CaPreFinalSchema.cod_prof_prel_std_forced, when(col(isCodProfForcedValidCol), col(CaPreFinalSchema.cod_prof_prel_std_forced)).otherwise(lit(null).cast(StringType)))
      .withColumn(CaPreFinalSchema.cat_uso_forced, when(col(isCodProfForcedValidCol), col(CaPreFinalSchema.cat_uso_forced)).otherwise(lit(null).cast(StringType)))
      .withColumn(CaPreFinalSchema.classe_prelievo_forced, when(col(isCodProfForcedValidCol), col(CaPreFinalSchema.classe_prelievo_forced)).otherwise(lit(null).cast(StringType)))
      .withColumn(CaPreFinalSchema.zona_climatica_forced, when(col(isCodProfForcedValidCol), col(CaPreFinalSchema.zona_climatica_forced)).otherwise(lit(null).cast(StringType)))
      .drop(col(isCodProfForcedValidCol))
  }

}
