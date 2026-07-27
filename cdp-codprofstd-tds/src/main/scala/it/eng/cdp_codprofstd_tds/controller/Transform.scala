package it.eng.cdp_codprofstd_tds.controller

import it.eng.cdp_codprofstd_tds.schema._
import it.eng.cdp_codprofstd_tds.utility.{Constants, Environment}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

object Transform {

  def transform(rcuGasMassivo: DataFrame,
                gasTds: DataFrame,
                istatRegClima: DataFrame,
                rcuGasConnessioniDistr2: DataFrame,
                rcuGasBilanciamento: DataFrame,
                prtVsg: DataFrame,
                prtVtg: DataFrame,
                prtVsgAggRcu: DataFrame,
                prtVtgAggRcu: DataFrame,
                excludedPdrFromCsv: DataFrame,
                exclusionIsActive: Boolean,
                forzaturaIsActive: Boolean,
                freezeDate: String,
                annoCompetenza: String,
                executionId: Long
               ): DataFrame = {

    val rcuGasMassivoWithZonaClimatica = getZonaClimatica(rcuGasMassivo, istatRegClima)
    val rcuGasMassivoWithZonaClimaticaRemiDistr = getCodRemiAndDistr(rcuGasMassivoWithZonaClimatica, rcuGasConnessioniDistr2)
    val rcuGasMassivoFinal = getUdb(rcuGasMassivoWithZonaClimaticaRemiDistr, rcuGasBilanciamento)
    val excludedPdr = getExcludedPdr(prtVsg, prtVtg, prtVsgAggRcu, prtVtgAggRcu)
    val filteredGasTds = if (exclusionIsActive) excludePdrFromCsv(gasTds, excludedPdrFromCsv, ExclusionPdrSchema.pdr) else gasTds

    val codProfStdDaTds = gasTdsJoins(filteredGasTds, rcuGasMassivoFinal, excludedPdr, freezeDate, annoCompetenza, executionId)

    if (forzaturaIsActive) forcing(codProfStdDaTds) else forcingNull(codProfStdDaTds)
  }

  private def getZonaClimatica(rcuGasMassivo: DataFrame, istatRegClima: DataFrame): DataFrame = {
    rcuGasMassivo
      .join(istatRegClima, rcuGasMassivo(RcuGasMassivoSchema.t_comune_istat_pdr) === istatRegClima(IstatRegClimaSchema.t_codice_istat), "left_outer")
      .drop(istatRegClima(IstatRegClimaSchema.t_codice_istat))
  }

  private def getCodRemiAndDistr(rcuGasMassivo: DataFrame, rcuGasConnessioniDistr2: DataFrame): DataFrame = {
    rcuGasMassivo
      .join(rcuGasConnessioniDistr2, rcuGasMassivo(RcuGasMassivoSchema.t_codice_pdr) === rcuGasConnessioniDistr2(RcuGasConnessioniDistr2Schema.t_codice_pdr))
      .drop(rcuGasConnessioniDistr2(RcuGasConnessioniDistr2Schema.t_codice_pdr))
  }

  private def getUdb(rcuGasMassivo: DataFrame, rcuGasBilanciamento: DataFrame): DataFrame = {
    rcuGasMassivo
      .join(rcuGasBilanciamento, rcuGasMassivo(RcuGasMassivoSchema.n_id_pdr) === rcuGasBilanciamento(RcuGasBilanciamentoSchema.n_id_pdr), "left_outer")
      .drop(rcuGasBilanciamento(RcuGasBilanciamentoSchema.n_id_pdr))
  }

  private def getExcludedPdr(prtVsg: DataFrame, prtVtg: DataFrame, prtVsgAggRcu: DataFrame, prtVtgAggRcu: DataFrame): DataFrame = {
    val pdrVsg = prtVsg.join(prtVsgAggRcu, prtVsg(PrtVsgSchema.n_id_pratica) === prtVsgAggRcu(PrtVsgAggRcuSchema.n_id_pratica))
      .withColumnRenamed(PrtVsgSchema.d_data_esecuzione, "data_filtro")
      .withColumnRenamed(PrtVsgSchema.t_codice_pdr, "codice_pdr")
      .select("codice_pdr", "data_filtro")
      .distinct()

    val pdrVtg = prtVtg.join(prtVtgAggRcu, prtVtg(PrtVtgSchema.n_id_pratica) === prtVtgAggRcu(PrtVtgAggRcuSchema.n_id_pratica))
      .withColumnRenamed(PrtVtgSchema.d_data_dec, "data_filtro")
      .withColumnRenamed(PrtVtgSchema.t_codice_pdr, "codice_pdr")
      .select("codice_pdr", "data_filtro")
      .distinct()

    pdrVsg
      .union(pdrVtg)
      .coalesce(pdrVsg.rdd.getNumPartitions)
      .groupBy("codice_pdr")
      .agg(max("data_filtro").as("data_filtro"))
  }

  private def excludePdrFromCsv(gasTds: DataFrame, excludedPdr: DataFrame, columnName: String): DataFrame = {
    gasTds
      .join(excludedPdr, gasTds(GasTdsSchema.cod_pdr) === excludedPdr(columnName), "left_outer")
      .filter(excludedPdr(columnName).isNull)
      .drop(excludedPdr(columnName))
  }

  private def gasTdsJoins(gasTds: DataFrame, rcuGasMassivo: DataFrame, excludedPdr: DataFrame, freezeDate: String, annoCompetenza: String, executionId: Long): DataFrame = {
    val (numOfNullColumns, nullColumnsNames) = getErrorLog(
      col(CodProfStdDaTdsSchema.cod_remi),
      col(CodProfStdDaTdsSchema.cat_uso),
      col(CodProfStdDaTdsSchema.zona_climatica),
      col(CodProfStdDaTdsSchema.classe_prelievo)
    )

    val zonaClimaticaX = when(col(CodProfStdDaTdsSchema.cat_uso).isin("C2", "C4", "T1"), lit("X"))
      .otherwise(col(CodProfStdDaTdsSchema.zona_climatica))

    gasTds
      .join(excludedPdr, gasTds(GasTdsSchema.cod_pdr) === excludedPdr("codice_pdr"), "left_outer")
      .filter(excludedPdr("codice_pdr").isNull || excludedPdr("data_filtro") < gasTds(GasTdsSchema.data_creazione))
      .drop(excludedPdr("codice_pdr"))
      .drop(excludedPdr("data_filtro"))
      .join(rcuGasMassivo, gasTds(GasTdsSchema.cod_pdr) === rcuGasMassivo(RcuGasMassivoSchema.t_codice_pdr), "left_outer")
      .drop(rcuGasMassivo(RcuGasMassivoSchema.t_codice_pdr))
      //.withColumn(CodProfStdDaTdsSchema.cat_uso, col(GasTdsSchema.cat_uso))
      //.withColumn(CodProfStdDaTdsSchema.id_regione_climatica, col(RcuGasMassivoSchema.id_regione_climatica))
      //.withColumn(CodProfStdDaTdsSchema.classe_prelievo, col(GasTdsSchema.classe_prelievo))
      .withColumn(CodProfStdDaTdsSchema.data_fine_for, col(RcuGasMassivoSchema.data_fine_for).cast(TimestampType))
      .withColumn(CodProfStdDaTdsSchema.data_creazione, col(GasTdsSchema.data_creazione).cast(TimestampType))
      //.withColumn(CodProfStdDaTdsSchema.valid, col(GasTdsSchema.valid))
      //.withColumn(CodProfStdDaTdsSchema.n_id_distr, col(RcuGasConnessioniDistr2.n_id_distr)
      .withColumnRenamed(GasTdsSchema.cod_pdr, CodProfStdDaTdsSchema.codice_pdr)
      .withColumnRenamed(RcuGasConnessioniDistr2Schema.t_remi, CodProfStdDaTdsSchema.cod_remi)
      .withColumnRenamed(IstatRegClimaSchema.t_regione_climatica, CodProfStdDaTdsSchema.zona_climatica)
      .withColumnRenamed(RcuGasMassivoSchema.n_prelievo_annuo, CodProfStdDaTdsSchema.prelievo_annuo_prev)
      .withColumnRenamed(RcuGasMassivoSchema.t_trattamento, CodProfStdDaTdsSchema.trattamento)
      .withColumnRenamed(RcuGasMassivoSchema.n_id_az_udd, CodProfStdDaTdsSchema.n_id_udd)
      .withColumnRenamed(RcuGasMassivoSchema.t_cod_profilo, CodProfStdDaTdsSchema.cod_prof_prel_std)
      .withColumn(CodProfStdDaTdsSchema.cod_prof_prel_std_calc,
        when(col(CodProfStdDaTdsSchema.cat_uso).isNull
          || col(CodProfStdDaTdsSchema.zona_climatica).isNull
          || col(CodProfStdDaTdsSchema.classe_prelievo).isNull, lit(""))
          .otherwise(concat(col(CodProfStdDaTdsSchema.cat_uso), zonaClimaticaX, col(CodProfStdDaTdsSchema.classe_prelievo)))
      )
      .withColumn(CodProfStdDaTdsSchema.error_log,
        when(numOfNullColumns === lit(0), lit(""))
          .when(numOfNullColumns === lit(1), concat(lit("Il campo "), nullColumnsNames, lit(" non è valorizzato.")))
          .when(numOfNullColumns > lit(1), concat(lit("I campi "), nullColumnsNames, lit(" non sono valorizzati.")))
      )
      .withColumn(CodProfStdDaTdsSchema.pres_tds, lit("SI"))
      .withColumn(CodProfStdDaTdsSchema.massivo_freeze_execution_id, lit(Environment.getRcugasMassivoExecutionId).cast(LongType))
      .withColumn(CodProfStdDaTdsSchema.massivo_freeze_date, lit(freezeDate).cast(TimestampType))
      .withColumn(CodProfStdDaTdsSchema.anno_competenza, lit(annoCompetenza).cast(IntegerType))
      .withColumn(CodProfStdDaTdsSchema.execution_id, lit(executionId).cast(LongType))
  }

  private def getErrorLog(exprs: Column*): (Column, Column) = {
    val numOfNullColumns = exprs
      .map(column => when(column.isNull, 1).otherwise(0))
      .reduce(_ + _)
    val nullColumnsNames = exprs
      .map(column => when(column.isNull, column.toString).otherwise(null))
      .reduce((colA, colB) => (colA, colB) match {
        case (null, null) => lit(null)
        case (stringA, null) => lit(stringA)
        case (stringA, stringB) => lit(concat_ws(", ", stringA, stringB))
      })
    (numOfNullColumns, nullColumnsNames)
  }

  private def forcingNull(codProfStdDaTds: DataFrame): DataFrame = {
    codProfStdDaTds
      .withColumn(CodProfStdDaTdsSchema.cod_prof_prel_std_forced, lit(null).cast(StringType))
      .withColumn(CodProfStdDaTdsSchema.prelievo_annuo_prev_forced, lit(null).cast(StringType))
      .withColumn(CodProfStdDaTdsSchema.cat_uso_forced, lit(null).cast(StringType))
      .withColumn(CodProfStdDaTdsSchema.classe_prelievo_forced, lit(null).cast(StringType))
      .withColumn(CodProfStdDaTdsSchema.trattamento_forced, lit(null).cast(StringType))
      .withColumn(CodProfStdDaTdsSchema.zona_climatica_forced, lit(null).cast(StringType))
  }

  private def forcing(codProfStdDaTds: DataFrame): DataFrame = {
    val filePath: String = Environment.getFilterPdrForzaturaPath

    val csv_trattamento = "csv_trattamento"
    val forcingInputParamDF: DataFrame = Environment.getSpark.sqlContext.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .format("csv").schema(ForcingInputParamSchema.schema).load(filePath)
      .withColumnRenamed(ForcingInputParamSchema.trattamento.toString, csv_trattamento)

    val codProfStdDaTdsForced = codProfStdDaTds
      .join(broadcast(forcingInputParamDF), codProfStdDaTds(CodProfStdDaTdsSchema.codice_pdr) === forcingInputParamDF(ForcingInputParamSchema.pdr), "left")
      .withColumn(CodProfStdDaTdsSchema.prelievo_annuo_prev_forced, forcingInputParamDF.col(ForcingInputParamSchema.ca).cast(StringType))
      .drop(forcingInputParamDF.col(ForcingInputParamSchema.pdr))
      .drop(forcingInputParamDF.col(ForcingInputParamSchema.ca))
      .withColumnRenamed(ForcingInputParamSchema.codPrel, CodProfStdDaTdsSchema.cod_prof_prel_std_forced)
      .withColumnRenamed(ForcingInputParamSchema.catUso, CodProfStdDaTdsSchema.cat_uso_forced)
      .withColumnRenamed(ForcingInputParamSchema.zonClimatica, CodProfStdDaTdsSchema.zona_climatica_forced)
      .withColumnRenamed(ForcingInputParamSchema.classePrelievo, CodProfStdDaTdsSchema.classe_prelievo_forced)
      .withColumnRenamed(csv_trattamento, CodProfStdDaTdsSchema.trattamento_forced)
      //replace blank with nulls
      .withColumn(CodProfStdDaTdsSchema.prelievo_annuo_prev_forced, replaceBlankStringWithNullExpression(CodProfStdDaTdsSchema.prelievo_annuo_prev_forced))
      .withColumn(CodProfStdDaTdsSchema.cod_prof_prel_std_forced, replaceBlankStringWithNullExpression(CodProfStdDaTdsSchema.cod_prof_prel_std_forced))
      .withColumn(CodProfStdDaTdsSchema.cat_uso_forced, replaceBlankStringWithNullExpression(CodProfStdDaTdsSchema.cat_uso_forced))
      .withColumn(CodProfStdDaTdsSchema.zona_climatica_forced, replaceBlankStringWithNullExpression(CodProfStdDaTdsSchema.zona_climatica_forced))
      .withColumn(CodProfStdDaTdsSchema.classe_prelievo_forced, replaceBlankStringWithNullExpression(CodProfStdDaTdsSchema.classe_prelievo_forced))
      .withColumn(CodProfStdDaTdsSchema.trattamento_forced, replaceBlankStringWithNullExpression(CodProfStdDaTdsSchema.trattamento_forced))

    addForcedColumnsLogic(codProfStdDaTdsForced)
  }

  private def replaceBlankStringWithNullExpression(colName: String): Column = {
    when(col(colName) =!= lit(""), col(colName))
  }

  /**
   * Scenarios: if an input file with pdr to force was specified for this execution then the pdrMassivoDF used to
   * compute CaFinalDF has at least one of the fields  prelievo_annuo_prev_forced, cod_prof_prel_std_forced, cat_uso_forced,
   * zona_climatica_forced, classe_prelievo_forced valued. Otherwise this method does nothing.
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
   * @param codProfStdDaTds without fields ending with _forced following AU business logic
   * @return codProfStdDaTds, the Same DataFrame in input but with  fields ending with _forced following AU business logic
   */
  def addForcedColumnsLogic(codProfStdDaTds: DataFrame): DataFrame = {
    val codPrelForcedColumn = col(CodProfStdDaTdsSchema.cod_prof_prel_std_forced)
    codProfStdDaTds.persist(StorageLevel.MEMORY_AND_DISK)

    val dfWithCodProfValued = codProfStdDaTds.where(codPrelForcedColumn.isNotNull)
    /*
    Quando in input non é specificato ne cod_prof_prel_std_forced ne i tre campi che lo compongono (cat_uso_forced,
     classe_prelievo_forced, zona_climatica_forced) forzarne comunque il valore sarebbe un errore. Questo filtro serve
    ad ignorare tutte le righe per cui sarebbe un errore forzare cod_prof_prel_std_forced.
    */
    val dfRowsToIgnore = codProfStdDaTds.where(col(CodProfStdDaTdsSchema.cod_prof_prel_std_forced).isNull)
      .where(col(CodProfStdDaTdsSchema.cat_uso_forced).isNull)
      .where(col(CodProfStdDaTdsSchema.classe_prelievo_forced).isNull)
      .where(col(CodProfStdDaTdsSchema.zona_climatica_forced).isNull)

    val dfWithCodProfNotValued = codProfStdDaTds.where(codPrelForcedColumn.isNull).except(dfRowsToIgnore)

    // se cod_prof_prel_std_forced non è nullo :
    // 1. set cat_uso_forced come i primi due caratteri di cod_prof_prel_std_forced
    // 2. set classe_prelievo_forced come l'ultimo carattere di cod_prof_prel_std_forced
    // 3. set zona_climatica_forced come:
    //       - se cod_prof_prel_std_forced[3] == X    =>  zona_climatica_forced
    //       - se cod_prof_prel_std_forced[3] != X    =>  cod_prof_prel_std_forced[3]
    val zonaClimFromCodPrelStd = "zona_clim_tmp"
    val zonaClimFromFile = "zona_clim_from_file"
    val df1 = dfWithCodProfValued
      .drop(CodProfStdDaTdsSchema.cat_uso_forced) //cat_uso_forced overwrote with first two char of cod_prof_prel_std_forced
      .withColumn(CodProfStdDaTdsSchema.cat_uso_forced, substring(codPrelForcedColumn, 0, 2))
      .drop(CodProfStdDaTdsSchema.classe_prelievo_forced) //classe_prelievo_forced overwrote with last char of cod_prof_prel_std_forced
      .withColumn(CodProfStdDaTdsSchema.classe_prelievo_forced, substring(codPrelForcedColumn, 4, 1))
      .withColumnRenamed(CodProfStdDaTdsSchema.zona_climatica_forced, zonaClimFromFile) //zona_climatica_forced overwrote with 3rd char of cod_prof_prel_std_forced if not X
      .withColumn(zonaClimFromCodPrelStd, substring(codPrelForcedColumn, 3, 1))
      .withColumn(CodProfStdDaTdsSchema.zona_climatica_forced, when(col(zonaClimFromCodPrelStd) === lit("X"), col(zonaClimFromFile)).otherwise(col(zonaClimFromCodPrelStd)))
      .drop(col(zonaClimFromFile))
      .drop(col(zonaClimFromCodPrelStd))

    // se cod_prof_prel_std_forced è nullo lo compongo concatenando cat_uso_forced, zona_climatica_forced, classe_prelievo_forced
    val zonaClimaticaTmp = "zona_climatica_tmp"
    val catUsoTmp = "cat_uso_tmp"
    val classePrelievoTmp = "classe_prelievo_tmp"
    val df2 = dfWithCodProfNotValued //use forced values to build cod_prof_prel_std_forced
      .withColumn(catUsoTmp, when(col(CodProfStdDaTdsSchema.cat_uso_forced).isNull, col(CodProfStdDaTdsSchema.cat_uso)).otherwise(col(CodProfStdDaTdsSchema.cat_uso_forced)))
      .withColumn(classePrelievoTmp, when(col(CodProfStdDaTdsSchema.classe_prelievo_forced).isNull, col(CodProfStdDaTdsSchema.classe_prelievo)).otherwise(col(CodProfStdDaTdsSchema.classe_prelievo_forced)))
      .withColumn(zonaClimaticaTmp, getZonaClimaticaExpression)
      .drop(CodProfStdDaTdsSchema.cod_prof_prel_std_forced)
      .withColumn(CodProfStdDaTdsSchema.cod_prof_prel_std_forced, concat(col(catUsoTmp), col(zonaClimaticaTmp), col(classePrelievoTmp)))
      .drop(catUsoTmp)
      .drop(classePrelievoTmp)
      .drop(zonaClimaticaTmp)
    // We must select since in this version of spark union is positional not nominal on columns
    val dfFinalWithForcedFields = df1.selectExpr(CodProfStdDaTdsSchema.getValues: _*)
      .union(df2.selectExpr(CodProfStdDaTdsSchema.getValues: _*))
      .coalesce(codProfStdDaTds.rdd.getNumPartitions)
      .union(dfRowsToIgnore.selectExpr(CodProfStdDaTdsSchema.getValues: _*))
      .coalesce(codProfStdDaTds.rdd.getNumPartitions)


    validateForcedFields(dfFinalWithForcedFields)
      .selectExpr(CodProfStdDaTdsSchema.getValues: _*)
  }

  private def getZonaClimaticaExpression: Column = {
    val catUsoForcedIsC2 = col(CodProfStdDaTdsSchema.cat_uso_forced) === lit("C2")
    val catUsoForcedIsC4 = col(CodProfStdDaTdsSchema.cat_uso_forced) === lit("C4")
    val catUsoForcedIsT1 = col(CodProfStdDaTdsSchema.cat_uso_forced) === lit("T1")
    val catUsoIsC2 = col(CodProfStdDaTdsSchema.cat_uso) === lit("C2")
    val catUsoIsC4 = col(CodProfStdDaTdsSchema.cat_uso) === lit("C4")
    val catUsoIsT1 = col(CodProfStdDaTdsSchema.cat_uso) === lit("T1")

    when(catUsoForcedIsC2.or(catUsoForcedIsC4).or(catUsoForcedIsT1), lit("X"))
      .when(col(CodProfStdDaTdsSchema.zona_climatica_forced).isNull.and(catUsoIsC2.or(catUsoIsC4).or(catUsoIsT1)), lit("X"))
      .when(col(CodProfStdDaTdsSchema.zona_climatica_forced).isNull, col(CodProfStdDaTdsSchema.zona_climatica))
      .otherwise(col(CodProfStdDaTdsSchema.zona_climatica_forced))
  }

  /**
   * if cod_prof_prel_std_forced is not valid we set cod_prof_prel_std_forced, cat_uso_forced, zona_climatica_forced,
   * classe_prelievo_forced to null before returning, otherwise the method does nothing.
   *
   * @param dfFinalWithForcedFileds , the ca final after business logic for forced fields is applied (NB. valid in case
   *                                of filterPdr.mode=forzatura)
   * @return the caFinal where  if cod_prof_prel_std_forced is not valid we set cod_prof_prel_std_forced,
   *         cat_uso_forced, zona_climatica_forced, classe_prelievo_forced to null before returning.
   */
  private def validateForcedFields(dfFinalWithForcedFileds: DataFrame): DataFrame = {
    val isCodProfForcedValidCol = "cod_prof_forced_is_valid"
    dfFinalWithForcedFileds
      .withColumn(isCodProfForcedValidCol, col(CodProfStdDaTdsSchema.cod_prof_prel_std_forced).isin(Constants.codProfPrelStdWitheList: _*))
      .withColumn(CodProfStdDaTdsSchema.cod_prof_prel_std_forced, when(col(isCodProfForcedValidCol), col(CodProfStdDaTdsSchema.cod_prof_prel_std_forced)).otherwise(lit(null)))
      .withColumn(CodProfStdDaTdsSchema.cat_uso_forced, when(col(isCodProfForcedValidCol), col(CodProfStdDaTdsSchema.cat_uso_forced)).otherwise(lit(null)))
      .withColumn(CodProfStdDaTdsSchema.classe_prelievo_forced, when(col(isCodProfForcedValidCol), col(CodProfStdDaTdsSchema.classe_prelievo_forced)).otherwise(lit(null)))
      .withColumn(CodProfStdDaTdsSchema.zona_climatica_forced, when(col(isCodProfForcedValidCol), col(CodProfStdDaTdsSchema.zona_climatica_forced)).otherwise(lit(null)))
      .drop(col(isCodProfForcedValidCol))
  }

}
