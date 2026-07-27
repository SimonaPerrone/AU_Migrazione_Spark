package it.eng.au.calcoloSettlementGas.controller

import it.eng.au.calcoloSettlementGas.model.ParametriCarattProfPrel
import it.eng.au.calcoloSettlementGas.schema.{TabParametriCaratteristiciProfPrelSchema, TabProfiliGiornStdPercSchema}
import it.eng.au.calcoloSettlementGas.utility.Constants.{CSV_DELIMITER, ID_REG_CLIM_VALUES_Complete}
import it.eng.au.trasmissioneSettlementGasCommon.schema.{TSGQKRIUDSchema, TSGTFCSchema, TSGVPGSchema}
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit, row_number, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

import java.io.File

object CalcoloController extends Serializable {
  def getLatestVPGRecords(df: DataFrame): DataFrame = {
    val window = Window.partitionBy(TSGVPGSchema.giorno_riferimento).orderBy(col(TSGVPGSchema.executionid).desc, col(TSGVPGSchema.progressivo).desc)

    df
      .withColumn("rn", row_number().over(window))
      .filter(col("rn") === 1)
      .drop(col("rn"))
  }

  def getLatestTFCRecords(df: DataFrame): DataFrame = {
    val window = Window.partitionBy(TSGTFCSchema.data, TSGTFCSchema.id_reg_clim).orderBy(col(TSGTFCSchema.executionid).desc, col(TSGTFCSchema.progressivo).desc)

    df
      .withColumn("rn", row_number().over(window))
      .filter(col("rn") === 1)
      .drop(col("rn"))
  }

  def getLatestQKRIUDRecords(df: DataFrame): DataFrame = {
    val window = Window.partitionBy(TSGQKRIUDSchema.data, TSGQKRIUDSchema.cod_remi).orderBy(col(TSGQKRIUDSchema.executionid).desc, col(TSGQKRIUDSchema.progressivo).desc)

    df
      .withColumn("rn", row_number().over(window))
      .filter(col("rn") === 1)
      .drop(col("rn"))
  }

  //FIXME: il file dei Parametri caratteristici dove lo andiamo a prendere? Io avevo pensato di metterlo in locale
  // Su qualche cartella...
  def getParametriCarattProfPrelFromFile(file: File): List[ParametriCarattProfPrel] = {
    val buffSourceFile = scala.io.Source.fromFile(file)

    val rowsList = buffSourceFile.getLines().map(_.split(CSV_DELIMITER)).toList


    val parametriCarattProfPrelList = rowsList.slice(1, rowsList.length)
      .map(line => ParametriCarattProfPrel(prof = line(0), b1prof = line(1).replace(',','.').toDouble,
        b2prof = line(2).replace(',','.').toDouble, b3prof = line(3).replace(',','.').toDouble, b4prof = line(4).replace(',','.').toDouble,
        categoriaUso = line(5), zonaClimatica = line.lift(6).filter(_.nonEmpty), ClassePrelievo = line(7)))


    parametriCarattProfPrelList
  }

  // TODO: IMPLEMENTARE PROCEDURA CHE SELEZIONA L'ULTIMO RECORD PER OGNI GIORNO. ---> PARTIZIONA PER ANNOMESE COMPETENZA.

  def calcoloPprof(dfTSGTFC: DataFrame, dfTSGVPG: DataFrame, dfTSGQKRIUD: DataFrame, dfTabParamCarattProfPrel: DataFrame, remiAnagrafica: DataFrame): DataFrame = {

    val dfIdRegClim = broadcast(Environment.spark.createDataFrame(Environment.sparkContext.parallelize(ID_REG_CLIM_VALUES_Complete))
      .toDF("id_regione_climatica", "ID_Reg_Clim_name")
      .select("id_regione_climatica"))

    val remiAnagraficaBroadcast = broadcast(remiAnagrafica)

    val crossJoinedDf = dfTSGVPG.crossJoin(dfIdRegClim)

    val joinedTables = dfTSGTFC
      .join(crossJoinedDf,
      dfTSGTFC(TSGTFCSchema.data).cast(StringType) ===
        crossJoinedDf(TSGVPGSchema.giorno_riferimento).cast(StringType) && dfTSGTFC(TSGTFCSchema.id_reg_clim).cast(StringType) ===
        crossJoinedDf("id_regione_climatica").cast(StringType), "right")

    val completeDf = joinedTables.crossJoin(dfTabParamCarattProfPrel)

    val completeDfWithC1 = completeDf.withColumn("C1",
      when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "A" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "1", col(TSGVPGSchema.C1_A1))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "B" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "1", col(TSGVPGSchema.C1_B1))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "C" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "1", col(TSGVPGSchema.C1_C1))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "D" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "1", col(TSGVPGSchema.C1_D1))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "E" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "1", col(TSGVPGSchema.C1_E1))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "F" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "1", col(TSGVPGSchema.C1_F1))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "A" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "2", col(TSGVPGSchema.C1_A2))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "B" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "2", col(TSGVPGSchema.C1_B2))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "C" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "2", col(TSGVPGSchema.C1_C2))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "D" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "2", col(TSGVPGSchema.C1_D2))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "E" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "2", col(TSGVPGSchema.C1_E2))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "F" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "2", col(TSGVPGSchema.C1_F2))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "A" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "3", col(TSGVPGSchema.C1_A3))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "B" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "3", col(TSGVPGSchema.C1_B3))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "C" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "3", col(TSGVPGSchema.C1_C3))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "D" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "3", col(TSGVPGSchema.C1_D3))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "E" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "3", col(TSGVPGSchema.C1_E3))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM) === "F" && col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "3", col(TSGVPGSchema.C1_F3))
        .when(col(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM).isNull, lit(0.0).cast(DoubleType)))


    val completeDfWithC1T1 = completeDfWithC1.withColumn("T1",
      when(col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "1", col(TSGVPGSchema.T1_1))
        .when(col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "2", col(TSGVPGSchema.T1_2))
        .when(col(TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL) === "3", col(TSGVPGSchema.T1_3)))


    val completeDfWithCalcolo = completeDfWithC1T1
      .crossJoin(remiAnagraficaBroadcast)
      .join(dfTSGQKRIUD
        .withColumnRenamed(TSGQKRIUDSchema.data, TSGVPGSchema.giorno_riferimento)
        .select(TSGVPGSchema.giorno_riferimento, TSGQKRIUDSchema.cod_remi, TSGQKRIUDSchema.qkriud),
        Seq(TSGVPGSchema.giorno_riferimento.toString, TSGQKRIUDSchema.cod_remi.toString),
        "left")
      .withColumn(TSGQKRIUDSchema.qkriud, when(col(TSGQKRIUDSchema.qkriud).isNull, lit(0.0)).otherwise(col(TSGQKRIUDSchema.qkriud)))
      .withColumn(TabProfiliGiornStdPercSchema.pprofk,
        when(col(TSGTFCSchema.data).isNull, lit(null).cast(DoubleType))
          .otherwise(
        col(TabParametriCaratteristiciProfPrelSchema.BETA1_PROF) *
        col(TSGTFCSchema.wkr) * col("C1") +
        col(TabParametriCaratteristiciProfPrelSchema.BETA2_PROF) * col(TSGVPGSchema.C2) +
        col(TabParametriCaratteristiciProfPrelSchema.BETA3_PROF) * col("T1") +
        col(TabParametriCaratteristiciProfPrelSchema.BETA4_PROF) * col(TSGVPGSchema.C4) +
        coalesce(col(TSGQKRIUDSchema.qkriud), lit(0.0))))

    // Bisogna aggiungere le colonne per wkr normalizzato ed il relativo calcolo del Pprof con tale argomento.

    val completeDfWithWkrNorm = completeDfWithCalcolo.
      withColumn(TabProfiliGiornStdPercSchema.wkr_norm, lit(1))


    val completeDfWithPProfNorm = completeDfWithWkrNorm.withColumn(TabProfiliGiornStdPercSchema.pprofk_norm,
      when(col(TSGTFCSchema.wkr) === 1.0, col(TabProfiliGiornStdPercSchema.pprofk)).otherwise(
      col(TabParametriCaratteristiciProfPrelSchema.BETA1_PROF) *
        col(TabProfiliGiornStdPercSchema.wkr_norm) * col("C1") +
        col(TabParametriCaratteristiciProfPrelSchema.BETA2_PROF) * col(TSGVPGSchema.C2) +
        col(TabParametriCaratteristiciProfPrelSchema.BETA3_PROF) * col("T1") +
        col(TabParametriCaratteristiciProfPrelSchema.BETA4_PROF) * col(TSGVPGSchema.C4) +
        coalesce(col(TSGQKRIUDSchema.qkriud), lit(0.0))))
      .withColumnRenamed(TSGTFCSchema.executionid, "old_exec_id")


    val outputDf = completeDfWithPProfNorm
      .withColumnRenamed(TSGTFCSchema.data, "data_tfc")
      .withColumnRenamed(TSGVPGSchema.giorno_riferimento, TabProfiliGiornStdPercSchema.data)
      .withColumnRenamed(TSGTFCSchema.id_reg_clim, TabProfiliGiornStdPercSchema.id_reg_clim)
      .withColumnRenamed(TabParametriCaratteristiciProfPrelSchema.PROF, TabProfiliGiornStdPercSchema.prof)
      .withColumnRenamed(TSGTFCSchema.wkr, TabProfiliGiornStdPercSchema.wkr)
      .withColumnRenamed(TSGVPGSchema.annotermico, TabProfiliGiornStdPercSchema.annotermico)
      .withColumn(TabProfiliGiornStdPercSchema.executionid, lit(Environment.executionId).cast(LongType))

    val finalDf = outputDf.
      withColumn(TabProfiliGiornStdPercSchema.data, col(TabProfiliGiornStdPercSchema.data).cast(StringType)).
      withColumn(TabProfiliGiornStdPercSchema.pprofk, col(TabProfiliGiornStdPercSchema.pprofk).cast(DoubleType)).
      withColumn(TabProfiliGiornStdPercSchema.pprofk_norm, col(TabProfiliGiornStdPercSchema.pprofk_norm).cast(DoubleType)).
      withColumn(TabProfiliGiornStdPercSchema.id_reg_clim, col(TabProfiliGiornStdPercSchema.id_reg_clim).cast(IntegerType)).
      withColumn(TabProfiliGiornStdPercSchema.prof, col(TabProfiliGiornStdPercSchema.prof).cast(StringType)).
      withColumn(TabProfiliGiornStdPercSchema.wkr, col(TabProfiliGiornStdPercSchema.wkr).cast(DoubleType)).
      withColumn(TabProfiliGiornStdPercSchema.wkr_norm, col(TabProfiliGiornStdPercSchema.wkr_norm).cast(IntegerType)).
      withColumn(TabProfiliGiornStdPercSchema.cod_remi, col(TabProfiliGiornStdPercSchema.cod_remi).cast(StringType)).
      withColumn(TabProfiliGiornStdPercSchema.annotermico, col(TabProfiliGiornStdPercSchema.annotermico).cast(StringType)).
      withColumn(TabProfiliGiornStdPercSchema.executionid, col(TabProfiliGiornStdPercSchema.executionid).cast(LongType))


    finalDf.selectExpr(TabProfiliGiornStdPercSchema.getValues:_*)
  }
}
