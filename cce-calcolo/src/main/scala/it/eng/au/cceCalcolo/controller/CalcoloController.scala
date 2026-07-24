package it.eng.au.cceCalcolo.controller

import it.eng.au.cceCalcolo.schema.ammissibilita.ReportAmmissibilitaPodSchema
import it.eng.au.cceCalcolo.schema.annullamenti.{CceMoAnnullate1gEinSchema, CceMoAnnullate1gSchema, CceMoAnnullate2gEinSchema, CceMoAnnullate2gSchema}
import it.eng.au.cceCalcolo.schema.calcoloOutput.{CceCalcTrackSchema, CceCalcoloAnagraficaSchema, CceCalcoloPRSchema, CceCalcoloPReinSchema, CceCalcoloPSchema, CceCalcoloPeinSchema, CceCalcoloTrattamentoSchema}
import it.eng.au.cceCalcolo.schema.flussiMisure.{FlussoMisureQuartiInSchema, FlussoMisureQuartiSchema}
import it.eng.au.cceCalcolo.schema.ghigliottina.DataGhigliottinaSchema
import it.eng.au.cceCalcolo.schema.rcu.{RcuAziendaPSchema, RcuFornituraPSchema, RcuPodDistrPSchema, RcuPodMisurePSchema, RcuPodPSchema, RcuPodTecnPSchema, RcuPodUddPSchema, RcuTariffaPSchema, RcuUddPSchema}
import it.eng.au.cceCalcolo.schema.rcus.{RcusFornituraPSchema, RcusPodMisurePSchema, RcusPodPSchema, RcusPodUddPSchema}
import it.eng.au.cceCalcolo.utility.ApplicationConstant.{calcoloPR, calcoloPRein, esitoPositivoCalcolo, joinTypeAmmissibilita, joinTypeAnnullamenti}
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.environment.Environment.startDateTime
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, coalesce, col, concat, concat_ws, hash, lit, lpad, rank, round, row_number, split, substring, to_date, trim, upper, when}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

import scala.util.matching.Regex

class CalcoloController {

  def getPAggregation (flussoMisureQuartiDF: DataFrame, dataGhigliottina: DataFrame, annullate1G: DataFrame, annullate2G: DataFrame, reportAmmissibilita: DataFrame): DataFrame = {

    val logicKey1g: String = "logicKey1g"
    val logicKey2g: String = "logicKey2g"
    val logicKey1gFields = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti))
    val logicKey2gFields = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType), 2, "0"))
    val partitionColumns = Window.partitionBy(col(FlussoMisureQuartiSchema.annoquarti), col(FlussoMisureQuartiSchema.mesequarti), col(FlussoMisureQuartiSchema.giornoquarti), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.podquarti))

    val broadcastAnnullate1g = broadcast(annullate1G
      .select(CceMoAnnullate1gSchema.k_dt)
      .withColumn(CceMoAnnullate1gSchema.k_dt, hash(col(CceMoAnnullate1gSchema.k_dt))))

    val broadcastAnnullate2g = broadcast(annullate2G
      .select(CceMoAnnullate2gSchema.k_dt)
      .withColumn(CceMoAnnullate2gSchema.k_dt, hash(col(CceMoAnnullate2gSchema.k_dt))))

    flussoMisureQuartiDF
      .filter(col(FlussoMisureQuartiSchema.giornoquarti).isNotNull)
      .filter(col(FlussoMisureQuartiSchema.validato) === "S")
      .filter(!(col(FlussoMisureQuartiSchema.trattamento_o) === "C" and col(FlussoMisureQuartiSchema.nomefile).like("%2G%")))
      .filter(!col(FlussoMisureQuartiSchema.nomefile).like("%RFO%"))
      .join(dataGhigliottina, concat(col(FlussoMisureQuartiSchema.annoquarti), lpad(col(FlussoMisureQuartiSchema.mesequarti).cast(StringType), 2, "0")).equalTo(col(DataGhigliottinaSchema.annomese_aggregato)), "left")
      .withColumn(DataGhigliottinaSchema.d_ghigliottina, when(col(DataGhigliottinaSchema.d_ghigliottina).isNull, lit("29991231")).otherwise(col(DataGhigliottinaSchema.d_ghigliottina)))
      .filter(col(FlussoMisureQuartiSchema.annomesegiornodir) <= col(DataGhigliottinaSchema.d_ghigliottina).cast(IntegerType))
      .selectExpr(FlussoMisureQuartiSchema.getValues:_*)
      .withColumn(logicKey1g, hash(logicKey1gFields))
      .join(broadcastAnnullate1g
        , col(logicKey1g).equalTo(col(CceMoAnnullate1gSchema.k_dt)), joinTypeAnnullamenti)
      .selectExpr(FlussoMisureQuartiInSchema.getValues: _*)
      .withColumn(logicKey2g, hash(logicKey2gFields))
      .join(broadcastAnnullate2g
        , col(logicKey2g).equalTo(col(CceMoAnnullate2gSchema.k_dt)), joinTypeAnnullamenti)
      .withColumn("hashKey", hash(concat(upper(col(FlussoMisureQuartiSchema.nomefile)), substring(col(FlussoMisureQuartiSchema.podquarti),1,14), split(col(FlussoMisureQuartiSchema.nomefile),"_")(2), col(FlussoMisureQuartiSchema.annomesegiornodir))))
      .join(reportAmmissibilita.filter(col(ReportAmmissibilitaPodSchema.flusso).like("%PDO%")).select("hashKey")
        , Seq("hashKey"), joinTypeAmmissibilita)
      .withColumn("row_number", row_number().over(partitionColumns.orderBy(col(FlussoMisureQuartiSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceCalcoloPSchema.h01, round((col(FlussoMisureQuartiSchema.e1) + col(FlussoMisureQuartiSchema.e2) + col(FlussoMisureQuartiSchema.e3) + col(FlussoMisureQuartiSchema.e4))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h02, round((col(FlussoMisureQuartiSchema.e5) + col(FlussoMisureQuartiSchema.e6) + col(FlussoMisureQuartiSchema.e7) + col(FlussoMisureQuartiSchema.e8))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h03, round((col(FlussoMisureQuartiSchema.e9) + col(FlussoMisureQuartiSchema.e10) + col(FlussoMisureQuartiSchema.e11) + col(FlussoMisureQuartiSchema.e12))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h04, round((col(FlussoMisureQuartiSchema.e13) + col(FlussoMisureQuartiSchema.e14) + col(FlussoMisureQuartiSchema.e15) + col(FlussoMisureQuartiSchema.e16))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h05, round((col(FlussoMisureQuartiSchema.e17) + col(FlussoMisureQuartiSchema.e18) + col(FlussoMisureQuartiSchema.e19) + col(FlussoMisureQuartiSchema.e20))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h06, round((col(FlussoMisureQuartiSchema.e21) + col(FlussoMisureQuartiSchema.e22) + col(FlussoMisureQuartiSchema.e23) + col(FlussoMisureQuartiSchema.e24))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h07, round((col(FlussoMisureQuartiSchema.e25) + col(FlussoMisureQuartiSchema.e26) + col(FlussoMisureQuartiSchema.e27) + col(FlussoMisureQuartiSchema.e28))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h08, round((col(FlussoMisureQuartiSchema.e29) + col(FlussoMisureQuartiSchema.e30) + col(FlussoMisureQuartiSchema.e31) + col(FlussoMisureQuartiSchema.e32))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h09, round((col(FlussoMisureQuartiSchema.e33) + col(FlussoMisureQuartiSchema.e34) + col(FlussoMisureQuartiSchema.e35) + col(FlussoMisureQuartiSchema.e36))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h10, round((col(FlussoMisureQuartiSchema.e37) + col(FlussoMisureQuartiSchema.e38) + col(FlussoMisureQuartiSchema.e39) + col(FlussoMisureQuartiSchema.e40))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h11, round((col(FlussoMisureQuartiSchema.e41) + col(FlussoMisureQuartiSchema.e42) + col(FlussoMisureQuartiSchema.e43) + col(FlussoMisureQuartiSchema.e44))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h12, round((col(FlussoMisureQuartiSchema.e45) + col(FlussoMisureQuartiSchema.e46) + col(FlussoMisureQuartiSchema.e47) + col(FlussoMisureQuartiSchema.e48))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h13, round((col(FlussoMisureQuartiSchema.e49) + col(FlussoMisureQuartiSchema.e50) + col(FlussoMisureQuartiSchema.e51) + col(FlussoMisureQuartiSchema.e52))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h14, round((col(FlussoMisureQuartiSchema.e53) + col(FlussoMisureQuartiSchema.e54) + col(FlussoMisureQuartiSchema.e55) + col(FlussoMisureQuartiSchema.e56))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h15, round((col(FlussoMisureQuartiSchema.e57) + col(FlussoMisureQuartiSchema.e58) + col(FlussoMisureQuartiSchema.e59) + col(FlussoMisureQuartiSchema.e60))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h16, round((col(FlussoMisureQuartiSchema.e61) + col(FlussoMisureQuartiSchema.e62) + col(FlussoMisureQuartiSchema.e63) + col(FlussoMisureQuartiSchema.e64))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h17, round((col(FlussoMisureQuartiSchema.e65) + col(FlussoMisureQuartiSchema.e66) + col(FlussoMisureQuartiSchema.e67) + col(FlussoMisureQuartiSchema.e68))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h18, round((col(FlussoMisureQuartiSchema.e69) + col(FlussoMisureQuartiSchema.e70) + col(FlussoMisureQuartiSchema.e71) + col(FlussoMisureQuartiSchema.e72))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h19, round((col(FlussoMisureQuartiSchema.e73) + col(FlussoMisureQuartiSchema.e74) + col(FlussoMisureQuartiSchema.e75) + col(FlussoMisureQuartiSchema.e76))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h20, round((col(FlussoMisureQuartiSchema.e77) + col(FlussoMisureQuartiSchema.e78) + col(FlussoMisureQuartiSchema.e79) + col(FlussoMisureQuartiSchema.e80))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h21, round((col(FlussoMisureQuartiSchema.e81) + col(FlussoMisureQuartiSchema.e82) + col(FlussoMisureQuartiSchema.e83) + col(FlussoMisureQuartiSchema.e84))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h22, round((col(FlussoMisureQuartiSchema.e85) + col(FlussoMisureQuartiSchema.e86) + col(FlussoMisureQuartiSchema.e87) + col(FlussoMisureQuartiSchema.e88))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h23, round((col(FlussoMisureQuartiSchema.e89) + col(FlussoMisureQuartiSchema.e90) + col(FlussoMisureQuartiSchema.e91) + col(FlussoMisureQuartiSchema.e92))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h24, round((col(FlussoMisureQuartiSchema.e93) + col(FlussoMisureQuartiSchema.e94) + col(FlussoMisureQuartiSchema.e95) + col(FlussoMisureQuartiSchema.e96))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.h25, round((col(FlussoMisureQuartiSchema.e97) + col(FlussoMisureQuartiSchema.e98) + col(FlussoMisureQuartiSchema.e99) + col(FlussoMisureQuartiSchema.e100))*col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPSchema.data_misura, concat_ws("-", col(FlussoMisureQuartiSchema.annoquarti), lpad(col(FlussoMisureQuartiSchema.mesequarti), 2, "0"), lpad(col(FlussoMisureQuartiSchema.giornoquarti), 2, "0")))
      .withColumn(CceCalcoloPSchema.data_calcolo, lit(startDateTime.toString))
      .withColumn(CceCalcoloPSchema.anno, col(FlussoMisureQuartiSchema.annoquarti).cast(StringType))
      .withColumn(CceCalcoloPSchema.mese, col(FlussoMisureQuartiSchema.mesequarti).cast(StringType))
      .withColumn(CceCalcoloPSchema.giorno, col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType))
      .withColumn(CceCalcoloPSchema.nome_file, concat(lit("/"), col(FlussoMisureQuartiSchema.annomesegiornodir).substr(1,4), lit("/"), col(FlussoMisureQuartiSchema.annomesegiornodir).substr(5,8), lit("/"), col(FlussoMisureQuartiSchema.nomefile)))
      .withColumnRenamed(FlussoMisureQuartiSchema.podquarti, CceCalcoloPSchema.pod)
      .selectExpr(CceCalcoloPSchema.getValues:_*)
  }

  def getPeinAggregation (flussoMisureQuartiInDF: DataFrame, dataGhigliottina: DataFrame, annullate1GEin: DataFrame, annullate2GEin: DataFrame, reportAmmissibilita: DataFrame): DataFrame = {

    val logicKey1g: String = "logicKey1g"
    val logicKey2g: String = "logicKey2g"
    val logicKey1gFields = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti))
    val logicKey2gFields = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiInSchema.giornoquarti).cast(StringType), 2, "0"))
    val partitionColumns = Window.partitionBy(col(FlussoMisureQuartiInSchema.annoquarti), col(FlussoMisureQuartiInSchema.mesequarti), col(FlussoMisureQuartiInSchema.giornoquarti), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.podquarti))

    val broadcastAnnullate1g = broadcast(annullate1GEin
      .select(CceMoAnnullate1gEinSchema.k_dt)
      .withColumn(CceMoAnnullate1gEinSchema.k_dt, hash(col(CceMoAnnullate1gEinSchema.k_dt))))

    val broadcastAnnullate2g = broadcast(annullate2GEin
      .select(CceMoAnnullate2gEinSchema.k_dt)
      .withColumn(CceMoAnnullate2gEinSchema.k_dt, hash(col(CceMoAnnullate2gEinSchema.k_dt))))

    flussoMisureQuartiInDF
      .filter(col(FlussoMisureQuartiInSchema.giornoquarti).isNotNull)
      .filter(col(FlussoMisureQuartiInSchema.validato) === "S")
      .filter(!(col(FlussoMisureQuartiInSchema.trattamento_o) === "C" and col(FlussoMisureQuartiInSchema.nomefile).like("%2G%")))
      .filter(!col(FlussoMisureQuartiInSchema.nomefile).like("%RIN%"))
      .join(dataGhigliottina, concat(col(FlussoMisureQuartiInSchema.annoquarti), lpad(col(FlussoMisureQuartiInSchema.mesequarti).cast(StringType), 2, "0")).equalTo(col(DataGhigliottinaSchema.annomese_aggregato)), "left")
      .withColumn(DataGhigliottinaSchema.d_ghigliottina, when(col(DataGhigliottinaSchema.d_ghigliottina).isNull, lit("29991231")).otherwise(col(DataGhigliottinaSchema.d_ghigliottina)))
      .filter(col(FlussoMisureQuartiInSchema.annomesegiornodir) <= col(DataGhigliottinaSchema.d_ghigliottina).cast(IntegerType))
      .selectExpr(FlussoMisureQuartiInSchema.getValues: _*)
      .withColumn(logicKey1g, hash(logicKey1gFields))
      .join(broadcastAnnullate1g
        , col(logicKey1g).equalTo(col(CceMoAnnullate1gEinSchema.k_dt)), joinTypeAnnullamenti)
      .selectExpr(FlussoMisureQuartiInSchema.getValues: _*)
      .withColumn(logicKey2g, hash(logicKey2gFields))
      .join(broadcastAnnullate2g
        , col(logicKey2g).equalTo(col(CceMoAnnullate2gEinSchema.k_dt)), joinTypeAnnullamenti)
      .withColumn("hashKey", hash(concat(upper(col(FlussoMisureQuartiInSchema.nomefile)), substring(col(FlussoMisureQuartiInSchema.podquarti),1,14), split(col(FlussoMisureQuartiInSchema.nomefile),"_")(2), col(FlussoMisureQuartiInSchema.annomesegiornodir))))
      .join(reportAmmissibilita.filter(col(ReportAmmissibilitaPodSchema.flusso).like("%EIN%")).select("hashKey")
        , Seq("hashKey"), joinTypeAmmissibilita)
      .withColumn("row_number", row_number().over(partitionColumns.orderBy(col(FlussoMisureQuartiInSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceCalcoloPeinSchema.h01, round((col(FlussoMisureQuartiInSchema.e1) + col(FlussoMisureQuartiInSchema.e2) + col(FlussoMisureQuartiInSchema.e3) + col(FlussoMisureQuartiInSchema.e4))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h02, round((col(FlussoMisureQuartiInSchema.e5) + col(FlussoMisureQuartiInSchema.e6) + col(FlussoMisureQuartiInSchema.e7) + col(FlussoMisureQuartiInSchema.e8))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h03, round((col(FlussoMisureQuartiInSchema.e9) + col(FlussoMisureQuartiInSchema.e10) + col(FlussoMisureQuartiInSchema.e11) + col(FlussoMisureQuartiInSchema.e12))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h04, round((col(FlussoMisureQuartiInSchema.e13) + col(FlussoMisureQuartiInSchema.e14) + col(FlussoMisureQuartiInSchema.e15) + col(FlussoMisureQuartiInSchema.e16))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h05, round((col(FlussoMisureQuartiInSchema.e17) + col(FlussoMisureQuartiInSchema.e18) + col(FlussoMisureQuartiInSchema.e19) + col(FlussoMisureQuartiInSchema.e20))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h06, round((col(FlussoMisureQuartiInSchema.e21) + col(FlussoMisureQuartiInSchema.e22) + col(FlussoMisureQuartiInSchema.e23) + col(FlussoMisureQuartiInSchema.e24))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h07, round((col(FlussoMisureQuartiInSchema.e25) + col(FlussoMisureQuartiInSchema.e26) + col(FlussoMisureQuartiInSchema.e27) + col(FlussoMisureQuartiInSchema.e28))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h08, round((col(FlussoMisureQuartiInSchema.e29) + col(FlussoMisureQuartiInSchema.e30) + col(FlussoMisureQuartiInSchema.e31) + col(FlussoMisureQuartiInSchema.e32))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h09, round((col(FlussoMisureQuartiInSchema.e33) + col(FlussoMisureQuartiInSchema.e34) + col(FlussoMisureQuartiInSchema.e35) + col(FlussoMisureQuartiInSchema.e36))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h10, round((col(FlussoMisureQuartiInSchema.e37) + col(FlussoMisureQuartiInSchema.e38) + col(FlussoMisureQuartiInSchema.e39) + col(FlussoMisureQuartiInSchema.e40))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h11, round((col(FlussoMisureQuartiInSchema.e41) + col(FlussoMisureQuartiInSchema.e42) + col(FlussoMisureQuartiInSchema.e43) + col(FlussoMisureQuartiInSchema.e44))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h12, round((col(FlussoMisureQuartiInSchema.e45) + col(FlussoMisureQuartiInSchema.e46) + col(FlussoMisureQuartiInSchema.e47) + col(FlussoMisureQuartiInSchema.e48))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h13, round((col(FlussoMisureQuartiInSchema.e49) + col(FlussoMisureQuartiInSchema.e50) + col(FlussoMisureQuartiInSchema.e51) + col(FlussoMisureQuartiInSchema.e52))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h14, round((col(FlussoMisureQuartiInSchema.e53) + col(FlussoMisureQuartiInSchema.e54) + col(FlussoMisureQuartiInSchema.e55) + col(FlussoMisureQuartiInSchema.e56))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h15, round((col(FlussoMisureQuartiInSchema.e57) + col(FlussoMisureQuartiInSchema.e58) + col(FlussoMisureQuartiInSchema.e59) + col(FlussoMisureQuartiInSchema.e60))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h16, round((col(FlussoMisureQuartiInSchema.e61) + col(FlussoMisureQuartiInSchema.e62) + col(FlussoMisureQuartiInSchema.e63) + col(FlussoMisureQuartiInSchema.e64))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h17, round((col(FlussoMisureQuartiInSchema.e65) + col(FlussoMisureQuartiInSchema.e66) + col(FlussoMisureQuartiInSchema.e67) + col(FlussoMisureQuartiInSchema.e68))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h18, round((col(FlussoMisureQuartiInSchema.e69) + col(FlussoMisureQuartiInSchema.e70) + col(FlussoMisureQuartiInSchema.e71) + col(FlussoMisureQuartiInSchema.e72))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h19, round((col(FlussoMisureQuartiInSchema.e73) + col(FlussoMisureQuartiInSchema.e74) + col(FlussoMisureQuartiInSchema.e75) + col(FlussoMisureQuartiInSchema.e76))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h20, round((col(FlussoMisureQuartiInSchema.e77) + col(FlussoMisureQuartiInSchema.e78) + col(FlussoMisureQuartiInSchema.e79) + col(FlussoMisureQuartiInSchema.e80))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h21, round((col(FlussoMisureQuartiInSchema.e81) + col(FlussoMisureQuartiInSchema.e82) + col(FlussoMisureQuartiInSchema.e83) + col(FlussoMisureQuartiInSchema.e84))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h22, round((col(FlussoMisureQuartiInSchema.e85) + col(FlussoMisureQuartiInSchema.e86) + col(FlussoMisureQuartiInSchema.e87) + col(FlussoMisureQuartiInSchema.e88))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h23, round((col(FlussoMisureQuartiInSchema.e89) + col(FlussoMisureQuartiInSchema.e90) + col(FlussoMisureQuartiInSchema.e91) + col(FlussoMisureQuartiInSchema.e92))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h24, round((col(FlussoMisureQuartiInSchema.e93) + col(FlussoMisureQuartiInSchema.e94) + col(FlussoMisureQuartiInSchema.e95) + col(FlussoMisureQuartiInSchema.e96))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.h25, round((col(FlussoMisureQuartiInSchema.e97) + col(FlussoMisureQuartiInSchema.e98) + col(FlussoMisureQuartiInSchema.e99) + col(FlussoMisureQuartiInSchema.e100))*col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPeinSchema.data_misura, concat_ws("-", col(FlussoMisureQuartiInSchema.annoquarti), lpad(col(FlussoMisureQuartiInSchema.mesequarti), 2, "0"), lpad(col(FlussoMisureQuartiInSchema.giornoquarti), 2, "0")))
      .withColumn(CceCalcoloPeinSchema.data_calcolo, lit(startDateTime.toString))
      .withColumn(CceCalcoloPeinSchema.anno, col(FlussoMisureQuartiInSchema.annoquarti).cast(StringType))
      .withColumn(CceCalcoloPeinSchema.mese, col(FlussoMisureQuartiInSchema.mesequarti).cast(StringType))
      .withColumn(CceCalcoloPeinSchema.giorno, col(FlussoMisureQuartiInSchema.giornoquarti).cast(StringType))
      .withColumn(CceCalcoloPeinSchema.nome_file, concat(lit("/"), col(FlussoMisureQuartiInSchema.annomesegiornodir).substr(1,4), lit("/"), col(FlussoMisureQuartiInSchema.annomesegiornodir).substr(5,8), lit("/"), col(FlussoMisureQuartiInSchema.nomefile)))
      .withColumnRenamed(FlussoMisureQuartiInSchema.podquarti, CceCalcoloPeinSchema.pod)
      .selectExpr(CceCalcoloPeinSchema.getValues: _*)
  }

  def getPRMassivoAggregation(flussoMisureQuartiDF: DataFrame, annullate1G: DataFrame, annullate2G: DataFrame, reportAmmissibilita: DataFrame): DataFrame = {

    val logicKey1g: String = "logicKey1g"
    val logicKey2g: String = "logicKey2g"
    val logicKey1gFields = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti))
    val logicKey2gFields = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType), 2, "0"))
    val partitionColumns = Window.partitionBy(col(FlussoMisureQuartiSchema.annoquarti), col(FlussoMisureQuartiSchema.mesequarti), col(FlussoMisureQuartiSchema.giornoquarti), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.podquarti))

    val broadcastAnnullate1g = broadcast(annullate1G
      .select(CceMoAnnullate1gSchema.k_dt)
      .withColumn(CceMoAnnullate1gSchema.k_dt, hash(col(CceMoAnnullate1gSchema.k_dt))))

    val broadcastAnnullate2g = broadcast(annullate2G
      .select(CceMoAnnullate2gSchema.k_dt)
      .withColumn(CceMoAnnullate2gSchema.k_dt, hash(col(CceMoAnnullate2gSchema.k_dt))))

    flussoMisureQuartiDF
      .filter(col(FlussoMisureQuartiSchema.giornoquarti).isNotNull)
      .filter(col(FlussoMisureQuartiSchema.validato) === "S")
      .filter(!(col(FlussoMisureQuartiSchema.trattamento_o) === "C" and col(FlussoMisureQuartiSchema.nomefile).like("%2G%")))
      .withColumn(logicKey1g, hash(logicKey1gFields))
      .join(broadcastAnnullate1g
        , col(logicKey1g).equalTo(col(CceMoAnnullate1gSchema.k_dt)), joinTypeAnnullamenti)
      .selectExpr(FlussoMisureQuartiInSchema.getValues: _*)
      .withColumn(logicKey2g, hash(logicKey2gFields))
      .join(broadcastAnnullate2g
        , col(logicKey2g).equalTo(col(CceMoAnnullate2gSchema.k_dt)), joinTypeAnnullamenti)
      .withColumn("hashKey", hash(concat(upper(col(FlussoMisureQuartiSchema.nomefile)), substring(col(FlussoMisureQuartiSchema.podquarti),1,14), split(col(FlussoMisureQuartiSchema.nomefile),"_")(2), col(FlussoMisureQuartiSchema.annomesegiornodir))))
      .join(reportAmmissibilita.filter(col(ReportAmmissibilitaPodSchema.flusso).like("%PDO%") || col(ReportAmmissibilitaPodSchema.flusso).like("%RFO%")).select("hashKey")
        , Seq("hashKey"), joinTypeAmmissibilita)
      .withColumn("row_number", row_number().over(partitionColumns.orderBy(col(FlussoMisureQuartiSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceCalcoloPRSchema.h01, round((col(FlussoMisureQuartiSchema.e1) + col(FlussoMisureQuartiSchema.e2) + col(FlussoMisureQuartiSchema.e3) + col(FlussoMisureQuartiSchema.e4)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h02, round((col(FlussoMisureQuartiSchema.e5) + col(FlussoMisureQuartiSchema.e6) + col(FlussoMisureQuartiSchema.e7) + col(FlussoMisureQuartiSchema.e8)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h03, round((col(FlussoMisureQuartiSchema.e9) + col(FlussoMisureQuartiSchema.e10) + col(FlussoMisureQuartiSchema.e11) + col(FlussoMisureQuartiSchema.e12)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h04, round((col(FlussoMisureQuartiSchema.e13) + col(FlussoMisureQuartiSchema.e14) + col(FlussoMisureQuartiSchema.e15) + col(FlussoMisureQuartiSchema.e16)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h05, round((col(FlussoMisureQuartiSchema.e17) + col(FlussoMisureQuartiSchema.e18) + col(FlussoMisureQuartiSchema.e19) + col(FlussoMisureQuartiSchema.e20)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h06, round((col(FlussoMisureQuartiSchema.e21) + col(FlussoMisureQuartiSchema.e22) + col(FlussoMisureQuartiSchema.e23) + col(FlussoMisureQuartiSchema.e24)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h07, round((col(FlussoMisureQuartiSchema.e25) + col(FlussoMisureQuartiSchema.e26) + col(FlussoMisureQuartiSchema.e27) + col(FlussoMisureQuartiSchema.e28)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h08, round((col(FlussoMisureQuartiSchema.e29) + col(FlussoMisureQuartiSchema.e30) + col(FlussoMisureQuartiSchema.e31) + col(FlussoMisureQuartiSchema.e32)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h09, round((col(FlussoMisureQuartiSchema.e33) + col(FlussoMisureQuartiSchema.e34) + col(FlussoMisureQuartiSchema.e35) + col(FlussoMisureQuartiSchema.e36)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h10, round((col(FlussoMisureQuartiSchema.e37) + col(FlussoMisureQuartiSchema.e38) + col(FlussoMisureQuartiSchema.e39) + col(FlussoMisureQuartiSchema.e40)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h11, round((col(FlussoMisureQuartiSchema.e41) + col(FlussoMisureQuartiSchema.e42) + col(FlussoMisureQuartiSchema.e43) + col(FlussoMisureQuartiSchema.e44)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h12, round((col(FlussoMisureQuartiSchema.e45) + col(FlussoMisureQuartiSchema.e46) + col(FlussoMisureQuartiSchema.e47) + col(FlussoMisureQuartiSchema.e48)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h13, round((col(FlussoMisureQuartiSchema.e49) + col(FlussoMisureQuartiSchema.e50) + col(FlussoMisureQuartiSchema.e51) + col(FlussoMisureQuartiSchema.e52)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h14, round((col(FlussoMisureQuartiSchema.e53) + col(FlussoMisureQuartiSchema.e54) + col(FlussoMisureQuartiSchema.e55) + col(FlussoMisureQuartiSchema.e56)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h15, round((col(FlussoMisureQuartiSchema.e57) + col(FlussoMisureQuartiSchema.e58) + col(FlussoMisureQuartiSchema.e59) + col(FlussoMisureQuartiSchema.e60)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h16, round((col(FlussoMisureQuartiSchema.e61) + col(FlussoMisureQuartiSchema.e62) + col(FlussoMisureQuartiSchema.e63) + col(FlussoMisureQuartiSchema.e64)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h17, round((col(FlussoMisureQuartiSchema.e65) + col(FlussoMisureQuartiSchema.e66) + col(FlussoMisureQuartiSchema.e67) + col(FlussoMisureQuartiSchema.e68)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h18, round((col(FlussoMisureQuartiSchema.e69) + col(FlussoMisureQuartiSchema.e70) + col(FlussoMisureQuartiSchema.e71) + col(FlussoMisureQuartiSchema.e72)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h19, round((col(FlussoMisureQuartiSchema.e73) + col(FlussoMisureQuartiSchema.e74) + col(FlussoMisureQuartiSchema.e75) + col(FlussoMisureQuartiSchema.e76)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h20, round((col(FlussoMisureQuartiSchema.e77) + col(FlussoMisureQuartiSchema.e78) + col(FlussoMisureQuartiSchema.e79) + col(FlussoMisureQuartiSchema.e80)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h21, round((col(FlussoMisureQuartiSchema.e81) + col(FlussoMisureQuartiSchema.e82) + col(FlussoMisureQuartiSchema.e83) + col(FlussoMisureQuartiSchema.e84)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h22, round((col(FlussoMisureQuartiSchema.e85) + col(FlussoMisureQuartiSchema.e86) + col(FlussoMisureQuartiSchema.e87) + col(FlussoMisureQuartiSchema.e88)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h23, round((col(FlussoMisureQuartiSchema.e89) + col(FlussoMisureQuartiSchema.e90) + col(FlussoMisureQuartiSchema.e91) + col(FlussoMisureQuartiSchema.e92)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h24, round((col(FlussoMisureQuartiSchema.e93) + col(FlussoMisureQuartiSchema.e94) + col(FlussoMisureQuartiSchema.e95) + col(FlussoMisureQuartiSchema.e96)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h25, round((col(FlussoMisureQuartiSchema.e97) + col(FlussoMisureQuartiSchema.e98) + col(FlussoMisureQuartiSchema.e99) + col(FlussoMisureQuartiSchema.e100)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.data_misura, concat_ws("-", col(FlussoMisureQuartiSchema.annoquarti), lpad(col(FlussoMisureQuartiSchema.mesequarti), 2, "0"), lpad(col(FlussoMisureQuartiSchema.giornoquarti), 2, "0")))
      .withColumn(CceCalcoloPRSchema.data_calcolo, lit(startDateTime.toString))
      .withColumn(CceCalcoloPRSchema.anno, col(FlussoMisureQuartiSchema.annoquarti).cast(StringType))
      .withColumn(CceCalcoloPRSchema.mese, col(FlussoMisureQuartiSchema.mesequarti).cast(StringType))
      .withColumn(CceCalcoloPRSchema.giorno, col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType))
      .withColumn(CceCalcoloPRSchema.nome_file, concat(lit("/"), col(FlussoMisureQuartiSchema.annomesegiornodir).substr(1,4), lit("/"), col(FlussoMisureQuartiSchema.annomesegiornodir).substr(5,8), lit("/"), col(FlussoMisureQuartiSchema.nomefile)))
      .withColumnRenamed(FlussoMisureQuartiSchema.podquarti, CceCalcoloPRSchema.pod)
      .selectExpr(CceCalcoloPRSchema.getValues: _*)
  }

  def getPRIncrementaleAggregation(flussoMisureQuartiDF: DataFrame, annullate1G: DataFrame, annullate2G: DataFrame, cceCalcTrackDF: DataFrame, annoIn: Int, reportAmmissibilita: DataFrame): DataFrame = {

    val logicKey1g: String = "logicKey1g"
    val logicKey2g: String = "logicKey2g"
    val logicKey1gFields = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti))
    val logicKey2gFields = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType), 2, "0"))
    val partitionColumns = Window.partitionBy(col(FlussoMisureQuartiSchema.annoquarti), col(FlussoMisureQuartiSchema.mesequarti), col(FlussoMisureQuartiSchema.giornoquarti), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.podquarti))
    val dataLastExecColName = "data_last_exec"
    val pattern: Regex = "\\d+".r

    val dataLastExec = cceCalcTrackDF
      .filter(col(CceCalcTrackSchema.t_tipo_calc) === calcoloPR)
      .filter(col(CceCalcTrackSchema.t_anno_calc) === annoIn.toString)
      .filter(col(CceCalcTrackSchema.t_mese_calc).isNull)
      .filter(col(CceCalcTrackSchema.t_esito) === esitoPositivoCalcolo)
      .orderBy(col(CceCalcTrackSchema.d_data_calc).desc)
      .select(CceCalcTrackSchema.d_data_calc)
      .rdd
      .take(1)
      .mkString
      .trim

    val dataLastExecF = if (dataLastExec.isEmpty)
      null
    else
      pattern.findAllIn(dataLastExec.substring(0, 11)).mkString

    val broadcastAnnullate1g = broadcast(annullate1G
      .select(CceMoAnnullate1gSchema.k_dt)
      .withColumn(CceMoAnnullate1gSchema.k_dt, hash(col(CceMoAnnullate1gSchema.k_dt))))

    val broadcastAnnullate2g = broadcast(annullate2G
      .select(CceMoAnnullate2gSchema.k_dt)
      .withColumn(CceMoAnnullate2gSchema.k_dt, hash(col(CceMoAnnullate2gSchema.k_dt))))

    flussoMisureQuartiDF
      .withColumn(dataLastExecColName, trim(lit(dataLastExecF)))
      .withColumn(dataLastExecColName, when(col(dataLastExecColName).isNotNull, col(dataLastExecColName).cast(IntegerType)).otherwise(lit(19000101)))
      .filter(col(FlussoMisureQuartiSchema.giornoquarti).isNotNull)
      .filter(col(FlussoMisureQuartiSchema.validato) === "S")
      .filter(!(col(FlussoMisureQuartiSchema.trattamento_o) === "C" and col(FlussoMisureQuartiSchema.nomefile).like("%2G%")))
      .filter(col(FlussoMisureQuartiSchema.annomesegiornodir) >= col(dataLastExecColName))
      .withColumn(logicKey1g, hash(logicKey1gFields))
      .join(broadcastAnnullate1g
        , col(logicKey1g).equalTo(col(CceMoAnnullate1gSchema.k_dt)), joinTypeAnnullamenti)
      .selectExpr(FlussoMisureQuartiInSchema.getValues: _*)
      .withColumn(logicKey2g, hash(logicKey2gFields))
      .join(broadcastAnnullate2g
        , col(logicKey2g).equalTo(col(CceMoAnnullate2gSchema.k_dt)), joinTypeAnnullamenti)
      .withColumn("hashKey", hash(concat(upper(col(FlussoMisureQuartiSchema.nomefile)), substring(col(FlussoMisureQuartiSchema.podquarti),1,14), split(col(FlussoMisureQuartiSchema.nomefile),"_")(2), col(FlussoMisureQuartiSchema.annomesegiornodir))))
      .join(reportAmmissibilita.filter(col(ReportAmmissibilitaPodSchema.flusso).like("%PDO%") || col(ReportAmmissibilitaPodSchema.flusso).like("%RFO%")).select("hashKey")
        , Seq("hashKey"), joinTypeAmmissibilita)
      .withColumn("row_number", row_number().over(partitionColumns.orderBy(col(FlussoMisureQuartiSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceCalcoloPRSchema.h01, round((col(FlussoMisureQuartiSchema.e1) + col(FlussoMisureQuartiSchema.e2) + col(FlussoMisureQuartiSchema.e3) + col(FlussoMisureQuartiSchema.e4)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h02, round((col(FlussoMisureQuartiSchema.e5) + col(FlussoMisureQuartiSchema.e6) + col(FlussoMisureQuartiSchema.e7) + col(FlussoMisureQuartiSchema.e8)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h03, round((col(FlussoMisureQuartiSchema.e9) + col(FlussoMisureQuartiSchema.e10) + col(FlussoMisureQuartiSchema.e11) + col(FlussoMisureQuartiSchema.e12)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h04, round((col(FlussoMisureQuartiSchema.e13) + col(FlussoMisureQuartiSchema.e14) + col(FlussoMisureQuartiSchema.e15) + col(FlussoMisureQuartiSchema.e16)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h05, round((col(FlussoMisureQuartiSchema.e17) + col(FlussoMisureQuartiSchema.e18) + col(FlussoMisureQuartiSchema.e19) + col(FlussoMisureQuartiSchema.e20)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h06, round((col(FlussoMisureQuartiSchema.e21) + col(FlussoMisureQuartiSchema.e22) + col(FlussoMisureQuartiSchema.e23) + col(FlussoMisureQuartiSchema.e24)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h07, round((col(FlussoMisureQuartiSchema.e25) + col(FlussoMisureQuartiSchema.e26) + col(FlussoMisureQuartiSchema.e27) + col(FlussoMisureQuartiSchema.e28)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h08, round((col(FlussoMisureQuartiSchema.e29) + col(FlussoMisureQuartiSchema.e30) + col(FlussoMisureQuartiSchema.e31) + col(FlussoMisureQuartiSchema.e32)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h09, round((col(FlussoMisureQuartiSchema.e33) + col(FlussoMisureQuartiSchema.e34) + col(FlussoMisureQuartiSchema.e35) + col(FlussoMisureQuartiSchema.e36)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h10, round((col(FlussoMisureQuartiSchema.e37) + col(FlussoMisureQuartiSchema.e38) + col(FlussoMisureQuartiSchema.e39) + col(FlussoMisureQuartiSchema.e40)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h11, round((col(FlussoMisureQuartiSchema.e41) + col(FlussoMisureQuartiSchema.e42) + col(FlussoMisureQuartiSchema.e43) + col(FlussoMisureQuartiSchema.e44)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h12, round((col(FlussoMisureQuartiSchema.e45) + col(FlussoMisureQuartiSchema.e46) + col(FlussoMisureQuartiSchema.e47) + col(FlussoMisureQuartiSchema.e48)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h13, round((col(FlussoMisureQuartiSchema.e49) + col(FlussoMisureQuartiSchema.e50) + col(FlussoMisureQuartiSchema.e51) + col(FlussoMisureQuartiSchema.e52)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h14, round((col(FlussoMisureQuartiSchema.e53) + col(FlussoMisureQuartiSchema.e54) + col(FlussoMisureQuartiSchema.e55) + col(FlussoMisureQuartiSchema.e56)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h15, round((col(FlussoMisureQuartiSchema.e57) + col(FlussoMisureQuartiSchema.e58) + col(FlussoMisureQuartiSchema.e59) + col(FlussoMisureQuartiSchema.e60)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h16, round((col(FlussoMisureQuartiSchema.e61) + col(FlussoMisureQuartiSchema.e62) + col(FlussoMisureQuartiSchema.e63) + col(FlussoMisureQuartiSchema.e64)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h17, round((col(FlussoMisureQuartiSchema.e65) + col(FlussoMisureQuartiSchema.e66) + col(FlussoMisureQuartiSchema.e67) + col(FlussoMisureQuartiSchema.e68)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h18, round((col(FlussoMisureQuartiSchema.e69) + col(FlussoMisureQuartiSchema.e70) + col(FlussoMisureQuartiSchema.e71) + col(FlussoMisureQuartiSchema.e72)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h19, round((col(FlussoMisureQuartiSchema.e73) + col(FlussoMisureQuartiSchema.e74) + col(FlussoMisureQuartiSchema.e75) + col(FlussoMisureQuartiSchema.e76)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h20, round((col(FlussoMisureQuartiSchema.e77) + col(FlussoMisureQuartiSchema.e78) + col(FlussoMisureQuartiSchema.e79) + col(FlussoMisureQuartiSchema.e80)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h21, round((col(FlussoMisureQuartiSchema.e81) + col(FlussoMisureQuartiSchema.e82) + col(FlussoMisureQuartiSchema.e83) + col(FlussoMisureQuartiSchema.e84)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h22, round((col(FlussoMisureQuartiSchema.e85) + col(FlussoMisureQuartiSchema.e86) + col(FlussoMisureQuartiSchema.e87) + col(FlussoMisureQuartiSchema.e88)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h23, round((col(FlussoMisureQuartiSchema.e89) + col(FlussoMisureQuartiSchema.e90) + col(FlussoMisureQuartiSchema.e91) + col(FlussoMisureQuartiSchema.e92)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h24, round((col(FlussoMisureQuartiSchema.e93) + col(FlussoMisureQuartiSchema.e94) + col(FlussoMisureQuartiSchema.e95) + col(FlussoMisureQuartiSchema.e96)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.h25, round((col(FlussoMisureQuartiSchema.e97) + col(FlussoMisureQuartiSchema.e98) + col(FlussoMisureQuartiSchema.e99) + col(FlussoMisureQuartiSchema.e100)) * col(FlussoMisureQuartiSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPRSchema.data_misura, concat_ws("-", col(FlussoMisureQuartiSchema.annoquarti), lpad(col(FlussoMisureQuartiSchema.mesequarti), 2, "0"), lpad(col(FlussoMisureQuartiSchema.giornoquarti), 2, "0")))
      .withColumn(CceCalcoloPRSchema.data_calcolo, lit(startDateTime.toString))
      .withColumn(CceCalcoloPRSchema.anno, col(FlussoMisureQuartiSchema.annoquarti).cast(StringType))
      .withColumn(CceCalcoloPRSchema.mese, col(FlussoMisureQuartiSchema.mesequarti).cast(StringType))
      .withColumn(CceCalcoloPRSchema.giorno, col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType))
      .withColumn(CceCalcoloPRSchema.nome_file, concat(lit("/"), col(FlussoMisureQuartiSchema.annomesegiornodir).substr(1,4), lit("/"), col(FlussoMisureQuartiSchema.annomesegiornodir).substr(5,8), lit("/"), col(FlussoMisureQuartiSchema.nomefile)))
      .withColumnRenamed(FlussoMisureQuartiSchema.podquarti, CceCalcoloPRSchema.pod)
      .selectExpr(CceCalcoloPRSchema.getValues: _*)
  }

  def getPReinMassivoAggregation(flussoMisureQuartiInDF: DataFrame, annullate1GEin: DataFrame, annullate2GEin: DataFrame, reportAmmissibilita: DataFrame): DataFrame = {

    val logicKey1g: String = "logicKey1g"
    val logicKey2g: String = "logicKey2g"
    val logicKey1gFields = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti))
    val logicKey2gFields = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType), 2, "0"))
    val partitionColumns = Window.partitionBy(col(FlussoMisureQuartiInSchema.annoquarti), col(FlussoMisureQuartiInSchema.mesequarti), col(FlussoMisureQuartiInSchema.giornoquarti), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.podquarti))

    val broadcastAnnullate1g = broadcast(annullate1GEin
      .select(CceMoAnnullate1gEinSchema.k_dt)
      .withColumn(CceMoAnnullate1gEinSchema.k_dt, hash(col(CceMoAnnullate1gEinSchema.k_dt))))

    val broadcastAnnullate2g = broadcast(annullate2GEin
      .select(CceMoAnnullate2gEinSchema.k_dt)
      .withColumn(CceMoAnnullate2gEinSchema.k_dt, hash(col(CceMoAnnullate2gEinSchema.k_dt))))

    flussoMisureQuartiInDF
      .filter(col(FlussoMisureQuartiInSchema.giornoquarti).isNotNull)
      .filter(col(FlussoMisureQuartiInSchema.validato) === "S")
      .filter(!(col(FlussoMisureQuartiInSchema.trattamento_o) === "C" and col(FlussoMisureQuartiInSchema.nomefile).like("%2G%")))
      .withColumn(logicKey1g, hash(logicKey1gFields))
      .join(broadcastAnnullate1g
        , col(logicKey1g).equalTo(col(CceMoAnnullate1gEinSchema.k_dt)), joinTypeAnnullamenti)
      .selectExpr(FlussoMisureQuartiInSchema.getValues:_*)
      .withColumn(logicKey2g, hash(logicKey2gFields))
      .join(broadcastAnnullate2g
        , col(logicKey2g).equalTo(col(CceMoAnnullate2gEinSchema.k_dt)), joinTypeAnnullamenti)
      .withColumn("hashKey", hash(concat(upper(col(FlussoMisureQuartiInSchema.nomefile)), substring(col(FlussoMisureQuartiInSchema.podquarti),1,14), split(col(FlussoMisureQuartiInSchema.nomefile),"_")(2), col(FlussoMisureQuartiInSchema.annomesegiornodir))))
      .join(reportAmmissibilita.filter(col(ReportAmmissibilitaPodSchema.flusso).like("%EIN%") || col(ReportAmmissibilitaPodSchema.flusso).like("%RIN%")).select("hashKey")
        , Seq("hashKey"), joinTypeAmmissibilita)
      .withColumn("row_number", row_number().over(partitionColumns.orderBy(col(FlussoMisureQuartiInSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceCalcoloPReinSchema.h01, round((col(FlussoMisureQuartiInSchema.e1) + col(FlussoMisureQuartiInSchema.e2) + col(FlussoMisureQuartiInSchema.e3) + col(FlussoMisureQuartiInSchema.e4)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h02, round((col(FlussoMisureQuartiInSchema.e5) + col(FlussoMisureQuartiInSchema.e6) + col(FlussoMisureQuartiInSchema.e7) + col(FlussoMisureQuartiInSchema.e8)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h03, round((col(FlussoMisureQuartiInSchema.e9) + col(FlussoMisureQuartiInSchema.e10) + col(FlussoMisureQuartiInSchema.e11) + col(FlussoMisureQuartiInSchema.e12)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h04, round((col(FlussoMisureQuartiInSchema.e13) + col(FlussoMisureQuartiInSchema.e14) + col(FlussoMisureQuartiInSchema.e15) + col(FlussoMisureQuartiInSchema.e16)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h05, round((col(FlussoMisureQuartiInSchema.e17) + col(FlussoMisureQuartiInSchema.e18) + col(FlussoMisureQuartiInSchema.e19) + col(FlussoMisureQuartiInSchema.e20)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h06, round((col(FlussoMisureQuartiInSchema.e21) + col(FlussoMisureQuartiInSchema.e22) + col(FlussoMisureQuartiInSchema.e23) + col(FlussoMisureQuartiInSchema.e24)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h07, round((col(FlussoMisureQuartiInSchema.e25) + col(FlussoMisureQuartiInSchema.e26) + col(FlussoMisureQuartiInSchema.e27) + col(FlussoMisureQuartiInSchema.e28)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h08, round((col(FlussoMisureQuartiInSchema.e29) + col(FlussoMisureQuartiInSchema.e30) + col(FlussoMisureQuartiInSchema.e31) + col(FlussoMisureQuartiInSchema.e32)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h09, round((col(FlussoMisureQuartiInSchema.e33) + col(FlussoMisureQuartiInSchema.e34) + col(FlussoMisureQuartiInSchema.e35) + col(FlussoMisureQuartiInSchema.e36)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h10, round((col(FlussoMisureQuartiInSchema.e37) + col(FlussoMisureQuartiInSchema.e38) + col(FlussoMisureQuartiInSchema.e39) + col(FlussoMisureQuartiInSchema.e40)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h11, round((col(FlussoMisureQuartiInSchema.e41) + col(FlussoMisureQuartiInSchema.e42) + col(FlussoMisureQuartiInSchema.e43) + col(FlussoMisureQuartiInSchema.e44)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h12, round((col(FlussoMisureQuartiInSchema.e45) + col(FlussoMisureQuartiInSchema.e46) + col(FlussoMisureQuartiInSchema.e47) + col(FlussoMisureQuartiInSchema.e48)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h13, round((col(FlussoMisureQuartiInSchema.e49) + col(FlussoMisureQuartiInSchema.e50) + col(FlussoMisureQuartiInSchema.e51) + col(FlussoMisureQuartiInSchema.e52)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h14, round((col(FlussoMisureQuartiInSchema.e53) + col(FlussoMisureQuartiInSchema.e54) + col(FlussoMisureQuartiInSchema.e55) + col(FlussoMisureQuartiInSchema.e56)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h15, round((col(FlussoMisureQuartiInSchema.e57) + col(FlussoMisureQuartiInSchema.e58) + col(FlussoMisureQuartiInSchema.e59) + col(FlussoMisureQuartiInSchema.e60)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h16, round((col(FlussoMisureQuartiInSchema.e61) + col(FlussoMisureQuartiInSchema.e62) + col(FlussoMisureQuartiInSchema.e63) + col(FlussoMisureQuartiInSchema.e64)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h17, round((col(FlussoMisureQuartiInSchema.e65) + col(FlussoMisureQuartiInSchema.e66) + col(FlussoMisureQuartiInSchema.e67) + col(FlussoMisureQuartiInSchema.e68)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h18, round((col(FlussoMisureQuartiInSchema.e69) + col(FlussoMisureQuartiInSchema.e70) + col(FlussoMisureQuartiInSchema.e71) + col(FlussoMisureQuartiInSchema.e72)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h19, round((col(FlussoMisureQuartiInSchema.e73) + col(FlussoMisureQuartiInSchema.e74) + col(FlussoMisureQuartiInSchema.e75) + col(FlussoMisureQuartiInSchema.e76)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h20, round((col(FlussoMisureQuartiInSchema.e77) + col(FlussoMisureQuartiInSchema.e78) + col(FlussoMisureQuartiInSchema.e79) + col(FlussoMisureQuartiInSchema.e80)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h21, round((col(FlussoMisureQuartiInSchema.e81) + col(FlussoMisureQuartiInSchema.e82) + col(FlussoMisureQuartiInSchema.e83) + col(FlussoMisureQuartiInSchema.e84)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h22, round((col(FlussoMisureQuartiInSchema.e85) + col(FlussoMisureQuartiInSchema.e86) + col(FlussoMisureQuartiInSchema.e87) + col(FlussoMisureQuartiInSchema.e88)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h23, round((col(FlussoMisureQuartiInSchema.e89) + col(FlussoMisureQuartiInSchema.e90) + col(FlussoMisureQuartiInSchema.e91) + col(FlussoMisureQuartiInSchema.e92)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h24, round((col(FlussoMisureQuartiInSchema.e93) + col(FlussoMisureQuartiInSchema.e94) + col(FlussoMisureQuartiInSchema.e95) + col(FlussoMisureQuartiInSchema.e96)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h25, round((col(FlussoMisureQuartiInSchema.e97) + col(FlussoMisureQuartiInSchema.e98) + col(FlussoMisureQuartiInSchema.e99) + col(FlussoMisureQuartiInSchema.e100)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.data_misura, concat_ws("-", col(FlussoMisureQuartiInSchema.annoquarti), lpad(col(FlussoMisureQuartiInSchema.mesequarti), 2, "0"), lpad(col(FlussoMisureQuartiInSchema.giornoquarti), 2, "0")))
      .withColumn(CceCalcoloPReinSchema.data_calcolo, lit(startDateTime.toString))
      .withColumn(CceCalcoloPReinSchema.anno, col(FlussoMisureQuartiInSchema.annoquarti).cast(StringType))
      .withColumn(CceCalcoloPReinSchema.mese, col(FlussoMisureQuartiInSchema.mesequarti).cast(StringType))
      .withColumn(CceCalcoloPReinSchema.giorno, col(FlussoMisureQuartiInSchema.giornoquarti).cast(StringType))
      .withColumn(CceCalcoloPReinSchema.nome_file, concat(lit("/"), col(FlussoMisureQuartiInSchema.annomesegiornodir).substr(1,4), lit("/"), col(FlussoMisureQuartiInSchema.annomesegiornodir).substr(5,8), lit("/"), col(FlussoMisureQuartiInSchema.nomefile)))
      .withColumnRenamed(FlussoMisureQuartiInSchema.podquarti, CceCalcoloPReinSchema.pod)
      .selectExpr(CceCalcoloPReinSchema.getValues: _*)
  }

  def getPReinIncrementaleAggregation(flussoMisureQuartiInDF: DataFrame, annullate1GEin: DataFrame, annullate2GEin: DataFrame, cceCalcTrackDF: DataFrame, annoIn: Int, reportAmmissibilita: DataFrame): DataFrame = {

    val logicKey1g: String = "logicKey1g"
    val logicKey2g: String = "logicKey2g"
    val logicKey1gFields = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti))
    val logicKey2gFields = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiInSchema.giornoquarti).cast(StringType), 2, "0"))
    val partitionColumns = Window.partitionBy(col(FlussoMisureQuartiInSchema.annoquarti), col(FlussoMisureQuartiInSchema.mesequarti), col(FlussoMisureQuartiInSchema.giornoquarti), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.podquarti))
    val dataLastExecColName = "data_last_exec"
    val pattern: Regex = "\\d+".r

    val dataLastExec = cceCalcTrackDF
      .filter(col(CceCalcTrackSchema.t_tipo_calc) === calcoloPRein)
      .filter(col(CceCalcTrackSchema.t_anno_calc) === annoIn.toString)
      .filter(col(CceCalcTrackSchema.t_mese_calc).isNull)
      .filter(col(CceCalcTrackSchema.t_esito) === esitoPositivoCalcolo)
      .orderBy(col(CceCalcTrackSchema.d_data_calc).desc)
      .select(CceCalcTrackSchema.d_data_calc)
      .rdd
      .take(1)
      .mkString
      .trim

    val dataLastExecF = if (dataLastExec.isEmpty)
      null
    else
      pattern.findAllIn(dataLastExec.substring(0,11)).mkString

    val broadcastAnnullate1g = broadcast(annullate1GEin
      .select(CceMoAnnullate1gEinSchema.k_dt)
      .withColumn(CceMoAnnullate1gEinSchema.k_dt, hash(col(CceMoAnnullate1gEinSchema.k_dt))))

    val broadcastAnnullate2g = broadcast(annullate2GEin
      .select(CceMoAnnullate2gEinSchema.k_dt)
      .withColumn(CceMoAnnullate2gEinSchema.k_dt, hash(col(CceMoAnnullate2gEinSchema.k_dt))))

    flussoMisureQuartiInDF
      .withColumn(dataLastExecColName, trim(lit(dataLastExecF)))
      .withColumn(dataLastExecColName, when(col(dataLastExecColName).isNotNull, col(dataLastExecColName).cast(IntegerType)).otherwise(lit(19000101)))
      .filter(col(FlussoMisureQuartiInSchema.giornoquarti).isNotNull)
      .filter(col(FlussoMisureQuartiInSchema.validato) === "S")
      .filter(!(col(FlussoMisureQuartiInSchema.trattamento_o) === "C" and col(FlussoMisureQuartiInSchema.nomefile).like("%2G%")))
      .filter(col(FlussoMisureQuartiInSchema.annomesegiornodir) >= col(dataLastExecColName))
      .withColumn(logicKey1g, hash(logicKey1gFields))
      .join(broadcastAnnullate1g
        , col(logicKey1g).equalTo(col(CceMoAnnullate1gEinSchema.k_dt)), joinTypeAnnullamenti)
      .selectExpr(FlussoMisureQuartiInSchema.getValues: _*)
      .withColumn(logicKey2g, hash(logicKey2gFields))
      .join(broadcastAnnullate2g
        , col(logicKey2g).equalTo(col(CceMoAnnullate2gEinSchema.k_dt)), joinTypeAnnullamenti)
      .withColumn("hashKey", hash(concat(upper(col(FlussoMisureQuartiInSchema.nomefile)), substring(col(FlussoMisureQuartiInSchema.podquarti),1,14), split(col(FlussoMisureQuartiInSchema.nomefile),"_")(2), col(FlussoMisureQuartiInSchema.annomesegiornodir))))
      .join(reportAmmissibilita.filter(col(ReportAmmissibilitaPodSchema.flusso).like("%EIN%") || col(ReportAmmissibilitaPodSchema.flusso).like("%RIN%")).select("hashKey")
        , Seq("hashKey"), joinTypeAmmissibilita)
      .withColumn("row_number", row_number().over(partitionColumns.orderBy(col(FlussoMisureQuartiInSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceCalcoloPReinSchema.h01, round((col(FlussoMisureQuartiInSchema.e1) + col(FlussoMisureQuartiInSchema.e2) + col(FlussoMisureQuartiInSchema.e3) + col(FlussoMisureQuartiInSchema.e4)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h02, round((col(FlussoMisureQuartiInSchema.e5) + col(FlussoMisureQuartiInSchema.e6) + col(FlussoMisureQuartiInSchema.e7) + col(FlussoMisureQuartiInSchema.e8)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h03, round((col(FlussoMisureQuartiInSchema.e9) + col(FlussoMisureQuartiInSchema.e10) + col(FlussoMisureQuartiInSchema.e11) + col(FlussoMisureQuartiInSchema.e12)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h04, round((col(FlussoMisureQuartiInSchema.e13) + col(FlussoMisureQuartiInSchema.e14) + col(FlussoMisureQuartiInSchema.e15) + col(FlussoMisureQuartiInSchema.e16)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h05, round((col(FlussoMisureQuartiInSchema.e17) + col(FlussoMisureQuartiInSchema.e18) + col(FlussoMisureQuartiInSchema.e19) + col(FlussoMisureQuartiInSchema.e20)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h06, round((col(FlussoMisureQuartiInSchema.e21) + col(FlussoMisureQuartiInSchema.e22) + col(FlussoMisureQuartiInSchema.e23) + col(FlussoMisureQuartiInSchema.e24)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h07, round((col(FlussoMisureQuartiInSchema.e25) + col(FlussoMisureQuartiInSchema.e26) + col(FlussoMisureQuartiInSchema.e27) + col(FlussoMisureQuartiInSchema.e28)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h08, round((col(FlussoMisureQuartiInSchema.e29) + col(FlussoMisureQuartiInSchema.e30) + col(FlussoMisureQuartiInSchema.e31) + col(FlussoMisureQuartiInSchema.e32)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h09, round((col(FlussoMisureQuartiInSchema.e33) + col(FlussoMisureQuartiInSchema.e34) + col(FlussoMisureQuartiInSchema.e35) + col(FlussoMisureQuartiInSchema.e36)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h10, round((col(FlussoMisureQuartiInSchema.e37) + col(FlussoMisureQuartiInSchema.e38) + col(FlussoMisureQuartiInSchema.e39) + col(FlussoMisureQuartiInSchema.e40)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h11, round((col(FlussoMisureQuartiInSchema.e41) + col(FlussoMisureQuartiInSchema.e42) + col(FlussoMisureQuartiInSchema.e43) + col(FlussoMisureQuartiInSchema.e44)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h12, round((col(FlussoMisureQuartiInSchema.e45) + col(FlussoMisureQuartiInSchema.e46) + col(FlussoMisureQuartiInSchema.e47) + col(FlussoMisureQuartiInSchema.e48)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h13, round((col(FlussoMisureQuartiInSchema.e49) + col(FlussoMisureQuartiInSchema.e50) + col(FlussoMisureQuartiInSchema.e51) + col(FlussoMisureQuartiInSchema.e52)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h14, round((col(FlussoMisureQuartiInSchema.e53) + col(FlussoMisureQuartiInSchema.e54) + col(FlussoMisureQuartiInSchema.e55) + col(FlussoMisureQuartiInSchema.e56)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h15, round((col(FlussoMisureQuartiInSchema.e57) + col(FlussoMisureQuartiInSchema.e58) + col(FlussoMisureQuartiInSchema.e59) + col(FlussoMisureQuartiInSchema.e60)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h16, round((col(FlussoMisureQuartiInSchema.e61) + col(FlussoMisureQuartiInSchema.e62) + col(FlussoMisureQuartiInSchema.e63) + col(FlussoMisureQuartiInSchema.e64)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h17, round((col(FlussoMisureQuartiInSchema.e65) + col(FlussoMisureQuartiInSchema.e66) + col(FlussoMisureQuartiInSchema.e67) + col(FlussoMisureQuartiInSchema.e68)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h18, round((col(FlussoMisureQuartiInSchema.e69) + col(FlussoMisureQuartiInSchema.e70) + col(FlussoMisureQuartiInSchema.e71) + col(FlussoMisureQuartiInSchema.e72)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h19, round((col(FlussoMisureQuartiInSchema.e73) + col(FlussoMisureQuartiInSchema.e74) + col(FlussoMisureQuartiInSchema.e75) + col(FlussoMisureQuartiInSchema.e76)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h20, round((col(FlussoMisureQuartiInSchema.e77) + col(FlussoMisureQuartiInSchema.e78) + col(FlussoMisureQuartiInSchema.e79) + col(FlussoMisureQuartiInSchema.e80)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h21, round((col(FlussoMisureQuartiInSchema.e81) + col(FlussoMisureQuartiInSchema.e82) + col(FlussoMisureQuartiInSchema.e83) + col(FlussoMisureQuartiInSchema.e84)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h22, round((col(FlussoMisureQuartiInSchema.e85) + col(FlussoMisureQuartiInSchema.e86) + col(FlussoMisureQuartiInSchema.e87) + col(FlussoMisureQuartiInSchema.e88)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h23, round((col(FlussoMisureQuartiInSchema.e89) + col(FlussoMisureQuartiInSchema.e90) + col(FlussoMisureQuartiInSchema.e91) + col(FlussoMisureQuartiInSchema.e92)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h24, round((col(FlussoMisureQuartiInSchema.e93) + col(FlussoMisureQuartiInSchema.e94) + col(FlussoMisureQuartiInSchema.e95) + col(FlussoMisureQuartiInSchema.e96)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.h25, round((col(FlussoMisureQuartiInSchema.e97) + col(FlussoMisureQuartiInSchema.e98) + col(FlussoMisureQuartiInSchema.e99) + col(FlussoMisureQuartiInSchema.e100)) * col(FlussoMisureQuartiInSchema.perdita).plus(1), 6))
      .withColumn(CceCalcoloPReinSchema.data_misura, concat_ws("-", col(FlussoMisureQuartiInSchema.annoquarti), lpad(col(FlussoMisureQuartiInSchema.mesequarti), 2, "0"), lpad(col(FlussoMisureQuartiInSchema.giornoquarti), 2, "0")))
      .withColumn(CceCalcoloPReinSchema.data_calcolo, lit(startDateTime.toString))
      .withColumn(CceCalcoloPReinSchema.anno, col(FlussoMisureQuartiInSchema.annoquarti).cast(StringType))
      .withColumn(CceCalcoloPReinSchema.mese, col(FlussoMisureQuartiInSchema.mesequarti).cast(StringType))
      .withColumn(CceCalcoloPReinSchema.giorno, col(FlussoMisureQuartiInSchema.giornoquarti).cast(StringType))
      .withColumn(CceCalcoloPReinSchema.nome_file, concat(lit("/"), col(FlussoMisureQuartiInSchema.annomesegiornodir).substr(1,4), lit("/"), col(FlussoMisureQuartiInSchema.annomesegiornodir).substr(5,8), lit("/"), col(FlussoMisureQuartiInSchema.nomefile)))
      .withColumnRenamed(FlussoMisureQuartiInSchema.podquarti, CceCalcoloPReinSchema.pod)
      .selectExpr(CceCalcoloPReinSchema.getValues: _*)
  }

  def getAnagrafica(rcuFornituraP: DataFrame, rcusFornituraP: DataFrame, rcuTariffaP: DataFrame, rcuPodDistrP: DataFrame, rcuAziendaP: DataFrame, rcuPodTecnP: DataFrame, rcuPodUddP: DataFrame, rcusPodUddP: DataFrame, rcuUddP: DataFrame, rcuPodP: DataFrame, rcusPodP: DataFrame): DataFrame = {
    val window = Window.partitionBy(col(RcuFornituraPSchema.n_id_pod))

    val pod = rcuPodP
      .unionByName(rcusPodP)
      .withColumn("row_number", row_number().over(window.orderBy(col(RcusPodPSchema.d_aggiornamento).desc)))
      .filter(col("row_number") === 1)
      .drop("row_number", RcusPodPSchema.d_aggiornamento)

    val podTariffa = rcuFornituraP.select(RcuFornituraPSchema.n_id_pod, RcuFornituraPSchema.n_id_fornitura, RcuFornituraPSchema.d_fine_titolarita)
      .unionByName(rcusFornituraP.select(RcusFornituraPSchema.n_id_pod, RcusFornituraPSchema.n_id_fornitura, RcusFornituraPSchema.d_fine_titolarita))
      .join(rcuTariffaP.select(RcuTariffaPSchema.n_id_fornitura, RcuTariffaPSchema.t_tariffa_distr, RcuTariffaPSchema.d_inizio_tariffa, RcuTariffaPSchema.d_fine_tariffa),
        Seq(RcuFornituraPSchema.n_id_fornitura.toString),
        "inner")
      .filter(to_date(col(RcuFornituraPSchema.d_fine_titolarita)).between(to_date(col(RcuTariffaPSchema.d_inizio_tariffa)), to_date(col(RcuTariffaPSchema.d_fine_tariffa))))
      .withColumn("row_number", row_number().over(window.orderBy(to_date(col(RcuFornituraPSchema.d_fine_titolarita)).desc)))
      .filter(col("row_number") === 1)
      .select(RcuFornituraPSchema.n_id_pod, RcuTariffaPSchema.t_tariffa_distr)

    val podUdd = rcuPodUddP.select(RcuPodUddPSchema.n_id_pod, RcuPodUddPSchema.d_inizio, RcuPodUddPSchema.d_fine, RcusPodUddPSchema.n_id_udd)
      .unionByName(rcusPodUddP.select(RcusPodUddPSchema.n_id_pod, RcusPodUddPSchema.d_inizio, RcusPodUddPSchema.d_fine, RcusPodUddPSchema.n_id_udd))
      .join(rcuUddP.select(RcuUddPSchema.n_id_udd, RcuUddPSchema.t_codice_terna), Seq(RcuUddPSchema.n_id_udd.toString), "inner")
      .join(rcuAziendaP.select(RcuAziendaPSchema.n_id_azienda, RcuAziendaPSchema.t_piva), col(RcuAziendaPSchema.n_id_azienda).equalTo(col(RcuPodUddPSchema.n_id_udd)), "inner")
      .withColumnRenamed(RcuAziendaPSchema.t_piva, CceCalcoloAnagraficaSchema.t_piva_udd)
      .drop(RcuAziendaPSchema.n_id_azienda)

    val anagrafica = pod.withColumn(RcuPodPSchema.t_codice_pod, substring(col(RcuPodPSchema.t_codice_pod), 1, 14))
      .join(podTariffa, Seq(RcuPodPSchema.n_id_pod.toString), "inner")
      .join(podUdd, Seq(RcuPodPSchema.n_id_pod.toString), "inner")
      .join(rcuPodTecnP.select(RcuPodTecnPSchema.n_id_pod, RcuPodTecnPSchema.n_tensione, RcuPodTecnPSchema.t_tipo_pod), Seq(RcuPodTecnPSchema.n_id_pod.toString), "inner")
      .join(rcuPodDistrP.select(RcuPodDistrPSchema.n_id_pod, RcuPodDistrPSchema.n_id_distr), Seq(RcuPodDistrPSchema.n_id_pod.toString), "inner")
      .join(rcuAziendaP.select(RcuAziendaPSchema.n_id_azienda, RcuAziendaPSchema.t_piva), col(RcuPodDistrPSchema.n_id_distr).equalTo(col(RcuAziendaPSchema.n_id_azienda)), "inner")
      .withColumnRenamed(RcuPodPSchema.t_codice_pod, CceCalcoloAnagraficaSchema.t_codice_pod)
      .withColumnRenamed(RcuPodPSchema.t_area_rif, CceCalcoloAnagraficaSchema.t_area_rif)
      .withColumnRenamed(RcuPodTecnPSchema.n_tensione, CceCalcoloAnagraficaSchema.n_tensione)
      .withColumnRenamed(RcuUddPSchema.t_codice_terna, CceCalcoloAnagraficaSchema.t_codice_terna)
      .withColumnRenamed(RcuTariffaPSchema.t_tariffa_distr, CceCalcoloAnagraficaSchema.t_tariffa_distr)
      .withColumn(RcuPodTecnPSchema.t_tipo_pod, coalesce(col(CceCalcoloAnagraficaSchema.t_tipo_pod), lit("06")))
      .withColumn(CceCalcoloAnagraficaSchema.d_inizio_udd, col(RcuPodUddPSchema.d_inizio).cast(StringType).substr(1,10))
      .withColumn(CceCalcoloAnagraficaSchema.d_fine_udd, col(RcuPodUddPSchema.d_fine).cast(StringType).substr(1,10))
      .withColumn(CceCalcoloAnagraficaSchema.t_piva_id, col(RcuAziendaPSchema.t_piva))
      .withColumn(CceCalcoloAnagraficaSchema.t_tensione,
        when(col(CceCalcoloAnagraficaSchema.n_tensione) <= 1000, lit("BASSA"))
          .when((col(CceCalcoloAnagraficaSchema.n_tensione) <= 35000) and (col(CceCalcoloAnagraficaSchema.n_tensione) > 1000), lit("MEDIA"))
          .when((col(CceCalcoloAnagraficaSchema.n_tensione) <= 220000) and (col(CceCalcoloAnagraficaSchema.n_tensione) > 35000), lit("ALTA"))
          .otherwise(lit("ALTISSIMA"))
      )
      .withColumn(CceCalcoloAnagraficaSchema.n_id_pod, col(RcuPodPSchema.n_id_pod).cast(LongType))
      /* IMPORTANTE: QUI VIENE ESEGUITO IL MAPPING DI UNA SERIE DI PARTITE IVA. RICHIESTA PERVENUTA IN FASE DI COLLAUDO. */
      .withColumn(CceCalcoloAnagraficaSchema.t_piva_udd,
        when(col(CceCalcoloAnagraficaSchema.t_piva_udd) === "00000000012", lit("06655971007"))
          .when(col(CceCalcoloAnagraficaSchema.t_piva_udd) === "00000000010", lit("02106730415"))
          .when(col(CceCalcoloAnagraficaSchema.t_piva_udd) === "00000000008", lit("01008081000"))
          .when(col(CceCalcoloAnagraficaSchema.t_piva_udd) === "00000000011", lit("12883420155"))
          .otherwise(col(CceCalcoloAnagraficaSchema.t_piva_udd))
      )
      .selectExpr(CceCalcoloAnagraficaSchema.getValues:_*)
      .distinct

    anagrafica
  }

  def getTrattamento(rcuPodP: DataFrame, rcusPodP: DataFrame, rcuPodMisureP: DataFrame, rcusPodMisureP: DataFrame, annoIn: Int, meseIn: Int): DataFrame = {
    val colAnnoMese = "colAnnoMese"
    val trattColName = "trattamento"
    val window = Window.partitionBy(col(RcusPodPSchema.t_codice_pod))

    val pod = rcuPodP.select(RcuPodPSchema.n_id_pod, RcuPodPSchema.t_codice_pod)
      .unionByName(rcusPodP.select(RcusPodPSchema.n_id_pod,RcusPodPSchema.t_codice_pod))
      .withColumn(RcuPodPSchema.t_codice_pod, substring(col(RcuPodPSchema.t_codice_pod), 1, 14))

    val podMisureX = rcuPodMisureP
      .join(pod, Seq(RcuPodPSchema.n_id_pod.toString), "right")
      .withColumn(RcuPodPSchema.t_codice_pod, substring(col(RcuPodPSchema.t_codice_pod), 1, 14))
      .withColumn(colAnnoMese, concat_ws("-" ,lit(annoIn.toString), lit(meseIn.toString), lit("01")))
      .filter(col(RcuPodMisurePSchema.d_anno_mese) <= col(colAnnoMese))
      .select(RcuPodPSchema.t_codice_pod, RcuPodMisurePSchema.t_trattamento)

    val podMisureY = rcusPodMisureP
      .join(pod, Seq(RcusPodPSchema.n_id_pod.toString), "inner")
      .withColumn(RcusPodPSchema.t_codice_pod, substring(col(RcusPodPSchema.t_codice_pod), 1, 14))
      .withColumn("rank", rank().over(window.orderBy(col(RcusPodMisurePSchema.d_aggiornamento).desc, col(RcusPodMisurePSchema.d_anno_mese).desc)))
      .filter(col("rank") === 1)
      .withColumn(colAnnoMese, concat_ws("-" ,lit(annoIn.toString), lit(meseIn.toString), lit("01")))
      .withColumn(RcusPodMisurePSchema.t_trattamento, when(to_date(col(RcusPodMisurePSchema.d_anno_mese)) < to_date(col(colAnnoMese)), col(RcusPodMisurePSchema.t_trattamento_succ)).otherwise(col(RcusPodMisurePSchema.t_trattamento)))
      .select(RcusPodPSchema.t_codice_pod ,RcusPodMisurePSchema.t_trattamento)

    val trattamento = pod
      .join(podMisureX, Seq(RcuPodPSchema.t_codice_pod.toString), "left")
      .join(podMisureY.withColumnRenamed(RcusPodMisurePSchema.t_trattamento, "tratt_y"), Seq(RcuPodPSchema.t_codice_pod.toString), "left")
      .withColumn(trattColName, coalesce(col(RcuPodMisurePSchema.t_trattamento), col("tratt_y")))
      .drop("tratt_y", RcuPodMisurePSchema.t_trattamento)
      .filter(col(trattColName) === "O")
      .withColumn(CceCalcoloTrattamentoSchema.is_t_trattamento, lit("Y"))
      .withColumn(CceCalcoloTrattamentoSchema.d_data_elaborazione, lit(Environment.startDateTime.toString))
      .withColumn(CceCalcoloTrattamentoSchema.t_anno_mese, concat(lit(annoIn.toString), lpad(lit(meseIn.toString), 2, "0")))
      .withColumn(CceCalcoloTrattamentoSchema.n_id_pod, col(RcusPodPSchema.n_id_pod).cast(LongType))
      .withColumnRenamed(RcusPodPSchema.t_codice_pod, CceCalcoloTrattamentoSchema.t_codice_pod)
      .selectExpr(CceCalcoloTrattamentoSchema.getValues:_*)
      .distinct

    trattamento
  }
}
