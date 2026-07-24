package it.eng.au.cceCalcolo.controller

import it.eng.au.cceCalcolo.schema.annullamenti.{CceMoAnnullate1gEinSchema, CceMoAnnullate1gSchema, CceMoAnnullate2gEinSchema, CceMoAnnullate2gSchema}
import it.eng.au.cceCalcolo.schema.flussiMisure.{FlussoMisureEstensioneQuartiInSchema, FlussoMisureEstensioneQuartiSchema, FlussoMisureQuartiInSchema, FlussoMisureQuartiSchema}
import it.eng.au.cceCalcolo.utility.environment.Environment.startDateTime
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, lit, lpad, row_number, substring}
import org.apache.spark.sql.types.{IntegerType, StringType}

class AnnullamentoMisureController {

  def get1G2G (flussoMisureEstensioneQuartiDF: DataFrame, flussoMisureQuartiDF: DataFrame): DataFrame = {

    val logicKey: String = "logicKey"
    val logicKeyFields = List(col(FlussoMisureQuartiSchema.pivadistributorequarti),col(FlussoMisureQuartiSchema.codcontrdispquarti), col(FlussoMisureQuartiSchema.podquarti))
    val logicKeyFieldsMot3 = List(col(FlussoMisureEstensioneQuartiSchema.pivadistributorequarti_mot3),col(FlussoMisureEstensioneQuartiSchema.codcontrdispquarti_mot3), col(FlussoMisureEstensioneQuartiSchema.podquarti_mot3))
    val window = Window.partitionBy(col(logicKey), col(FlussoMisureEstensioneQuartiSchema.time_stamp_mot3))
    val kDtFields1g = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti))

    flussoMisureEstensioneQuartiDF
      .filter(col(FlussoMisureEstensioneQuartiSchema.motivazione_mot3)==="3")
      .filter(col(FlussoMisureEstensioneQuartiSchema.areaquarti_mot3).isin("NEW_F_RFO","NEW_F_RFO2G"))
      .filter(col(FlussoMisureEstensioneQuartiSchema.data_misura_mot3).isNull)
      .withColumn(FlussoMisureEstensioneQuartiSchema.podquarti_mot3, substring(col(FlussoMisureEstensioneQuartiSchema.podquarti_mot3), 1, 14))
      .withColumn(logicKey, concat(logicKeyFieldsMot3:_*))
      .join(
       flussoMisureQuartiDF
      .withColumn(FlussoMisureQuartiSchema.podquarti, substring(col(FlussoMisureQuartiSchema.podquarti), 1, 14))
      .withColumn(logicKey, concat(logicKeyFields:_*)),
        Seq(logicKey), "inner"
      )
      .filter(col(FlussoMisureEstensioneQuartiSchema.time_stamp_mot3) > col(FlussoMisureQuartiSchema.time_stamp))
      .withColumn("row_number", row_number().over(window.orderBy(col(FlussoMisureQuartiSchema.time_stamp).desc)))
      .filter(col("row_number")===1)
      .withColumn(CceMoAnnullate1gSchema.k_dt, kDtFields1g)
      .withColumn(CceMoAnnullate1gSchema.data_aggiornamento, lit(startDateTime.toString))
      .withColumn(CceMoAnnullate1gSchema.anno, lit(col(FlussoMisureQuartiSchema.annoquarti).cast(StringType)))
      .withColumn(CceMoAnnullate1gSchema.mese, lit(col(FlussoMisureQuartiSchema.mesequarti).cast(StringType)))
      .withColumn(CceMoAnnullate1gSchema.timestamp, lit(col(FlussoMisureQuartiSchema.time_stamp).cast(StringType)))
      .withColumnRenamed(FlussoMisureQuartiSchema.nomefile, CceMoAnnullate1gSchema.nomefile)
      .withColumnRenamed(FlussoMisureQuartiSchema.podquarti, CceMoAnnullate1gSchema.podquarti)
      .withColumnRenamed(FlussoMisureQuartiSchema.pivadistributorequarti, CceMoAnnullate1gSchema.pivadistributorequarti)
      .withColumnRenamed(FlussoMisureQuartiSchema.pivautentequarti, CceMoAnnullate1gSchema.pivautentequarti)
      .withColumnRenamed(FlussoMisureQuartiSchema.codcontrdispquarti, CceMoAnnullate1gSchema.codcontrdispquarti)
      .selectExpr(CceMoAnnullate1gSchema.getValues:_*)
      .distinct

  }

  def get2G (flussoMisureEstensioneQuartiDF: DataFrame, flussoMisureQuartiDF: DataFrame): DataFrame = {

    val logicKey: String = "logicKey"
    val giornoquarti_mot3: String = "giornoquarti_mot3"

    val logicKeyFields = List(col(FlussoMisureQuartiSchema.giornoquarti),col(FlussoMisureQuartiSchema.pivadistributorequarti),col(FlussoMisureQuartiSchema.codcontrdispquarti), col(FlussoMisureQuartiSchema.podquarti))
    val logicKeyFieldsMot3 = List(col(giornoquarti_mot3),col(FlussoMisureEstensioneQuartiSchema.pivadistributorequarti_mot3),col(FlussoMisureEstensioneQuartiSchema.codcontrdispquarti_mot3), col(FlussoMisureEstensioneQuartiSchema.podquarti_mot3))
    val window = Window.partitionBy(col(logicKey), col(FlussoMisureEstensioneQuartiSchema.time_stamp_mot3))
    val kDtFields2g = concat(col(FlussoMisureQuartiSchema.podquarti), col(FlussoMisureQuartiSchema.time_stamp), col(FlussoMisureQuartiSchema.pivadistributorequarti), col(FlussoMisureQuartiSchema.pivautentequarti), col(FlussoMisureQuartiSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiSchema.giornoquarti).cast(StringType), 2, "0"))

    flussoMisureEstensioneQuartiDF
      .withColumn(giornoquarti_mot3, substring(col(FlussoMisureEstensioneQuartiSchema.data_misura_mot3),1, 2).cast(IntegerType))
      .filter(col(FlussoMisureEstensioneQuartiSchema.motivazione_mot3) === "3")
      .filter(col(FlussoMisureEstensioneQuartiSchema.areaquarti_mot3) === "NEW_F_RFO2G")
      .filter(col(FlussoMisureEstensioneQuartiSchema.data_misura_mot3).isNotNull)
      .withColumn(FlussoMisureEstensioneQuartiSchema.podquarti_mot3, substring(col(FlussoMisureEstensioneQuartiSchema.podquarti_mot3), 1, 14))
      .withColumn(logicKey, concat(logicKeyFieldsMot3: _*))
      .join(
        flussoMisureQuartiDF
          .withColumn(FlussoMisureQuartiSchema.podquarti, substring(col(FlussoMisureQuartiSchema.podquarti), 1, 14))
          .withColumn(logicKey, concat(logicKeyFields: _*)),
        Seq(logicKey), "inner"
      )
      .filter(col(FlussoMisureEstensioneQuartiSchema.time_stamp_mot3) > col(FlussoMisureQuartiSchema.time_stamp))
      .withColumn("row_number", row_number().over(window.orderBy(col(FlussoMisureQuartiSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceMoAnnullate2gSchema.k_dt, kDtFields2g)
      .withColumn(CceMoAnnullate2gSchema.data_aggiornamento, lit(startDateTime.toString))
      .withColumn(CceMoAnnullate2gSchema.anno, lit(col(FlussoMisureQuartiSchema.annoquarti).cast(StringType)))
      .withColumn(CceMoAnnullate2gSchema.mese, lit(col(FlussoMisureQuartiSchema.mesequarti).cast(StringType)))
      .withColumn(CceMoAnnullate2gSchema.timestamp, lit(col(FlussoMisureQuartiSchema.time_stamp).cast(StringType)))
      .withColumnRenamed(FlussoMisureQuartiSchema.nomefile, CceMoAnnullate1gSchema.nomefile)
      .withColumnRenamed(FlussoMisureQuartiSchema.podquarti, CceMoAnnullate2gSchema.podquarti)
      .withColumnRenamed(FlussoMisureQuartiSchema.pivadistributorequarti, CceMoAnnullate2gSchema.pivadistributorequarti)
      .withColumnRenamed(FlussoMisureQuartiSchema.pivautentequarti, CceMoAnnullate2gSchema.pivautentequarti)
      .withColumnRenamed(FlussoMisureQuartiSchema.codcontrdispquarti, CceMoAnnullate2gSchema.codcontrdispquarti)
      .selectExpr(CceMoAnnullate2gSchema.getValues: _*)
      .distinct

  }

  def get1G2Gein (flussoMisureEstensioneQuartiInDF: DataFrame, flussoMisureQuartiInDF: DataFrame): DataFrame = {

    val logicKey: String = "logicKey"
    val logicKeyFields = List(col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti), col(FlussoMisureQuartiInSchema.podquarti))
    val logicKeyFieldsMot3 = List(col(FlussoMisureEstensioneQuartiInSchema.pivadistributorequarti_mot3),col(FlussoMisureEstensioneQuartiInSchema.codcontrdispquarti_mot3), col(FlussoMisureEstensioneQuartiInSchema.podquarti_mot3))
    val window = Window.partitionBy(col(logicKey), col(FlussoMisureEstensioneQuartiInSchema.time_stamp_mot3))
    val kDtFields1g = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti))

    flussoMisureEstensioneQuartiInDF
      .filter(col(FlussoMisureEstensioneQuartiInSchema.motivazione_mot3) === "3")
      .filter(col(FlussoMisureEstensioneQuartiInSchema.areaquarti_mot3).isin("NEW_F_RFO", "NEW_F_RFO2G"))
      .filter(col(FlussoMisureEstensioneQuartiInSchema.data_misura_mot3).isNull)
      .withColumn(FlussoMisureEstensioneQuartiInSchema.podquarti_mot3, substring(col(FlussoMisureEstensioneQuartiInSchema.podquarti_mot3), 1, 14))
      .withColumn(logicKey, concat(logicKeyFieldsMot3: _*))
      .join(
        flussoMisureQuartiInDF
          .withColumn(FlussoMisureQuartiInSchema.podquarti, substring(col(FlussoMisureQuartiInSchema.podquarti), 1, 14))
          .withColumn(logicKey, concat(logicKeyFields: _*)),
        Seq(logicKey), "inner"
      )
      .filter(col(FlussoMisureEstensioneQuartiInSchema.time_stamp_mot3) > col(FlussoMisureQuartiInSchema.time_stamp))
      .withColumn("row_number", row_number().over(window.orderBy(col(FlussoMisureQuartiInSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceMoAnnullate1gEinSchema.k_dt, kDtFields1g)
      .withColumn(CceMoAnnullate1gEinSchema.data_aggiornamento, lit(startDateTime.toString))
      .withColumn(CceMoAnnullate1gEinSchema.anno, lit(col(FlussoMisureQuartiInSchema.annoquarti).cast(StringType)))
      .withColumn(CceMoAnnullate1gEinSchema.mese, lit(col(FlussoMisureQuartiInSchema.mesequarti).cast(StringType)))
      .withColumn(CceMoAnnullate1gEinSchema.timestamp, lit(col(FlussoMisureQuartiInSchema.time_stamp).cast(StringType)))
      .withColumnRenamed(FlussoMisureQuartiInSchema.nomefile, CceMoAnnullate1gEinSchema.nomefile)
      .withColumnRenamed(FlussoMisureQuartiInSchema.podquarti, CceMoAnnullate1gEinSchema.podquarti)
      .withColumnRenamed(FlussoMisureQuartiInSchema.pivadistributorequarti, CceMoAnnullate1gEinSchema.pivadistributorequarti)
      .withColumnRenamed(FlussoMisureQuartiInSchema.pivautentequarti, CceMoAnnullate1gEinSchema.pivautentequarti)
      .withColumnRenamed(FlussoMisureQuartiInSchema.codcontrdispquarti, CceMoAnnullate1gEinSchema.codcontrdispquarti)
      .selectExpr(CceMoAnnullate1gEinSchema.getValues: _*)
      .distinct

  }

  def get2Gein (flussoMisureEstensioneQuartiInDF: DataFrame, flussoMisureQuartiInDF: DataFrame): DataFrame = {

    val logicKey: String = "logicKey"
    val giornoquarti_mot3: String = "giornoquarti_mot3"

    val logicKeyFields = List(col(FlussoMisureQuartiSchema.giornoquarti), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti), col(FlussoMisureQuartiInSchema.podquarti))
    val logicKeyFieldsMot3 = List(col(giornoquarti_mot3), col(FlussoMisureEstensioneQuartiInSchema.pivadistributorequarti_mot3),col(FlussoMisureEstensioneQuartiInSchema.codcontrdispquarti_mot3), col(FlussoMisureEstensioneQuartiInSchema.podquarti_mot3))
    val window = Window.partitionBy(col(logicKey), col(FlussoMisureEstensioneQuartiInSchema.time_stamp_mot3))
    val kDtFields2g = concat(col(FlussoMisureQuartiInSchema.podquarti), col(FlussoMisureQuartiInSchema.time_stamp), col(FlussoMisureQuartiInSchema.pivadistributorequarti), col(FlussoMisureQuartiInSchema.pivautentequarti), col(FlussoMisureQuartiInSchema.codcontrdispquarti), lpad(col(FlussoMisureQuartiInSchema.giornoquarti).cast(StringType), 2, "0"))

    flussoMisureEstensioneQuartiInDF
      .withColumn(giornoquarti_mot3, substring(col(FlussoMisureEstensioneQuartiInSchema.data_misura_mot3),1, 2).cast(IntegerType))
      .filter(col(FlussoMisureEstensioneQuartiInSchema.motivazione_mot3) === "3")
      .filter(col(FlussoMisureEstensioneQuartiInSchema.areaquarti_mot3) === "NEW_F_RFO2G")
      .filter(col(FlussoMisureEstensioneQuartiInSchema.data_misura_mot3).isNotNull)
      .withColumn(FlussoMisureEstensioneQuartiInSchema.podquarti_mot3, substring(col(FlussoMisureEstensioneQuartiInSchema.podquarti_mot3), 1, 14))
      .withColumn(logicKey, concat(logicKeyFieldsMot3: _*))
      .join(
        flussoMisureQuartiInDF
          .withColumn(FlussoMisureQuartiInSchema.podquarti, substring(col(FlussoMisureQuartiInSchema.podquarti), 1, 14))
          .withColumn(logicKey, concat(logicKeyFields: _*)),
        Seq(logicKey), "inner"
      )
      .filter(col(FlussoMisureEstensioneQuartiInSchema.time_stamp_mot3) > col(FlussoMisureQuartiInSchema.time_stamp))
      .withColumn("row_number", row_number().over(window.orderBy(col(FlussoMisureQuartiInSchema.time_stamp).desc)))
      .filter(col("row_number") === 1)
      .withColumn(CceMoAnnullate2gEinSchema.k_dt, kDtFields2g)
      .withColumn(CceMoAnnullate2gEinSchema.data_aggiornamento, lit(startDateTime.toString))
      .withColumn(CceMoAnnullate2gEinSchema.timestamp, lit(col(FlussoMisureQuartiInSchema.time_stamp).cast(StringType)))
      .withColumn(CceMoAnnullate2gEinSchema.anno, lit(col(FlussoMisureQuartiInSchema.annoquarti).cast(StringType)))
      .withColumn(CceMoAnnullate2gEinSchema.mese, lit(col(FlussoMisureQuartiInSchema.mesequarti).cast(StringType)))
      .withColumnRenamed(FlussoMisureQuartiInSchema.nomefile, CceMoAnnullate2gEinSchema.nomefile)
      .withColumnRenamed(FlussoMisureQuartiInSchema.podquarti, CceMoAnnullate2gEinSchema.podquarti)
      .withColumnRenamed(FlussoMisureQuartiInSchema.pivadistributorequarti, CceMoAnnullate2gEinSchema.pivadistributorequarti)
      .withColumnRenamed(FlussoMisureQuartiInSchema.pivautentequarti, CceMoAnnullate2gEinSchema.pivautentequarti)
      .withColumnRenamed(FlussoMisureQuartiInSchema.codcontrdispquarti, CceMoAnnullate2gEinSchema.codcontrdispquarti)
      .selectExpr(CceMoAnnullate2gEinSchema.getValues: _*)
      .distinct

  }
}
