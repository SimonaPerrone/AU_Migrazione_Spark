package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.VPGMetadata
import it.eng.au.ammissibilitaSettlementGas.schema.ReportAmmissibilitaVPGSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

class ReportAmmissibilitaVPGDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2ReportAmmissibilitaVPGTableName
  override val parquetPath: String = Properties.getTSG2ReportAmmissibilitaVPGPath
  override val partitionByColumns: List[String] = List(ReportAmmissibilitaVPGSchema.annomese_ricezione.toString)
  override val columns: List[String] = ReportAmmissibilitaVPGSchema.getValues
  override val partitionColumn: String = ReportAmmissibilitaVPGSchema.executionid.toString

  def get(VPGFilesAndRecordsChecked: RDD[VPGMetadata]): DataFrame = {

    val rdd = VPGFilesAndRecordsChecked.map(vpgMeta => {
      (vpgMeta.file.getName, vpgMeta.yearDir, vpgMeta.monthDir,
        vpgMeta.pivaRdb, vpgMeta.isAmmissibile, vpgMeta.statusCode, vpgMeta.statusMessage, vpgMeta.annoTermico,
        vpgMeta.csv, vpgMeta.progressivo, vpgMeta.tipoFile)
    })

    val df = Environment.spark.createDataFrame(rdd).toDF(ReportAmmissibilitaVPGSchema.nome_file, "anno", "mese",
      ReportAmmissibilitaVPGSchema.piva_utente, ReportAmmissibilitaVPGSchema.verifica_amm, ReportAmmissibilitaVPGSchema.cod_causale,
      ReportAmmissibilitaVPGSchema.motivazione, ReportAmmissibilitaVPGSchema.annotermico, "csv",
      ReportAmmissibilitaVPGSchema.progressivo, ReportAmmissibilitaVPGSchema.tipo_file)

    val dfWithIDFile = df.withColumn(ReportAmmissibilitaVPGSchema.n_id_tsg2_file, hash(df.col(ReportAmmissibilitaVPGSchema.nome_file), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    val df1 = dfWithIDFile.filter(col(ReportAmmissibilitaVPGSchema.verifica_amm) === "false" ||
      col(ReportAmmissibilitaVPGSchema.verifica_amm) === false)
      .select(col(ReportAmmissibilitaVPGSchema.n_id_tsg2_file), col(ReportAmmissibilitaVPGSchema.nome_file),
        col(ReportAmmissibilitaVPGSchema.piva_utente), col(ReportAmmissibilitaVPGSchema.verifica_amm), col(ReportAmmissibilitaVPGSchema.cod_causale),
        col(ReportAmmissibilitaVPGSchema.progressivo), col(ReportAmmissibilitaVPGSchema.motivazione), concat(col("anno"), col("mese")).as(ReportAmmissibilitaVPGSchema.annomese_ricezione),
        col(ReportAmmissibilitaVPGSchema.annotermico), col(ReportAmmissibilitaVPGSchema.tipo_file))
      .withColumn(ReportAmmissibilitaVPGSchema.giorno_riferimento, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_A1, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_B1, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_C1, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_E1, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_D1, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_F1, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_A2, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_B2, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_C2, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_D2, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_E2, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_F2, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_A3, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_B3, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_C3, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_D3, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C1_E3, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_F3, lit("")).withColumn(ReportAmmissibilitaVPGSchema.C2, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.C4, lit("")).withColumn(ReportAmmissibilitaVPGSchema.T1_1, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.T1_2, lit("")).withColumn(ReportAmmissibilitaVPGSchema.T1_3, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.numero_riga, lit(""))
      .withColumn(ReportAmmissibilitaVPGSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(ReportAmmissibilitaVPGSchema.data_amm, lit(Environment.startDateTime.toString).cast(TimestampType))
      .selectExpr(ReportAmmissibilitaVPGSchema.getValues: _*)

    val df2 = dfWithIDFile.filter(col(ReportAmmissibilitaVPGSchema.verifica_amm) === true)
      .select(col(ReportAmmissibilitaVPGSchema.n_id_tsg2_file),
        col(ReportAmmissibilitaVPGSchema.nome_file), col(ReportAmmissibilitaVPGSchema.piva_utente), col(ReportAmmissibilitaVPGSchema.progressivo),
        col(ReportAmmissibilitaVPGSchema.annotermico), explode(col("csv")).alias("explode"),
        concat(col("anno"), col("mese")).as(ReportAmmissibilitaVPGSchema.annomese_ricezione), col(ReportAmmissibilitaVPGSchema.tipo_file))
      .withColumn(ReportAmmissibilitaVPGSchema.giorno_riferimento, col("explode.giornoRiferimento"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_A1, col("explode.C1A1")).withColumn(ReportAmmissibilitaVPGSchema.C1_B1, col("explode.C1B1"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_C1, col("explode.C1C1")).withColumn(ReportAmmissibilitaVPGSchema.C1_E1, col("explode.C1E1"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_D1, lit(col("explode.C1D1")))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_F1, col("explode.C1F1")).withColumn(ReportAmmissibilitaVPGSchema.C1_A2, col("explode.C1A2"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_B2, col("explode.C1B2")).withColumn(ReportAmmissibilitaVPGSchema.C1_C2, col("explode.C1C2"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_D2, col("explode.C1D2")).withColumn(ReportAmmissibilitaVPGSchema.C1_E2, col("explode.C1E2"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_F2, col("explode.C1F2")).withColumn(ReportAmmissibilitaVPGSchema.C1_A3, col("explode.C1A3"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_B3, col("explode.C1B3")).withColumn(ReportAmmissibilitaVPGSchema.C1_C3, col("explode.C1C3"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_D3, col("explode.C1D3")).withColumn(ReportAmmissibilitaVPGSchema.C1_E3, col("explode.C1E3"))
      .withColumn(ReportAmmissibilitaVPGSchema.C1_F3, col("explode.C1F3")).withColumn(ReportAmmissibilitaVPGSchema.C2, col("explode.C2"))
      .withColumn(ReportAmmissibilitaVPGSchema.C4, col("explode.C4")).withColumn(ReportAmmissibilitaVPGSchema.T1_1, col("explode.T11"))
      .withColumn(ReportAmmissibilitaVPGSchema.T1_2, col("explode.T12")).withColumn(ReportAmmissibilitaVPGSchema.T1_3, col("explode.T13"))
      .withColumn(ReportAmmissibilitaVPGSchema.numero_riga, col("explode.numeroRiga"))
      .withColumn(ReportAmmissibilitaVPGSchema.verifica_amm, col("explode.isAmmissibile"))
      .withColumn(ReportAmmissibilitaVPGSchema.cod_causale, col("explode.statusCode"))
      .withColumn(ReportAmmissibilitaVPGSchema.motivazione, col("explode.statusMessage"))
      .withColumn(ReportAmmissibilitaVPGSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(ReportAmmissibilitaVPGSchema.data_amm, lit(Environment.startDateTime.toString).cast(TimestampType))
      .drop("explode").selectExpr(ReportAmmissibilitaVPGSchema.getValues: _*)

    val resultDF = df1.union(df2).selectExpr(ReportAmmissibilitaVPGSchema.getValues: _*)

    resultDF
  }
}
