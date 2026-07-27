package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.TFCMetadata
import it.eng.au.ammissibilitaSettlementGas.schema.ReportAmmissibilitaTFCSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, TimestampType}

class ReportAmmissibilitaTFCDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2ReportAmmissibilitaTFCTableName
  override val parquetPath: String = Properties.getTSG2ReportAmmissibilitaTFCPath
  override val partitionByColumns: List[String] = List(ReportAmmissibilitaTFCSchema.annomese_ricezione.toString)
  override val columns: List[String] = ReportAmmissibilitaTFCSchema.getValues
  override val partitionColumn: String = ReportAmmissibilitaTFCSchema.executionid.toString


  def get(TFCFilesAndRecordsChecked: RDD[TFCMetadata]): DataFrame = {

    val rdd = TFCFilesAndRecordsChecked.map(tfcMeta => {(tfcMeta.file.getName, tfcMeta.yearDir, tfcMeta.monthDir, tfcMeta.pivaRdb,
      tfcMeta.isAmmissibile, tfcMeta.statusCode, tfcMeta.statusMessage, tfcMeta.annoMese, tfcMeta.csv, tfcMeta.progressivo, tfcMeta.tipoFile)})

    val df = Environment.spark.createDataFrame(rdd).toDF(ReportAmmissibilitaTFCSchema.nome_file, "anno", "mese", ReportAmmissibilitaTFCSchema.piva_utente,
      ReportAmmissibilitaTFCSchema.verifica_amm, ReportAmmissibilitaTFCSchema.cod_causale, ReportAmmissibilitaTFCSchema.motivazione,
      ReportAmmissibilitaTFCSchema.annomese, "csv", ReportAmmissibilitaTFCSchema.progressivo, ReportAmmissibilitaTFCSchema.tipo_file)

    val dfWithIDFile = df.withColumn(ReportAmmissibilitaTFCSchema.n_id_tsg2_file, hash(df.col(ReportAmmissibilitaTFCSchema.nome_file),
      df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    val df1 = dfWithIDFile.filter(col(ReportAmmissibilitaTFCSchema.verifica_amm) === "false" ||
      col(ReportAmmissibilitaTFCSchema.verifica_amm) === false).select(col(ReportAmmissibilitaTFCSchema.n_id_tsg2_file), col(ReportAmmissibilitaTFCSchema.nome_file),
      col(ReportAmmissibilitaTFCSchema.piva_utente), col(ReportAmmissibilitaTFCSchema.verifica_amm), col(ReportAmmissibilitaTFCSchema.cod_causale),
      col(ReportAmmissibilitaTFCSchema.progressivo), col(ReportAmmissibilitaTFCSchema.motivazione),
      concat(col("anno"), col("mese")).as(ReportAmmissibilitaTFCSchema.annomese_ricezione),
      col(ReportAmmissibilitaTFCSchema.annomese), col(ReportAmmissibilitaTFCSchema.tipo_file))
      .withColumn(ReportAmmissibilitaTFCSchema.wkr, lit(null).cast(DoubleType))
      .withColumn(ReportAmmissibilitaTFCSchema.id_reg_clim, lit(null).cast(LongType))
      .withColumn(ReportAmmissibilitaTFCSchema.data, lit(""))
      .withColumn(ReportAmmissibilitaTFCSchema.numero_riga, lit(""))
      .withColumn(ReportAmmissibilitaTFCSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(ReportAmmissibilitaTFCSchema.data_amm, lit(Environment.startDateTime.toString).cast(TimestampType))
      .selectExpr(ReportAmmissibilitaTFCSchema.getValues: _*)

    val df2 = dfWithIDFile.filter(col(ReportAmmissibilitaTFCSchema.verifica_amm) === true).select(col(ReportAmmissibilitaTFCSchema.n_id_tsg2_file),
      col(ReportAmmissibilitaTFCSchema.nome_file), col(ReportAmmissibilitaTFCSchema.piva_utente), col(ReportAmmissibilitaTFCSchema.progressivo),
      concat(col("anno"), col("mese")).as(ReportAmmissibilitaTFCSchema.annomese_ricezione),
      col(ReportAmmissibilitaTFCSchema.annomese), explode(col("csv")).alias("explode"), col(ReportAmmissibilitaTFCSchema.tipo_file))
      .withColumn(ReportAmmissibilitaTFCSchema.wkr, col("explode.wkr").cast(DoubleType))
      .withColumn(ReportAmmissibilitaTFCSchema.id_reg_clim, col("explode.idRegClimatica").cast(LongType))
      .withColumn(ReportAmmissibilitaTFCSchema.data, col("explode.data"))
      .withColumn(ReportAmmissibilitaTFCSchema.numero_riga, col("explode.numeroRiga"))
      .withColumn(ReportAmmissibilitaTFCSchema.verifica_amm, col("explode.isAmmissibile"))
      .withColumn(ReportAmmissibilitaTFCSchema.cod_causale, col("explode.statusCode"))
      .withColumn(ReportAmmissibilitaTFCSchema.motivazione, col("explode.statusMessage"))
      .withColumn(ReportAmmissibilitaTFCSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(ReportAmmissibilitaTFCSchema.data_amm, lit(Environment.startDateTime.toString).cast(TimestampType))
      .drop("explode").selectExpr(ReportAmmissibilitaTFCSchema.getValues: _*)

    val resultDF = df1.union(df2).selectExpr(ReportAmmissibilitaTFCSchema.getValues: _*)

    resultDF

  }

}
