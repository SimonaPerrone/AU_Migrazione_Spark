package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.QKRIUDMetadata
import it.eng.au.ammissibilitaSettlementGas.schema.ReportAmmissibilitaQKRIUDSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, explode, hash, lit, when}
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, TimestampType}

class ReportAmmissibilitaQKRIUDDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2ReportAmmissibilitaQKRIUDTableName
  override val parquetPath: String = Properties.getTSG2ReportAmmissibilitaQKRIUDPath
  override val partitionByColumns: List[String] = List(ReportAmmissibilitaQKRIUDSchema.annomese_ricezione.toString)
  override val columns: List[String] = ReportAmmissibilitaQKRIUDSchema.getValues
  override val partitionColumn: String = ReportAmmissibilitaQKRIUDSchema.executionid.toString

  def get(QKRIUDFilesAndRecordsChecked: RDD[QKRIUDMetadata]): DataFrame = {

    val rdd = QKRIUDFilesAndRecordsChecked.map(qkriudMeta => {
      (qkriudMeta.file.getName, qkriudMeta.yearDir, qkriudMeta.monthDir,
        qkriudMeta.pivaRdb, qkriudMeta.isAmmissibile, qkriudMeta.statusCode, qkriudMeta.statusMessage,
        qkriudMeta.annoTermico, qkriudMeta.csv, qkriudMeta.progressivo, qkriudMeta.tipoFile)
    })

    val df = Environment.spark.createDataFrame(rdd).toDF(
      ReportAmmissibilitaQKRIUDSchema.nome_file, "anno", "mese",
      ReportAmmissibilitaQKRIUDSchema.piva_utente, ReportAmmissibilitaQKRIUDSchema.verifica_amm,
      ReportAmmissibilitaQKRIUDSchema.cod_causale, ReportAmmissibilitaQKRIUDSchema.motivazione,
      ReportAmmissibilitaQKRIUDSchema.annomese, "csv", ReportAmmissibilitaQKRIUDSchema.progressivo,
      ReportAmmissibilitaQKRIUDSchema.tipo_file)

    val dfWithIDFile = df
      .withColumn(ReportAmmissibilitaQKRIUDSchema.n_id_tsg2_file,
        hash(df.col(ReportAmmissibilitaQKRIUDSchema.nome_file),
          df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    val df1 = dfWithIDFile.filter(col(ReportAmmissibilitaQKRIUDSchema.verifica_amm).cast(BooleanType) === false)
      .select(col(ReportAmmissibilitaQKRIUDSchema.n_id_tsg2_file), col(ReportAmmissibilitaQKRIUDSchema.nome_file),
        col(ReportAmmissibilitaQKRIUDSchema.piva_utente), col(ReportAmmissibilitaQKRIUDSchema.verifica_amm),
        col(ReportAmmissibilitaQKRIUDSchema.cod_causale), col(ReportAmmissibilitaQKRIUDSchema.progressivo),
        col(ReportAmmissibilitaQKRIUDSchema.motivazione), col(ReportAmmissibilitaQKRIUDSchema.tipo_file),
        concat(col("anno"), col("mese")).as(ReportAmmissibilitaQKRIUDSchema.annomese_ricezione),
        col(ReportAmmissibilitaQKRIUDSchema.annomese), col(ReportAmmissibilitaQKRIUDSchema.tipo_file))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.data, lit(""))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.cod_remi, lit(""))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.qkriud, lit(0.0).cast(DoubleType))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.numero_riga, lit(""))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.data_amm, lit(Environment.startDateTime.toString).cast(TimestampType))
      .selectExpr(ReportAmmissibilitaQKRIUDSchema.getValues: _*)

    val df2 = dfWithIDFile.filter(col(ReportAmmissibilitaQKRIUDSchema.verifica_amm).cast(BooleanType) === true)
      .select(col(ReportAmmissibilitaQKRIUDSchema.n_id_tsg2_file), col(ReportAmmissibilitaQKRIUDSchema.nome_file),
        col(ReportAmmissibilitaQKRIUDSchema.piva_utente), col(ReportAmmissibilitaQKRIUDSchema.verifica_amm),
        concat(col("anno"), col("mese")).as(ReportAmmissibilitaQKRIUDSchema.annomese_ricezione),
        col(ReportAmmissibilitaQKRIUDSchema.annomese), explode(col("csv")).alias("explode"),
        col(ReportAmmissibilitaQKRIUDSchema.tipo_file), col(ReportAmmissibilitaQKRIUDSchema.progressivo)
      )
      .withColumn(ReportAmmissibilitaQKRIUDSchema.data, col("explode.data"))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.cod_remi, col("explode.codRemi"))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.qkriud, col("explode.qkriud").cast(DoubleType))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.numero_riga, col("explode.numeroRiga"))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.cod_causale, col("explode.statusCode"))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.motivazione, col("explode.statusMessage"))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(ReportAmmissibilitaQKRIUDSchema.data_amm, lit(Environment.startDateTime.toString).cast(TimestampType))
      //operazione dovuta all'allineamento dei metadati relativi alla verifica ammissibilità record
      .withColumn(ReportAmmissibilitaQKRIUDSchema.verifica_amm, when(col(ReportAmmissibilitaQKRIUDSchema.cod_causale)==="", lit(true)).otherwise(lit(false)))
      .selectExpr(ReportAmmissibilitaQKRIUDSchema.getValues: _*)

    val resultDF = df1.unionByName(df2)
      .selectExpr(ReportAmmissibilitaQKRIUDSchema.getValues: _*)
      .distinct

    resultDF
  }
}
