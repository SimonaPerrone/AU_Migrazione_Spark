package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.{TFCFile, TFCMetadata}
import it.eng.au.ammissibilitaSettlementGas.schema.TFCFileSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{hash, lit}
import org.apache.spark.sql.types.LongType

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

class TFCFileDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2TFCFileTableName
  override val parquetPath: String = Properties.getTSG2TFCFilePath
  override val columns: List[String] = TFCFileSchema.getValues
  override val partitionColumn: String = TFCFileSchema.executionid.toString // TFCFileSchema.n_id_TSG2_file.toString //TFCFileSchema.executionid.toString
  override val partitionByColumns: List[String] = List(TFCFileSchema.executionid.toString) //List(TFCFileSchema.n_id_TSG2_file.toString) //List(TFCFileSchema.executionid.toString)


  def get(TFCFiles: RDD[TFCMetadata]): DataFrame = {
    val rdd = TFCFiles.map(tfcMetaRow => {(tfcMetaRow.file.getName, tfcMetaRow.yearDir, tfcMetaRow.monthDir, tfcMetaRow.pivaRdb,
      tfcMetaRow.annoMese, tfcMetaRow.progressivo, Files.readAttributes(tfcMetaRow.file.toPath, classOf[BasicFileAttributes]).creationTime().toString)})
    //val rdd = TFCFiles.map(vpgMetaRow => {(vpgMetaRow.file.getName, vpgMetaRow.yearDir, vpgMetaRow.monthDir, vpgMetaRow.pivaRdb, vpgMetaRow.annoMese, vpgMetaRow.lastModified)})

    //val df = Environment.spark.createDataFrame(rdd).toDF("nomeFile", "anno", "mese", TFCFileSchema.piva_rdb, TFCFileSchema.annomese, TFCFileSchema.data_creazione)
    val df = Environment.spark.createDataFrame(rdd).toDF(TFCFileSchema.nome_file, "anno", "mese", TFCFileSchema.piva_rdb, TFCFileSchema.annomese,
      TFCFileSchema.progressivo, TFCFileSchema.data_creazione)
    //val dfWithIDFile = df.withColumn("n_id_tsg2_file", hash(df.col("nomeFile"), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)
    val dfWithIDFile = df.withColumn(TFCFileSchema.n_id_TSG2_file, hash(df.col(TFCFileSchema.nome_file), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    dfWithIDFile.withColumn(TFCFileSchema.executionid, lit(Environment.executionId).cast(LongType))
      .selectExpr(TFCFileSchema.getValues: _*)

  }

}
