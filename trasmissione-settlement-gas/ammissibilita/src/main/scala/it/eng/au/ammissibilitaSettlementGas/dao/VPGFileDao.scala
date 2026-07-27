package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.{VPGFile, VPGMetadata}
import it.eng.au.ammissibilitaSettlementGas.schema.VPGFileSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{hash, lit}
import org.apache.spark.sql.types.LongType

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

class VPGFileDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2VPGFileTableName
  override val parquetPath: String = Properties.getTSG2VPGFilePath
  override val partitionByColumns: List[String] = List(VPGFileSchema.executionid.toString)
  override val columns: List[String] = VPGFileSchema.getValues
  override val partitionColumn: String = VPGFileSchema.executionid.toString


  def get(VPGFiles: RDD[VPGMetadata]): DataFrame = {
    val rdd = VPGFiles.map(vpgMetaRow => {(vpgMetaRow.file.getName, vpgMetaRow.yearDir, vpgMetaRow.monthDir, vpgMetaRow.pivaRdb,
      vpgMetaRow.annoTermico, vpgMetaRow.progressivo, Files.readAttributes(vpgMetaRow.file.toPath, classOf[BasicFileAttributes]).creationTime().toString)})

    //val df = Environment.spark.createDataFrame(rdd).toDF("nomeFile", "anno", "mese", VPGFileSchema.piva_rdb, VPGFileSchema.annotermico, VPGFileSchema.data_creazione)
    val df = Environment.spark.createDataFrame(rdd).toDF(VPGFileSchema.nome_file, "anno", "mese", VPGFileSchema.piva_rdb,
      VPGFileSchema.annotermico, VPGFileSchema.progressivo,VPGFileSchema.data_creazione)
    //val dfWithIDFile = df.withColumn("n_id_tsg2_file", hash(df.col("nomeFile"), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)
    val dfWithIDFile = df.withColumn(VPGFileSchema.n_id_TSG2_file, hash(df.col(VPGFileSchema.nome_file), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    dfWithIDFile.withColumn(VPGFileSchema.executionid, lit(Environment.executionId).cast(LongType))
      .selectExpr(VPGFileSchema.getValues: _*)
  }
}
