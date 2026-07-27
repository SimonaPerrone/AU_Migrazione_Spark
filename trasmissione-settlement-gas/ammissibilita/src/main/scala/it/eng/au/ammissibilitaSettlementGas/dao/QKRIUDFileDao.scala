package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.QKRIUDMetadata
import it.eng.au.ammissibilitaSettlementGas.schema.QKRIUDFileSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{hash, lit}
import org.apache.spark.sql.types.LongType

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

class QKRIUDFileDao extends OutputDAO{
  override val tableName: String = Properties.getTSG2QKRIUDFileTableName
  override val parquetPath: String = Properties.getTSG2QKRIUDFilePath
  override val partitionByColumns: List[String] = List(QKRIUDFileSchema.executionid.toString)
  override val columns: List[String] = QKRIUDFileSchema.getValues
  override val partitionColumn: String = QKRIUDFileSchema.executionid.toString

  def get(QKRIUDFiles: RDD[QKRIUDMetadata]): DataFrame = {
    val rdd = QKRIUDFiles.map(QKRIUDMetaRow => {
      (QKRIUDMetaRow.file.getName, QKRIUDMetaRow.pivaRdb, QKRIUDMetaRow.yearDir, QKRIUDMetaRow.monthDir,
        QKRIUDMetaRow.annoTermico, QKRIUDMetaRow.progressivo, Files.readAttributes(QKRIUDMetaRow.file.toPath, classOf[BasicFileAttributes]).creationTime().toString)
    })

    val df = Environment.spark.createDataFrame(rdd)
      .toDF(QKRIUDFileSchema.nome_file, QKRIUDFileSchema.piva_rdb, "anno", "mese",
      QKRIUDFileSchema.annotermico, QKRIUDFileSchema.progressivo, QKRIUDFileSchema.data_creazione)
    val dfWithIDFile = df.withColumn(QKRIUDFileSchema.n_id_TSG2_file, hash(df.col(QKRIUDFileSchema.nome_file), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    dfWithIDFile.withColumn(QKRIUDFileSchema.executionid, lit(Environment.executionId).cast(LongType))
      .selectExpr(QKRIUDFileSchema.getValues: _*)
  }
}
