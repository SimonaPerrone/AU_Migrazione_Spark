package it.eng.au.calcoloSettlementGas.dao

import it.eng.au.calcoloSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.calcoloSettlementGas.model.ParametriCarattProfPrel
import it.eng.au.calcoloSettlementGas.schema.TabParametriCaratteristiciProfPrelSchema
import it.eng.au.calcoloSettlementGas.utility.Constants.CSV_DELIMITER
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, LongType}

import java.io.File

class TabParametriCaratteristiciProfPrelDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2TabParametriCaratteristiciProfPrelTableName
  override val parquetPath: String = Properties.getTSG2TabParametriCaratteristiciProfPrelPath
  override val partitionByColumns: List[String] = List(TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM)
  override val columns: List[String] = TabParametriCaratteristiciProfPrelSchema.getValues
  override val partitionColumn: String = TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM

  def get(parametriCarattProfPrelList: List[ParametriCarattProfPrel]): DataFrame = {
    val df = Environment.spark.createDataFrame(parametriCarattProfPrelList).toDF(TabParametriCaratteristiciProfPrelSchema.BETA1_PROF,
      TabParametriCaratteristiciProfPrelSchema.BETA2_PROF, TabParametriCaratteristiciProfPrelSchema.BETA3_PROF,
      TabParametriCaratteristiciProfPrelSchema.BETA4_PROF, TabParametriCaratteristiciProfPrelSchema.CAT_USO, TabParametriCaratteristiciProfPrelSchema.ZONA_CLIM,
      TabParametriCaratteristiciProfPrelSchema.CLASSE_PREL, TabParametriCaratteristiciProfPrelSchema.PROF)

    val outputDf = df.withColumn(TabParametriCaratteristiciProfPrelSchema.BETA1_PROF,col(TabParametriCaratteristiciProfPrelSchema.BETA1_PROF).cast(DoubleType))
      .withColumn(TabParametriCaratteristiciProfPrelSchema.BETA2_PROF,col(TabParametriCaratteristiciProfPrelSchema.BETA2_PROF).cast(DoubleType))
      .withColumn(TabParametriCaratteristiciProfPrelSchema.BETA3_PROF,col(TabParametriCaratteristiciProfPrelSchema.BETA3_PROF).cast(DoubleType))
      .withColumn(TabParametriCaratteristiciProfPrelSchema.BETA4_PROF, col(TabParametriCaratteristiciProfPrelSchema.BETA4_PROF).cast(DoubleType))

    outputDf
  }

  def getParametriCarattProfPrelFromFile(): List[ParametriCarattProfPrel] = {
    val file = new File("src/main/resources/input/Tabella_Generale_Codici_Profili_Standard.csv")
    val buffSourceFile = scala.io.Source.fromFile(file)

    val rowsList = buffSourceFile.getLines().map(_.split(CSV_DELIMITER)).toList


    val parametriCarattProfPrelList = rowsList.slice(1, rowsList.length)
      .map(line => ParametriCarattProfPrel(prof = line(0), b1prof = line(1).replace(',','.').toDouble,
        b2prof = line(2).replace(',','.').toDouble, b3prof = line(3).replace(',','.').toDouble, b4prof = line(4).replace(',','.').toDouble,
        categoriaUso = line(5), zonaClimatica = line.lift(6).filter(_.nonEmpty), ClassePrelievo = line(7)))

    parametriCarattProfPrelList
  }
  /*
  def get(TFCFiles: RDD[TFCMetadata]): DataFrame = {
    val rdd = TFCFiles.map(tfcMetaRow => {(tfcMetaRow.file.getName, tfcMetaRow.yearDir, tfcMetaRow.monthDir, tfcMetaRow.pivaRdb, tfcMetaRow.annoMese, tfcMetaRow.progressivo, tfcMetaRow.lastModified)})
    //val rdd = TFCFiles.map(vpgMetaRow => {(vpgMetaRow.file.getName, vpgMetaRow.yearDir, vpgMetaRow.monthDir, vpgMetaRow.pivaRdb, vpgMetaRow.annoMese, vpgMetaRow.lastModified)})

    //val df = Environment.spark.createDataFrame(rdd).toDF("nomeFile", "anno", "mese", TFCFileSchema.piva_rdb, TFCFileSchema.annomese, TFCFileSchema.data_creazione)
    val df = Environment.spark.createDataFrame(rdd).toDF(TFCFileSchema.nome_file, "anno", "mese", TFCFileSchema.piva_rdb, TFCFileSchema.annomese,
      TFCFileSchema.progressivo, TFCFileSchema.data_creazione)
    //val dfWithIDFile = df.withColumn("n_id_tsg2_file", hash(df.col("nomeFile"), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)
    val dfWithIDFile = df.withColumn(TFCFileSchema.n_id_TSG2_file, hash(df.col(TFCFileSchema.nome_file), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    dfWithIDFile.withColumn(TFCFileSchema.executionid, lit(Environment.executionId).cast(LongType))
      .selectExpr(TFCFileSchema.getValues: _*)

  }
   */
}
