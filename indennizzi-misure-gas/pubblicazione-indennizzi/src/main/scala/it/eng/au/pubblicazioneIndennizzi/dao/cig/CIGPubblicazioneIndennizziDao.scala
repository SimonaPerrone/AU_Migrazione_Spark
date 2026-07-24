package it.eng.au.pubblicazioneIndennizzi.dao.cig
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.model.CIGPubblicazioneIndennizzi
import it.eng.au.pubblicazioneIndennizzi.schema.CIGPubblicazioneIndennizziSchema
import it.eng.au.pubblicazioneIndennizzi.utility.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


class CIGPubblicazioneIndennizziDao extends WriteDao {
  override val tableName: String = Properties.getPubblicazioneIndennizziTableName
  override val parquetPath: String = Properties.getPubblicazioneIndennizziParquetPath
  override val columns: List[String] = CIGPubblicazioneIndennizziSchema.getValues

  def writeParquet(reportPubblicazioneIndennizzi: RDD[CIGPubblicazioneIndennizzi]): Unit = {
    val df = Environment.sqlContext.createDataFrame(reportPubblicazioneIndennizzi)
    writeParquet(df)

    if (!Environment.isLocal) Environment.sqlContext.sql(s"MSCK REPAIR TABLE $tableName")
  }
}