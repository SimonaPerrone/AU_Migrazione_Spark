package it.eng.au.pubblicazionePcg.dao.sbg

import it.eng.au.pubblicazionePcg.dao.DAO
import it.eng.au.pubblicazionePcg.schema.SbgMisureSchema

import java.util.Properties

class SbgMisureDAO(implicit prop: Properties) extends DAO {
  override val tableName: String = prop.getProperty("sbgmisure.hdfs.path")
  override val hdfsOutput: String = prop.getProperty("sbgmisure.hdfs.path")
  override val partitionColumn: String = SbgMisureSchema.annomese_rif
  override val partitionValue: String = prop.getProperty("year.month")
  override val columns: List[String] = SbgMisureSchema.getValues
  val repartitionFactor = 80
}
