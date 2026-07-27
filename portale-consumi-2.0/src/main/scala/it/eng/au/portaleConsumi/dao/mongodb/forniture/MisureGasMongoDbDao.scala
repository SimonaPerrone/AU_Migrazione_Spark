package it.eng.au.portaleConsumi.dao.mongodb.forniture

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import it.eng.au.portaleConsumi.dao.mongodb.MongoDbSession
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.bson.BsonDocument

abstract class MisureGasMongoDbDao {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  val readConfig: ReadConfig
  val writeConfig: WriteConfig

  def indexes: List[List[String]] = List()

  def read(): DataFrame = {
    logger.warn(s"Lettura collezione ${writeConfig.databaseName}.${writeConfig.collectionName}")
    MongoSpark.load(Environment.getSpark, readConfig)
  }

  def write(rdd: RDD[BsonDocument], overwrite: Boolean = false): Unit = {
    if (!overwrite) {
      logger.warn(s"Salva in tabella  ${writeConfig.databaseName}.${writeConfig.collectionName}")
      MongoSpark.save(rdd, writeConfig)
    } else {
      // Salva in tabella temporanea e poi scambia con tabella originale
      val originalCollectionName = writeConfig.collectionName
      val tmpCollectionName = originalCollectionName + "_load_tmp"
      val tmpCollectionNameOriginal = originalCollectionName + "_swap_tmp"
      val tmpWriteConfig = writeConfig.withOption("collection", tmpCollectionName)
      for (index <- indexes) {
        logger.warn(s"Creazione indice ${index.mkString(", ")}")
        MongoDbSession.creaIndice(writeConfig.databaseName, tmpCollectionName, index: _*)
      }
      logger.warn(s"Salva in tabella temporanea ${writeConfig.databaseName}.$tmpCollectionName")
      MongoSpark.save(rdd, tmpWriteConfig)
      logger.warn(s"Rinomina collezione $originalCollectionName a $tmpCollectionNameOriginal")
      MongoDbSession.rinominaCollezione(writeConfig.databaseName, originalCollectionName, tmpCollectionNameOriginal)
      logger.warn(s"Rinomina collezione $tmpCollectionName a $originalCollectionName")
      MongoDbSession.rinominaCollezione(writeConfig.databaseName, tmpCollectionName, originalCollectionName)
      logger.warn(s"Cancellazione collezione ${writeConfig.databaseName}.$tmpCollectionNameOriginal")
      MongoDbSession.cancellaCollezione(writeConfig.databaseName, tmpCollectionNameOriginal)
    }
  }

}
