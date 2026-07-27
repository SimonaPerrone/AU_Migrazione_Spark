package it.eng.au.portale_consumi_ee.dao.mongo.Forniture


import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import it.eng.au.portale_consumi_ee.dao.mongo.MongoDbSession
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import com.mongodb.client.model.Filters.lte
import org.mongodb.scala.model.Filters._
import scala.reflect.runtime.universe.TypeTag

abstract class MongoDbDao[T <: Product : TypeTag] {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  val readConfig: ReadConfig
  val writeConfig: WriteConfig

  def indexes: List[List[String]] = List()

  def read(): Dataset[T] = {
    val spark = EnvironmentMisure.getSpark
    import spark.implicits._

    logger.warn(s"Lettura collezione ${writeConfig.databaseName}.${writeConfig.collectionName}")
    MongoSpark.load(EnvironmentMisure.getSpark, readConfig)
      .as[T]
  }

  def write(dataset: Dataset[T],mongoUri:String, overwrite: Boolean = false): Unit = {
    val dbName = writeConfig.databaseName
    val originalCollectionName = writeConfig.collectionName

    writeToMongo(dataset.asInstanceOf[Dataset[T]], dbName, originalCollectionName,overwrite,mongoUri)
//
//    if (!overwrite) {
//      logger.warn(s"Salva in tabella  ${writeConfig.databaseName}.${writeConfig.collectionName}")
//      writeToMongo(dataset.asInstanceOf[Dataset[T]], dbName, originalCollectionName,overwrite,mongoUri)
//    } else {
//      // Salva in tabella temporanea e poi scambia con tabella originale
//      val originalCollectionName = writeConfig.collectionName
//      val tmpCollectionName = originalCollectionName + "_load_tmp"
//      val tmpCollectionNameOriginal = originalCollectionName + "_swap_tmp"
//      val tmpWriteConfig = writeConfig.withOption("collection", tmpCollectionName)
//      for (index <- indexes) {
//        logger.warn(s"Creazione indice ${index.mkString(", ")}")
//        MongoDbSession.creaIndice(writeConfig.databaseName, tmpCollectionName, index: _*)
//      }
//      logger.warn(s"Salva in tabella temporanea ${writeConfig.databaseName}.$tmpCollectionName")
//      // Write to temp collection using writeToMongo
//      writeToMongo(dataset.asInstanceOf[Dataset[T]], dbName, tmpCollectionName,overwrite,mongoUri)
//      logger.warn(s"Rinomina collezione $originalCollectionName a $tmpCollectionNameOriginal")
//      MongoDbSession.rinominaCollezione(writeConfig.databaseName, originalCollectionName, tmpCollectionNameOriginal)
//      logger.warn(s"Rinomina collezione $tmpCollectionName a $originalCollectionName")
//      MongoDbSession.rinominaCollezione(writeConfig.databaseName, tmpCollectionName, originalCollectionName)
//      logger.warn(s"Cancellazione collezione ${writeConfig.databaseName}.$tmpCollectionNameOriginal")
//      MongoDbSession.cancellaCollezione(writeConfig.databaseName, tmpCollectionNameOriginal)
//    }
  }

  def removeData(yearMonth: Int): Boolean = {
    val collection = writeConfig.collectionName
    val idPattern = s".*_${yearMonth}" // Matches _yyyyMM pattern

    val mongoClient = MongoDbSession.mongo

    // Start a client session
    val session = mongoClient.startSession()

    try {
      // Start transaction
      session.startTransaction()
      val db = mongoClient.getDatabase(writeConfig.databaseName)
      val originalColl = db.getCollection(collection)
      val deleteResultFuture = originalColl.deleteMany(regex("_id", idPattern))


      logger.warn(s"Deleted ${deleteResultFuture.getDeletedCount} documents from $collection")
      // Commit the transaction if delete is successful
      session.commitTransaction()
      logger.warn("Transaction committed successfully.")
      return true
    }
    catch {
      case e: Exception =>
        println(s"Transaction failed: ${e.getMessage}")
        session.abortTransaction()
        return false
    } finally {
      // End session
      session.close()
    }
  }

  def removeOldPartition(yearMonth: Int): Boolean = {
    val collection = writeConfig.collectionName

    val mongoClient = MongoDbSession.mongo

    // Start a client session
    val session = mongoClient.startSession()

    try {
      // Start transaction
      session.startTransaction()
      val db = mongoClient.getDatabase(writeConfig.databaseName)
      val originalColl = db.getCollection(collection)
      val deleteResultFuture = originalColl.deleteMany(equal("competenza_consumi", yearMonth))


      logger.warn(s"Deleted ${deleteResultFuture.getDeletedCount} documents from $collection")
      // Commit the transaction if delete is successful
      session.commitTransaction()
      logger.warn("Transaction committed successfully.")
      return true
    }
    catch {
      case e: Exception =>
        println(s"Transaction failed: ${e.getMessage}")
        session.abortTransaction()
        return false
    } finally {
      // End session
      session.close()
    }
  }

  def removeOldPartitionsUpTo(yearMonth: Int): Boolean = {
    val collection = writeConfig.collectionName
    val mongoClient = MongoDbSession.mongo

    // Start a client session
    val session = mongoClient.startSession()

    try {
      // Start transaction
      session.startTransaction()
      val db = mongoClient.getDatabase(writeConfig.databaseName)
      val originalColl = db.getCollection(collection)
      // Use $lte to match all documents where competenza_consumi <= yearMonth
      val deleteResult = originalColl.deleteMany(lte("competenza_consumi", yearMonth))

      logger.warn(s"Deleted ${deleteResult.getDeletedCount} documents from $collection")

      // Commit the transaction
      session.commitTransaction()
      logger.warn("Transaction committed successfully.")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Transaction failed: ${e.getMessage}", e)
        session.abortTransaction()
        false
    } finally {
      session.close()
    }
  }

  def removeOldPartitionsGreaterThan(yearMonth: Int): Boolean = {
    val collection = writeConfig.collectionName
    val mongoClient = MongoDbSession.mongo

    // Start a client session
    val session = mongoClient.startSession()

    try {
      // Start transaction
      session.startTransaction()
      val db = mongoClient.getDatabase(writeConfig.databaseName)
      val originalColl = db.getCollection(collection)
      // Use $lte to match all documents where competenza_consumi <= yearMonth
      val deleteResult = originalColl.deleteMany(gt("competenza_consumi", yearMonth))

      logger.warn(s"Deleted ${deleteResult.getDeletedCount} documents from $collection")

      // Commit the transaction
      session.commitTransaction()
      logger.warn("Transaction committed successfully.")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Transaction failed: ${e.getMessage}", e)
        session.abortTransaction()
        false
    } finally {
      session.close()
    }
  }


  def moveDataToBackup(yearMonth: Int): Unit = {
    val spark = EnvironmentMisure.getSpark
    import spark.implicits._

    val collection = writeConfig.collectionName
    val backupCollection = collection + "_backup_" + yearMonth
    val idPattern = s".*_${yearMonth}" // Matches _yyyyMM pattern

    // Step 1: Identify data to be moved
    val dataToMove = read()
      .filter(col("_id").rlike(idPattern))

    if (!dataToMove.isEmpty) {
      logger.warn(s"Moving data from $collection to $backupCollection")
      val backupWriteConfig = writeConfig.withOption("collection", backupCollection)
      MongoSpark.save(dataToMove, backupWriteConfig)
    }
  }
  def saveWithUpdate(dataset: Dataset[T]): Unit = {
    val spark = EnvironmentMisure.getSpark
    import spark.implicits._

    logger.warn(s"Upserting dataset into ${writeConfig.collectionName}")
    val writeConfigWithUpsert = writeConfig.withOptions(Map("replaceDocument" -> "false", "upsert" -> "true"))
    MongoSpark.save(dataset, writeConfigWithUpsert)
  }

  def writeToMongo(dataset: Dataset[T], db: String, collectionName: String,overwrite: Boolean,mongoUri:String): Unit


}

