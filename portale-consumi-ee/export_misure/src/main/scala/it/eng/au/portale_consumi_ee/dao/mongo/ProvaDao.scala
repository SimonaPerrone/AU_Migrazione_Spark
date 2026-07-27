package it.eng.au.portale_consumi_ee.dao.mongo

import com.mongodb.client.{MongoClients, MongoCollection}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import it.eng.au.portale_consumi_ee.dao.mongo.Forniture.MongoDbDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.Forniture.MisureElettricheModel
import it.eng.au.portale_consumi_ee.trasformations.BSON.FromDatasetToBson
import org.apache.spark.sql.Dataset
import org.bson.BsonDocument


//todo MisureElettricheModel is not the proper model, there is no _id
class ProvaDao  extends MongoDbDao[MisureElettricheModel]{

  override val readConfig: ReadConfig = ReadConfig(Map(
    "uri" -> EnvironmentMisure.getProperty("mongodb.db.uri"),
    "database" -> EnvironmentMisure.getProperty("mongodb.db.name"),
    "collection" -> EnvironmentMisure.getProperty("mongodb.collection.prova.uno"),
    "readPreference.name" -> EnvironmentMisure.getProperty("mongodb.conf.readPreference")
  ))

  override val writeConfig: WriteConfig = WriteConfig(Map(
    "uri" -> EnvironmentMisure.getProperty("mongodb.db.uri"),
    "database" -> EnvironmentMisure.getProperty("mongodb.db.name"),
    "collection" -> EnvironmentMisure.getProperty("mongodb.collection.prova.uno"),
    "writeConcern.w" -> EnvironmentMisure.getProperty("mongodb.conf.writeConcern"),
    "replaceDocument" -> "false"
  ))

  override def writeToMongo(dataset: Dataset[MisureElettricheModel], db: String, collectionName: String,overwrite: Boolean,mongoUri: String): Unit = {}
}
