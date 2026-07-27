package it.eng.au.portale_consumi_ee.dao.mongo

import com.mongodb.client.{MongoClients, MongoCollection}
import com.mongodb.client.model.{BulkWriteOptions, Filters, ReplaceOneModel, ReplaceOptions}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import it.eng.au.portale_consumi_ee.dao.mongo.Forniture.MongoDbDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.Forniture.MisureElettricheModel
import it.eng.au.portale_consumi_ee.trasformations.BSON.FromDatasetToBson
import org.apache.spark.sql.Dataset
import org.bson.BsonDocument
import scala.collection.JavaConverters._



//todo MisureElettricheModel is not the proper model, there is no _id
class MisureElettriche33MDao  extends MongoDbDao[MisureElettricheModel]{

  override val readConfig: ReadConfig = ReadConfig(Map(
    "uri" -> EnvironmentMisure.getProperty("mongodb.db.uri"),
    "database" -> EnvironmentMisure.getProperty("mongodb.db.name"),
    "collection" -> EnvironmentMisure.getProperty("mongodb.collection.misureElettriche33MNewSchema"),
    "readPreference.name" -> EnvironmentMisure.getProperty("mongodb.conf.readPreference")
  ))

  override val writeConfig: WriteConfig = WriteConfig(Map(
    "uri" -> EnvironmentMisure.getProperty("mongodb.db.uri"),
    "database" -> EnvironmentMisure.getProperty("mongodb.db.name"),
    "collection" -> EnvironmentMisure.getProperty("mongodb.collection.misureElettriche33MNewSchema"),
    "writeConcern.w" -> EnvironmentMisure.getProperty("mongodb.conf.writeConcern")
  ))

  override def writeToMongo(
                    dataset: Dataset[MisureElettricheModel],
                    db: String,
                    collectionName: String,
                    overwrite: Boolean,
                      mongoUri: String

  ): Unit = {
//    val mongoUri = EnvironmentMisure.getProperty("mongodb.db.uri")
//    require(mongoUri != null, "MongoDB URI is null. Check that 'mongodb.db.uri' is correctly loaded.")
//
//    val mongoUriBroadcast = dataset.sparkSession.sparkContext.broadcast(mongoUri)

    dataset.foreachPartition { partition =>
      val mongoClient = MongoClients.create(mongoUri)
      val collection: MongoCollection[BsonDocument] = mongoClient
        .getDatabase(db)
        .getCollection(collectionName, classOf[BsonDocument])

      val docs = partition.map(FromDatasetToBson.misureElettricheToBson).toList

      if (docs.nonEmpty) {
        if (overwrite) {
          val updates = docs.map { doc =>
            val filter = Filters.eq("_id", doc.get("_id"))
            val update = new ReplaceOneModel[BsonDocument](filter, doc, new ReplaceOptions().upsert(true))
            update
          }
          collection.bulkWrite(updates.asJava, new BulkWriteOptions().ordered(false))
        } else {
          collection.insertMany(docs.asJava)
        }
      }

      mongoClient.close()
    }
  }
}
