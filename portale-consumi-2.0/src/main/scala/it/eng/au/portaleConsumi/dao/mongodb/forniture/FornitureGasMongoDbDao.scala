package it.eng.au.portaleConsumi.dao.mongodb.forniture

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import it.eng.au.portaleConsumi.dao.mongodb.MongoDbDao
import it.eng.au.portaleConsumi.model.mongodb.forniture.FornitureGasMongoDbModel
import it.eng.au.portaleConsumi.schema.mongodb.forniture.FornitureGasMongoDbSchema
import it.eng.au.portaleConsumi.utility.environment.Environment


class FornitureGasMongoDbDao() extends MongoDbDao[FornitureGasMongoDbModel] {

  override def indexes: List[List[String]] = List(List(FornitureGasMongoDbSchema.codice_fiscale.toString))

  override val readConfig: ReadConfig = ReadConfig(Map(
    "uri" -> Environment.getProperty("mongodb.db.uri"),
    "database" -> Environment.getProperty("mongodb.db.name"),
    "collection" -> Environment.getProperty("mongodb.collection.fornitureGas"),
    "readPreference.name" -> Environment.getProperty("mongodb.conf.readPreference")
  ))

  override val writeConfig: WriteConfig = WriteConfig(Map(
    "uri" -> Environment.getProperty("mongodb.db.uri"),
    "database" -> Environment.getProperty("mongodb.db.name"),
    "collection" -> Environment.getProperty("mongodb.collection.fornitureGas"),
    "writeConcern.w" -> Environment.getProperty("mongodb.conf.writeConcern")
  ))

}
