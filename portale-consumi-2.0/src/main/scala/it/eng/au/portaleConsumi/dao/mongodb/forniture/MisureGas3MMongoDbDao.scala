package it.eng.au.portaleConsumi.dao.mongodb.forniture

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import it.eng.au.portaleConsumi.utility.environment.Environment


class MisureGas3MMongoDbDao() extends MisureGasMongoDbDao {

  override val readConfig: ReadConfig = ReadConfig(Map(
    "uri" -> Environment.getProperty("mongodb.db.uri"),
    "database" -> Environment.getProperty("mongodb.db.name"),
    "collection" -> Environment.getProperty("mongodb.collection.misureGas3m"),
    "readPreference.name" -> Environment.getProperty("mongodb.conf.readPreference")
  ))

  override val writeConfig: WriteConfig = WriteConfig(Map(
    "uri" -> Environment.getProperty("mongodb.db.uri"),
    "database" -> Environment.getProperty("mongodb.db.name"),
    "collection" -> Environment.getProperty("mongodb.collection.misureGas3m"),
    "writeConcern.w" -> Environment.getProperty("mongodb.conf.writeConcern")
  ))

}
