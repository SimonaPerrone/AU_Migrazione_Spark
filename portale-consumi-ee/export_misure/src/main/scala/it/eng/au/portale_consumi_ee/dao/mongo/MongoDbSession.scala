package it.eng.au.portale_consumi_ee.dao.mongo

import com.mongodb.client.model.Indexes
import com.mongodb.{MongoClient, MongoClientURI, MongoCommandException, MongoNamespace}
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import org.apache.log4j.Logger

object MongoDbSession {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

   val mongo: MongoClient = new MongoClient(new MongoClientURI(
//    s"${EnvironmentMisure.getProperty("spark.app.mongodbs.connstr.produzione")}/${EnvironmentMisure.getProperty("mongodbs.db.name")}"
     s"${EnvironmentMisure.getProperty("mongodb.db.uri")}"
   ))

  def cancellaCollezione(db: String, collection: String): Unit = {
    logger.warn(s"Cancellazione collezione $db.$collection")
    mongo.getDatabase(db).getCollection(collection).drop()
  }

  def creaIndice(db: String, collection: String, attributi: String*): Unit = {
    logger.warn(s"Creazione indice per ${attributi.mkString(", ")} su collezione $db.$collection")
    mongo
      .getDatabase(db)
      .getCollection(collection)
      .createIndex(Indexes.ascending(attributi: _*))
  }

  def rinominaCollezione(db: String, collection: String, nuovoNome: String, nuovoDb: String = null): Unit = {
    val dbFinale = if (nuovoDb == null) db else nuovoDb
    val mongoNamespace = new MongoNamespace(dbFinale, nuovoNome)
    try {
      mongo
        .getDatabase(db)
        .getCollection(collection)
        .renameCollection(mongoNamespace)
    } catch {
      case _: MongoCommandException => logger.warn(s"Collezione $db.$collection non trovata, nessuna rinomina")
    }
  }

}
