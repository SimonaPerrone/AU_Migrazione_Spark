package it.eng.au.portaleConsumi.dao.mongodb

import com.mongodb.client.model.Indexes
import com.mongodb.{MongoClient, MongoClientURI, MongoCommandException}
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.mongodb.scala.MongoNamespace

object MongoDbSession {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  private val mongo: MongoClient = new MongoClient(new MongoClientURI(
    s"${Environment.getProperty("mongodb.client.uri")}/${Environment.getProperty("mongodb.db.name")}"))

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
