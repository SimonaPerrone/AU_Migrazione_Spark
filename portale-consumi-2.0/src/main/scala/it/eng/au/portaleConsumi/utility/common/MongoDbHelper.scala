package it.eng.au.portaleConsumi.utility.common

import org.bson.{BsonNull, BsonString, BsonValue}

object MongoDbHelper {

  /** *
   * Converte nel tipo corretto BSON una stinga, gestendo il caso sia nulla (che richiede un tipo diverso BSON)
   */
  def bsonValue(value: String): BsonValue = {
    if (value == null) BsonNull.VALUE else new BsonString(value)
  }

}
