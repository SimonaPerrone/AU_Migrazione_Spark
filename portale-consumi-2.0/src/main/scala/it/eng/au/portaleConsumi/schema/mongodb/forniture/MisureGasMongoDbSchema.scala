package it.eng.au.portaleConsumi.schema.mongodb.forniture

import it.eng.au.portaleConsumi.schema.SchemaEnum

object MisureGasMongoDbSchema extends SchemaEnum {

  val
  _id,
  codice_fornitura,
  pdr,
  misure,
  misure_giornaliere,
  misure_mensili,
  misure_altre_frequenze,
  volture,
  autoletture,
  competenza_consumi,
  data_lettura,
  delta_misure,
  lettura_mese,
  lettura_giorno,
  lettura_misura,
  tipo_misura
  = Value
}
