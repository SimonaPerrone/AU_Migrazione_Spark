package it.eng.au.portale_consumi_ee.schema.Forniture

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object MisureElettricheSchema extends SchemaEnum {
  val
  _id,
  codice_fornitura,
  competenza_consumi,
  pod,
  misure_orarie,
  misure_mensili,
  misure_non_orarie,
  volture,
  autoletture
  = Value
}
