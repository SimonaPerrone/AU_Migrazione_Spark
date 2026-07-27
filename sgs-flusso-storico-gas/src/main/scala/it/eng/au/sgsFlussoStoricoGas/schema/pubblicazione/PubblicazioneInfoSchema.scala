package it.eng.au.sgsFlussoStoricoGas.schema.pubblicazione

import it.eng.au.sgsFlussoStoricoGas.schema.SchemaEnum

object PubblicazioneInfoSchema extends SchemaEnum {
  val
  id_pubblicazione_info,
  nome_flusso,
  piva_utente_dest,
  tipo_dest,
  path,
  nome_file,
  seq,
  xsd_validated,
  anno_mese_decorrenza,
  execution_id_agg_sgs,
  data_pubblicazione,
  execution_id
  = Value
}
