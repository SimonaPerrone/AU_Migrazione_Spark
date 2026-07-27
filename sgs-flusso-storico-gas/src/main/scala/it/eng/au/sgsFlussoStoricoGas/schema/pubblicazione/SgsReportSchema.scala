package it.eng.au.sgsFlussoStoricoGas.schema.pubblicazione

import it.eng.au.sgsFlussoStoricoGas.schema.SchemaEnum

object SgsReportSchema extends SchemaEnum {
  val
  cod_pdr,
  data_decorrenza_pratica,
  tipologia_pratica,
  pathfile,
  nome_file,
  piva_udd,
  piva_udb,
  piva_id,
  trattamento,
  anno_mese,
  prelievo_aggregato,
  data_pubblicazione
  = Value
}
