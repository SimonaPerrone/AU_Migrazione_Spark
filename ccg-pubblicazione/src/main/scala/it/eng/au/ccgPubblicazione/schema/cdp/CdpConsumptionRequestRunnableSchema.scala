package it.eng.au.ccgPubblicazione.schema.cdp

import it.eng.au.ccgPubblicazione.schema.SchemaEnum

object CdpConsumptionRequestRunnableSchema extends SchemaEnum {
  val
  codice_pdr,
  piva_distr,
  piva_udd,
  piva_udb,
  codice_remi,
  cat_uso,
  classe_prelievo,
  zona_climatica,
  id_reg_clim,
  cod_prof_prel_std,
  prelievo_annuo_prev,
  trattamento,
  anno_competenza,//serve per la data
  tipo_trasmissione,
  session,
  idRichiesta,
//  dataRichiesta,
  pivaGestore
  = Value
}
