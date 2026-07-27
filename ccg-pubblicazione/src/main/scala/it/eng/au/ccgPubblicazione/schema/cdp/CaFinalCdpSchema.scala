package it.eng.au.ccgPubblicazione.schema.cdp

import it.eng.au.ccgPubblicazione.schema.SchemaEnum

object CaFinalCdpSchema extends SchemaEnum{
  val codice_pdr,
  piva_distr,// TODO da aggiungerla nel calcolo
  piva_udd,// TODO da aggiungerla nel calcolo
  piva_udb,// TODO da aggiungerla nel calcolo
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
  executionid
  = Value
}
