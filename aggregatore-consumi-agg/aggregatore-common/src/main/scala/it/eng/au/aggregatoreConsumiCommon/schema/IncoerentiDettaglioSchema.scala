package it.eng.au.aggregatoreConsumiCommon.schema

object IncoerentiDettaglioSchema extends SchemaEnum {
  val
  //Aggregato
  data,
  cod_pdr,
  piva_distr,
  piva_it,
  piva_udd,
  piva_udb,
  piva_rdb,
  dtg,
  cod_remi,
  prel_annuo_prev,
  id_reg_clim,
  cod_prof_prel_std,
  trattamento,
  tipo_cliente,
  classe_gruppo_mis,
  un_mis_prel,
  prelievo_aggregato,
  giorno_sterilizzato,

  //Elenco flussi
  pdr,
  nomefile,
  sessione,
  annomese,
  let_tot_prel,
  let_tot_conv,
  data_lettura,
  tipo_lettura,
  matr_mis,
  matr_conv,
  coeff_cor,
  mot_ret_lett,
  cau_int_mis,
  cau_int_cor
  = Value
}
