package it.au.misure.ingestionMisureGasUnico.model.schema.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum

object MisuraXMLSchema extends SchemaEnum {
  val
  FlussoMisure
  , IdentificativiFlusso
  , DatiPdr
  , DatiTecnPdr
  , DatiLettura
  , LettureGiornaliere

  , piva_utente
  , piva_distr
  , cod_pdr
  , mese_comp
  , data_prest
  , CodPrat_SII
  , Trattamento
  , matr_mis
  , matr_conv
  , n_cifre_mis
  , n_cifre_conv
  , coeff_corr
  , freq_let
  , acc_mis
  , Raccolta
  , esito_raccolta
  , causa_manc_raccolta
  , mod_alt_racc
  , dir_indennizzo
  , pros_fin_inizio
  , pros_fin_fine
  , vol_annuo_sost
  , classe_gruppo_mis
  , pre_conv
  , gruppo_mis_int
  , tipo_lettura
  , esito_val
  , Note
  , num_tentativi
  , data_racc
  , let_tot_prel
  , let_tot_conv
  , data_mis_eff
  , segn_mis_eff
  , segn_conv_eff
  , data_comp
  = Value

  val cod_flusso: MisuraXMLSchema.Value = Value("@cod_flusso")
}
