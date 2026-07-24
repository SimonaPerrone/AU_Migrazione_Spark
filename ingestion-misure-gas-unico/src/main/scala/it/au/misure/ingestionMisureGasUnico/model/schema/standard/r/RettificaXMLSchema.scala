package it.au.misure.ingestionMisureGasUnico.model.schema.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum

object RettificaXMLSchema extends SchemaEnum {
  val
  FlussoMisure
  , IdentificativiFlusso
  , DatiPdr
  , DatiTecnPdrRett
  , DatiLetturaRett
  , LettureGiornaliereRett

  , piva_utente
  , piva_distr
  , cod_pdr
  , mese_comp
  , data_comp
  , tipo_rettifica
  , mot_ret_lett
  , causa_ostativa
  , data_prest
  , CodPrat_SII
  , Trattamento
  , matr_mis
  , matr_conv
  , coeff_corr
  , freq_let
  , vol_annuo_rettificato
  , data_racc
  , let_tot_prel
  , let_tot_conv
  , vol_ric
  , ini_periodo
  , fine_periodo
  , periodo_ric
  = Value

  val cod_flusso: RettificaXMLSchema.Value = Value("@cod_flusso")
}
