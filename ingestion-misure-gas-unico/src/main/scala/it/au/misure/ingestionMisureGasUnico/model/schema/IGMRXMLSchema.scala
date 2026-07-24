package it.au.misure.ingestionMisureGasUnico.model.schema

object IGMRXMLSchema extends SchemaEnum {
  val
  FlussoIGMR
  , IdentificativiFlusso
  , DatiPdR
  , cod_PdR
  , cau_int_mis
  , cau_int_cor
  , data_misura
  , causa_ostativa
  , mot_ret_lett

  , piva_utente
  , piva_distr
  , rinuncia_verifica
  , causa_stima
  , matr_mis
  , classe_gruppo_mis
  , tipo_mis
  , telegestione
  , pre_conv
  , matr_conv
  , n_cifre_conv
  , anno_fabb_conv
  , data_inst_conv
  , coeff_corr
  , press_misura
  , acc_mis
  , n_cifre_mis
  , anno_fabb_mis
  , data_inst_mis
  , gruppo_mis_int
  , let_misuratore
  , let_correttore
  , data_inservizio_sm
  = Value

  val Pre_int: IGMRXMLSchema.Value = Value("Pre-int")
  val Post_int: IGMRXMLSchema.Value = Value("Post-int")
  val cod_flusso: IGMRXMLSchema.Value = Value("@CodFlusso")

}
