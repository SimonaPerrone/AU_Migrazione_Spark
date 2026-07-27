package it.sferanet.au.model

case class RcuGasFields(
                         mindate: String,
                         maxdate: String,
                         t_codice_pdr: String,
                         t_cod_cat_uso: String,
                         t_cod_profilo: String,
                         n_coeff_correzione: String,
                         t_misuratore_integrato: String,
                         t_pre_conv: String,
                         n_num_cifre_misuratore: String,
                         n_num_cifre_convertitore: String
                       ) extends Serializable
