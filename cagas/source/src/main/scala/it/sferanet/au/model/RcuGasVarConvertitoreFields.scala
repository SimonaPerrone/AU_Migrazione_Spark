package it.sferanet.au.model

case class RcuGasVarConvertitoreFields(
                                        mindate: String,
                                        maxdate: String,
                                        t_codice_pdr: String,
                                        t_pre_conv: String,
                                        n_num_cifre_convertitore: String
                                      ) extends Serializable
