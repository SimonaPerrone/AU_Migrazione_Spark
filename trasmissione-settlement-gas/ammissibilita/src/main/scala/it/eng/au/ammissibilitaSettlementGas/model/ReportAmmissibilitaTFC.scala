package it.eng.au.ammissibilitaSettlementGas.model

import java.sql.Timestamp

case class ReportAmmissibilitaTFC(
                                   n_id_tsg2_file: Long,
                                   data: Option[String],
                                   id_reg_clim: Option[Long],
                                   wkr: Option[Double],
                                   numero_riga: Option[String],
                                   tipo_file: String,
                                   piva_utente: Option[String],
                                   verifica_amm: Boolean,
                                   cod_causale: String,
                                   motivazione: String,
                                   data_amm: Timestamp,
                                   executionid: Long,
                                   annomese: Option[String]
                                 ) extends Serializable
