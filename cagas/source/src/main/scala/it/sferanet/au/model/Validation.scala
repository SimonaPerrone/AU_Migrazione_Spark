package it.sferanet.au.model

import java.sql.Timestamp

case class Validation(
                       service: String,
                       pdr: String,
                       dat: Timestamp,
                       measure: Option[Double],
                       converted: Option[Double],
                       readtype: String,
                       serialnumbermis: Option[String],
                       serialnumberconv: Option[String],
                       timestamplocalfile: Timestamp,
                       d_caricamento: Int,
                       local_file: Option[String],
                       cat_uso: String,
                       classe_prelievo: String,
                       data_creazione: Timestamp,
                       motivazione_rettifica: Option[Int],
                       cau_int_mis: Option[Int],
                       cau_int_cor: Option[Int],
                       file_rettifica: Option[String],
                       n_coeff_correzione: Option[Double],
                       session: String,
                       executionid: Long
                     )
