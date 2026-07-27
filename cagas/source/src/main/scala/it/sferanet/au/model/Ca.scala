package it.sferanet.au.model

import java.sql.Timestamp

case class Ca(
               pdr: String,
               startService: String,
               endService: String,
               startSegment: Timestamp,
               endSegment: Timestamp,
               startValue: Double,
               endValue: Double,
               idConsumptionErrorState: Int,
               idCaErrorCode: Int,
               caMethods: Int,
               codiceProfilo: String,
               id_regClim: Int,
               t_comune_istat_pdr: String,
               next_cod_profilo: String,
               profMode: Int,

               start_local_file: Option[String],
               end_local_file: Option[String],
               start_t_misuratore_integrato: Option[String],
               end_t_misuratore_integrato: Option[String],
               start_t_pre_conv: Option[String],
               end_t_pre_conv: Option[String],
               n_coeff_correzione: Option[String],

               cod_istat_last_rcu: Option[String],
               zona_climatica_lookup: Option[String],
               ce_mean: Option[Double],
               session: String,
               executionid: Long
             )
