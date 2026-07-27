package it.sferanet.au.model

import java.sql.Timestamp

case class ConsumptionOutput(
                              pdr: String,
                              startservice: String,
                              endservice: String,
                              startsegment: Timestamp,
                              endsegment: Timestamp,
                              var startvalue: Double,
                              var endvalue: Double,
                              idconsumptionerrorstate: Int,
                              n_coeff_correzione: Option[Double],
                              t_misuratore_integrato: Option[String],
                              end_t_misuratore_integrato: Option[String],
                              t_pre_conv: Option[String],
                              end_t_pre_conv: Option[String],
                              t_cod_prof: Option[String],
                              n_prelievo_annuo: Option[String],
                              tipo_coeff: String = null,
                              tipo_forzatura: Option[String] = None,
                              coerenza_dim: String = null,
                              session: String,
                              executionid: Long
                            )
