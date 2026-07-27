package it.eng.au.aggiustamentoGas.model.rcugas

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class RcuGasPdrDatiPrelievo (
                                  nIdPdr: String,
                                  @deprecated("Use rcugas_var_prel_annuo_p", "31/05/2021") nPrelievoAnnuo: Option[Double],
                                  @deprecated("Use rcugas_var_profilo_p", "31/05/2021") tCodProfilo: Option[String],
                                  tAnno: String
                                ) {
  private val formatter = DateTimeFormat.forPattern("yyyy")
  private val year = formatter.parseDateTime(tAnno)

  lazy val startThermalYear: DateTime = year.minusMonths(3)
  lazy val endThermalYear: DateTime = year.plusMonths(9).minusDays(1)
}
