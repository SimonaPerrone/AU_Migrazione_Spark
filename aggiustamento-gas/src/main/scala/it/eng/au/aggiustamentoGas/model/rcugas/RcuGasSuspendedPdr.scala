package it.eng.au.aggiustamentoGas.model.rcugas

import org.joda.time.DateTime

case class RcuGasSuspendedPdr(
                               nIdPdr: String,
                               dataIniSosp: DateTime,
                               dataFineSosp: DateTime
                             ) {
}
