package it.eng.au.aggiustamentoGas.model.rcugas

import org.joda.time.DateTime

case class RcuGasVarProfiloP(
                                nIdPdr: String,
                                dataInizio: DateTime,
                                dataFine: DateTime,
                                tCodProfilo: Option[String]
                              )



