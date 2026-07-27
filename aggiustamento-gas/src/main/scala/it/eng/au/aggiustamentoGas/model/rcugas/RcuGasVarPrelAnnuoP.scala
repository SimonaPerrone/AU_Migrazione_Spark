package it.eng.au.aggiustamentoGas.model.rcugas

import org.joda.time.DateTime

case class RcuGasVarPrelAnnuoP(
                                nIdPdr: String,
                                dataInizio: DateTime,
                                dataFine: DateTime,
                                nPrelivevoAnnuo: Option[Double]
                              )



