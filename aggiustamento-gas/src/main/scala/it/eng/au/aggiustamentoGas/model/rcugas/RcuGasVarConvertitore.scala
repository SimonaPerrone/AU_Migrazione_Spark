package it.eng.au.aggiustamentoGas.model.rcugas

import org.joda.time.DateTime

case class RcuGasVarConvertitore(
                                  nIdPdr: String,
                                  tPreConv: Option[String] = None,
                                  startDateConv: DateTime,
                                  endDateConv: DateTime,
                                  nCifreConv: Option[Int] = None
                                )
