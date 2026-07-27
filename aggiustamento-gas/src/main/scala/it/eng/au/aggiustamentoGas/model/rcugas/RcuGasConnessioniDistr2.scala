package it.eng.au.aggiustamentoGas.model.rcugas

import org.joda.time.DateTime

case class RcuGasConnessioniDistr2(
                                    tCodicePdr: String,
                                    nIdDistr: String,
                                    tRemi: String,
                                    dataInizioConn: DateTime,
                                    dataFineConn: DateTime,
                                    idRegioneClimatica: Option[Int]
                                  )
