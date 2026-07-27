package it.eng.au.aggiustamentoGas.model.rcugas

import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import org.joda.time.DateTime

case class RcuGasVarTrattamentoP(
                                  codicePdr: String,
                                  dataInizio: DateTime,
                                  dataFine: DateTime,
                                  tTrattamentoSettlement: Treatment.Value
                              )



