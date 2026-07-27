package it.eng.au.aggiustamentoGas.utility

import it.eng.au.aggiustamentoGas.model.measure.Flow

object UtilityFunctions {
  // Funzione chiave comune
  def makeKey(f: Flow) = (f.pdr, f.date)
  def makeKeyWithInfos(f: Flow) = (f.pdr, f.date, f.service, f.dataCaricamento)
}
