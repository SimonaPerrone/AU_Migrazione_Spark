package it.au.misure.calcolo_capacita.component.hdao

import it.au.misure.calcolo_capacita.component.hdao.model._

case class CalcoloCtcFlowInputHDao() {

  def getAnagraficaHDao: AnagraficaHDao = AnagraficaHDao()

  def getPerimetroPdrHDao: PerimetroPdrHDao = PerimetroPdrHDao()

  def getCalcoloConsumiSbgHDao: CalcoloConsumiSbgHDao = CalcoloConsumiSbgHDao()

  def getClgPdrCapacitaHDao: ClgPdrCapacitaHDao = ClgPdrCapacitaHDao()

  def getRCUGasMassivoPHDao: RCUGasMassivoPHDao = RCUGasMassivoPHDao()
}
