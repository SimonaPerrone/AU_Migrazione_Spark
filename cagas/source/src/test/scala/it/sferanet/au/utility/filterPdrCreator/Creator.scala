package it.sferanet.au.utility.filterPdrCreator


trait Creator {

  def createMeasures(): Unit

  def createRcuTech(): Unit

  def createRcuGasMassivoCaP(): Unit

  def createRcuGasMassivoP(): Unit

  def createRcuGasConnessioniDistr2P(): Unit

  def createRcuGasBilanciamentoP(): Unit

  def createPrtIstatRegioneClimaticaP(): Unit

  def createVRcuGasDistributoreP()

  def createRcuAziendaP()

  def createRcuGasUdbP()

  def createSettleGasGasTds()

  def createCaPreFinal()

  def createCaFinal()

  def createRcuGasVarProfilo()

  def createRcuGasVarTrattamento()

  def createRcuGasVarPrelAnnuo()
}
