package it.sferanet.au.utility.filterPdrCreator.filter7ExclusioneFilePdrCaFinalTest

import it.sferanet.au.utility.filterPdrCreator
import it.sferanet.au.utility.filterPdrCreator.simpleCalculateFinalCaTest


case class Creator() extends filterPdrCreator.Creator {

  val creator: simpleCalculateFinalCaTest.Creator = it.sferanet.au.utility.filterPdrCreator.simpleCalculateFinalCaTest.Creator()

  override def createMeasures(): Unit = creator.createMeasures()


  override def createRcuTech(): Unit = creator.createRcuTech()

  override def createRcuGasMassivoCaP(): Unit = creator.createRcuGasMassivoCaP()

  override def createRcuGasMassivoP(): Unit = creator.createRcuGasMassivoP()

  override def createRcuGasConnessioniDistr2P(): Unit = creator.createRcuGasConnessioniDistr2P()

  override def createRcuGasBilanciamentoP(): Unit = creator.createRcuGasBilanciamentoP()

  override def createPrtIstatRegioneClimaticaP(): Unit = creator.createPrtIstatRegioneClimaticaP()

  override def createVRcuGasDistributoreP(): Unit = None

  override def createRcuAziendaP(): Unit = None

  override def createRcuGasUdbP(): Unit = None

  override def createSettleGasGasTds(): Unit = None

  override def createCaPreFinal(): Unit = None

  override def createCaFinal(): Unit = None

  override def createRcuGasVarProfilo(): Unit = creator.createRcuGasVarProfilo()

  override def createRcuGasVarTrattamento(): Unit = creator.createRcuGasVarTrattamento()

  override def createRcuGasVarPrelAnnuo(): Unit = creator.createRcuGasVarPrelAnnuo()
}
