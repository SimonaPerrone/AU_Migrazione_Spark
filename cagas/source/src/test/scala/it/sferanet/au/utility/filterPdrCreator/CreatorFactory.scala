package it.sferanet.au.utility.filterPdrCreator

object CreatorFactory {

  val filter6CalculateCaFinalForcingTest: String = "Filter6CalculateCaFinalForcingTest"
  val simpleCalculateFinalCaTest: String = "SampleCalculateFinalCaTest"
  val filter7ExclusioneFilePdrCaFinalTest: String = "Filter7ExclusioneFilePdrCaFinalTest"
  val appTest: String = "AppTest"

  def getTestCreator(test: String): Creator = {

    val creator: Option[Creator] = test match {
      case CreatorFactory.appTest => Some(it.sferanet.au.utility.filterPdrCreator.appTest.Creator())
      case CreatorFactory.filter6CalculateCaFinalForcingTest => Some(it.sferanet.au.utility.filterPdrCreator.filter6CalculateCaFinalForcingTest.Creator())
      case CreatorFactory.filter7ExclusioneFilePdrCaFinalTest => Some(it.sferanet.au.utility.filterPdrCreator.filter7ExclusioneFilePdrCaFinalTest.Creator())
      case CreatorFactory.simpleCalculateFinalCaTest => Some(it.sferanet.au.utility.filterPdrCreator.simpleCalculateFinalCaTest.Creator())
      case _ =>
        None
    }

    if (creator.isEmpty) {
      println("error with test mock")
      System.exit(1)
    }
    writeInputFile(creator.get)
    creator.get

  }

  private def writeInputFile(creator: Creator): Unit = {
    creator.createMeasures()
    creator.createRcuTech()
    creator.createRcuGasMassivoCaP()
    creator.createRcuGasMassivoP()
    creator.createRcuGasConnessioniDistr2P()
    creator.createRcuGasBilanciamentoP()
    creator.createPrtIstatRegioneClimaticaP()
    creator.createVRcuGasDistributoreP()
    creator.createRcuAziendaP()
    creator.createRcuGasUdbP()
    creator.createSettleGasGasTds()
    creator.createCaFinal()
    creator.createCaPreFinal()
    creator.createRcuGasVarProfilo()
    creator.createRcuGasVarPrelAnnuo()
    creator.createRcuGasVarTrattamento()
  }
}
