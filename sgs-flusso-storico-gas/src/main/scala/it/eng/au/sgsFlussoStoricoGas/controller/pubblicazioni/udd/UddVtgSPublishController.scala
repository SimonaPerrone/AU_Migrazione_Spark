package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udd

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UddVtgSPublishController extends UddPublishControllerTrait {
  override val tipoFlusso: String = TipoFlusso.S.toString
  override val tipoPratica: String = TipoPratica.VTG.toString
}
