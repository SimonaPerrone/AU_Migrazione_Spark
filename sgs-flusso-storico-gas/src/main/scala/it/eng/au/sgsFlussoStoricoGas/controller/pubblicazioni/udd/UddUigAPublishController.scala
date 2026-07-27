package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udd

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UddUigAPublishController extends UddPublishControllerTrait {
  override val tipoFlusso: String = TipoFlusso.A.toString
  override val tipoPratica: String = TipoPratica.UIG.toString
}
