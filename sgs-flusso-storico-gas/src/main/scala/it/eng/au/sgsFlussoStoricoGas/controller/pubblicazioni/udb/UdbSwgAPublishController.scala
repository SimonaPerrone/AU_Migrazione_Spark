package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udb

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UdbSwgAPublishController extends UdbPublishControllerTrait {
  override val tipoFlusso: String = TipoFlusso.A.toString
  override val tipoPratica: String = TipoPratica.SWG.toString
}
