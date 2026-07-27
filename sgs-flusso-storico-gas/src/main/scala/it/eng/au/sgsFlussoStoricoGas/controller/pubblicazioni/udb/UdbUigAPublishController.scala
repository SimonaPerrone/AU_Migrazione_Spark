package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udb

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UdbUigAPublishController extends UdbPublishControllerTrait {
  override val tipoFlusso: String = TipoFlusso.A.toString
  override val tipoPratica: String = TipoPratica.UIG.toString
}
