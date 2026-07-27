package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udb

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UdbVtgSPublishController extends UdbPublishControllerTrait {
  override val tipoFlusso: String = TipoFlusso.S.toString
  override val tipoPratica: String = TipoPratica.VTG.toString
}
