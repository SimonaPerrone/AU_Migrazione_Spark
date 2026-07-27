package it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udb

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UdbUigAAggregationController extends UdbAggregationController {
  override val tipoFlusso: String = TipoFlusso.A.toString
  override val tipoPratica: String = TipoPratica.UIG.toString
}
