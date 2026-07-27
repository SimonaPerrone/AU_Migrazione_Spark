package it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udb

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UdbSwgSAggregationController extends UdbAggregationController {
  override val tipoFlusso: String = TipoFlusso.S.toString
  override val tipoPratica: String = TipoPratica.SWG.toString
}
