package it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udd

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UddUigAAggregationController extends UddAggregationController {
  override val tipoFlusso: String = TipoFlusso.A.toString
  override val tipoPratica: String = TipoPratica.UIG.toString
}
