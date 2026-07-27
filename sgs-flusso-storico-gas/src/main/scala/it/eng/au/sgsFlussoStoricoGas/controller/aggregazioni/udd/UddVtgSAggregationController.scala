package it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udd

import it.eng.au.sgsFlussoStoricoGas.utility.constants.{TipoFlusso, TipoPratica}

class UddVtgSAggregationController extends UddAggregationController {
  override val tipoFlusso: String = TipoFlusso.S.toString
  override val tipoPratica: String = TipoPratica.VTG.toString
}
