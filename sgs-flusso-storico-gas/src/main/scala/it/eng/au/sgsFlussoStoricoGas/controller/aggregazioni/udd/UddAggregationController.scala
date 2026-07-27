package it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udd

import it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.AggregationControllerTrait
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.utility.constants.FieldConstants.UDD

trait UddAggregationController extends AggregationControllerTrait {
  override val nomeServizio: String = UDD
  override val pivaUtenteDest: String = SgsPerimetroSchema.piva_udd_entrante.toString
}
