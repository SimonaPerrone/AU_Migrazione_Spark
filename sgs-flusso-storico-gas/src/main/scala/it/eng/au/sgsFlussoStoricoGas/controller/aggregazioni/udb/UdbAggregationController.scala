package it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udb

import it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.AggregationControllerTrait
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.utility.constants.FieldConstants.UDB

trait UdbAggregationController extends AggregationControllerTrait {
  override val nomeServizio: String = UDB
  override val pivaUtenteDest: String = SgsPerimetroSchema.piva_udb_entrante.toString
}