package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udb

import it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.{PublishControllerTrait, PublishPreparationControllerTrait}
import it.eng.au.sgsFlussoStoricoGas.utility.constants.FieldConstants.UDB

trait UdbPublishControllerTrait extends PublishPreparationControllerTrait with PublishControllerTrait {
  override val nomeServizio: String = UDB
}
