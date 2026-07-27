package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udd

import it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.{PublishControllerTrait, PublishPreparationControllerTrait}
import it.eng.au.sgsFlussoStoricoGas.utility.constants.FieldConstants.UDD

trait UddPublishControllerTrait extends PublishPreparationControllerTrait with PublishControllerTrait {
  override val nomeServizio: String = UDD
}
