package it.eng.au.aggiustamentoGas.model.measure.im1Igmg

import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow

trait Post extends Flow {
  val coefCorr: Option[Double]
  val cau_int_mis: Option[Int]
  val cau_int_cor: Option[Int]
  val correctionFlow: Option[RettificaFlow]
}