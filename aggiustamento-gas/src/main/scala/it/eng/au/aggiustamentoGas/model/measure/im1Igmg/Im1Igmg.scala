package it.eng.au.aggiustamentoGas.model.measure.im1Igmg

import it.eng.au.aggiustamentoGas.model.measure.Flow

trait Im1Igmg extends Flow{
  val coefCorr: Option[Double] = None
  val cau_int_mis: Option[Int]
  val cau_int_cor: Option[Int]
  val pre: Pre
  val post: Post
  var sameDayFlow: Option[Flow]

  def setSameDayFlow(f: Flow): Flow = {
    this.sameDayFlow = Option(f)
    this
  }
}
