package it.eng.au.aggiustamentoGas.utility.constants

object OptionMath {
  /**
   * @param x a term
   * @param y a term
   * @return x + y if both are defined else None
   */
  def sum(x: Option[Double], y: Option[Double]): Option[Double] = {
    if (x.isDefined && y.isDefined) Option(x.get + y.get)
    else None
  }

  /**
   * @param x a term
   * @param y a term
   * @return x - y if both are defined else None
   */
  def diff(x: Option[Double], y: Option[Double]): Option[Double] = {
    if (x.isDefined && y.isDefined) Option(x.get - y.get)
    else None
  }
  /**
   * @param x a term
   * @param y a term
   * @return x * y if both are defined else None
   */
  def mult(x: Option[Double], y: Option[Double]): Option[Double] = {
    if (x.isDefined && y.isDefined) Option(x.get * y.get)
    else None
  }

  /**
   * @param x a term
   * @param y a term
   * @return x/y if both are defined else None
   */
  def fract(x: Option[Double], y: Option[Double]): Option[Double] = {
    if (x.isDefined && y.isDefined) Option(x.get / y.get)
    else None
  }
}
