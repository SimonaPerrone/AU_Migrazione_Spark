package it.eng.au.freezerPreCalcolo.freezer.flow

/** Abstract interface for execution modes. It only contains a method, [[runFreezer]], which
 * deploys the desired freezer process. */
trait RunnableFreezer {
  /** Performs the desired freezing procedure. */
  def runFreezer(): Unit
}
