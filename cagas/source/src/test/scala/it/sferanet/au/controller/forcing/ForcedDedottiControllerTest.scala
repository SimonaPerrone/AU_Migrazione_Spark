package it.sferanet.au.controller.forcing

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.schema.PdrMassivoSchema
import it.sferanet.au.utilities.Environment

class ForcedDedottiControllerTest extends EnvironmentSparkTest {

  def testForcing(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val rcuGasMassivo = Seq(
      ("pdr1", "1000", null.asInstanceOf[String]),
      ("pdr2", "2000", null.asInstanceOf[String]),
      ("pdr3", "3000", null.asInstanceOf[String])
    ).toDF(PdrMassivoSchema.codice_pdr, PdrMassivoSchema.n_prelievo_annuo, PdrMassivoSchema.prelievo_annuo_prev_forced)

    ForcedDedottiController.forcing(rcuGasMassivo).show
  }
}
