package it.eng.au.portale_consumi_ee.calcolo.forniture_ele_1

import it.eng.au.portale_consumi_ee.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureModel
import it.eng.au.portale_consumi_ee.model.rcu._
import it.eng.au.portale_consumi_ee.model.rcus.{RcusFornituraPModel, RcusPodstatoPModel}
import it.eng.au.portale_consumi_ee.model.swtch.SwtchPrtSePModel
import it.eng.au.portale_consumi_ee.model.userappl.UserapplT001AppPrtPratichePModel
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_0_trasformations, forniture_ele_1_trasformations}

class calcolo_switch  extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

   //test calcolo_fornitura
  def testCalcoloFornitura(): Unit = {
    val dsSwtchPrtSeP = Seq(SwtchPrtSePModel()).toDS()
    val dsUserapplT001AppPrtPraticheP = Seq(UserapplT001AppPrtPratichePModel()).toDS()


    val switch = forniture_ele_1_trasformations.calcolo_switch(dsSwtchPrtSeP,dsUserapplT001AppPrtPraticheP)

    switch.show()
  }

}
