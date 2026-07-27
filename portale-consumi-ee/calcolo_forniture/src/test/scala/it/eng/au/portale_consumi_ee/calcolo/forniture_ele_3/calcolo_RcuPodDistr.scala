package it.eng.au.portale_consumi_ee.calcolo.forniture_ele_3

import it.eng.au.portale_consumi_ee.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuPodDistrPModel}
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_3_trasformations}

class calcolo_RcuPodDistr extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

   //test calcolo_fornitura
  def testCalcoloFornitura(): Unit = {

    val dsRcuPodDistrPDao = Seq(RcuPodDistrPModel()).toDS()
    val dsRcuAziendaPDao = Seq(RcuAziendaPModel()).toDS()


    val rcuPodDistr = forniture_ele_3_trasformations.calcolo_RcuPodDistr(dsRcuPodDistrPDao,dsRcuAziendaPDao)

    rcuPodDistr.show()
  }

}
