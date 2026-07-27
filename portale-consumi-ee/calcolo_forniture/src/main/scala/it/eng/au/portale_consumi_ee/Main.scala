package it.eng.au.portale_consumi_ee

import it.eng.au.portale_consumi_ee.common.Driver
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.flow.FornitureInfo.{fornitura_ele_0, fornitura_ele_1, fornitura_ele_2, fornitura_ele_3, fornitura_ele_4, fornitura_ele_7}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Main extends Driver{

  @transient  lazy val logger = Logger.getLogger(getClass.getName)

  override def run(args: Array[String]): Unit = {

    val job_properties = PropertyUtility.job_properties

    Environment.getOrCreate("portale_consumi_ee_calcolo_forniture",job_properties,false)

    implicit val spark: SparkSession =  Environment.getSpark

    def flowFornitura_ele_0 = new fornitura_ele_0()
    def flowFornitura_ele_1 = new fornitura_ele_1()
    def flowFornitura_ele_2 = new fornitura_ele_2()
    def flowFornitura_ele_3 = new fornitura_ele_3()
    def flowFornitura_ele_4 = new fornitura_ele_4()
    def flowFornitura_ele_7 = new fornitura_ele_7()

    val (fornitureDs,forniture_infoDs) = flowFornitura_ele_0.runForniture()

    val switchDs = flowFornitura_ele_1.runSwitch()

    val gdmDs = flowFornitura_ele_2.runGdm()

    val rcuPodDistrDs =flowFornitura_ele_3.runRcuPodDistr()

    val fasce = flowFornitura_ele_4.runFasce()

    flowFornitura_ele_7.runForniture()

    spark.stop()
  }
}
