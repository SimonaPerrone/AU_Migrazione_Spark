package it.eng.au.portale_consumi_ee.flow.FornitureInfo

import it.eng.au.portale_consumi_ee.common.flow.{FlowUnitOutput}
import it.eng.au.portale_consumi_ee.dao.mongodbs.{SwitchDao}
import it.eng.au.portale_consumi_ee.dao.swtch.SwtchPrtSePDao
import it.eng.au.portale_consumi_ee.dao.userappl.UserapplT001AppPrtPratichePDao
import it.eng.au.portale_consumi_ee.model.mongodbs.SwitchModel
import it.eng.au.portale_consumi_ee.schema.swtch.SwtchPrtSePSchema
import it.eng.au.portale_consumi_ee.schema.userappl.UserapplT001AppPrtPratichePSchema
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_1_trasformations}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

case class fornitura_ele_1(implicit spark: SparkSession)  extends FlowUnitOutput{

  //hdao input tables
  val getSwtchPrtSePDao = new SwtchPrtSePDao
  val getUserapplT001AppPrtPratichePDao = new UserapplT001AppPrtPratichePDao

  //hdao output tables
  val getSwitchDao = new SwitchDao

   def runSwitch(): Dataset[SwitchModel] = {

    //dataset input
    def dsSwtchPrtSeP =  getSwtchPrtSePDao.read().repartition(col(SwtchPrtSePSchema.t_codice_pod))
    def dsUserapplT001AppPrtPraticheP = getUserapplT001AppPrtPratichePDao.read().repartition(col(UserapplT001AppPrtPratichePSchema.n_id_pratica))

    logger.info(s"Inizio calcolo switch")
    //calcolo forniture
    val switch = forniture_ele_1_trasformations.calcolo_switch(dsSwtchPrtSeP,dsUserapplT001AppPrtPraticheP)
    logger.info(s"Fine calcolo switch")
    //todo necessario?
    logger.info(s"Inizio scrittura mongodbs.switch")
    getSwitchDao.write(switch,true)
    logger.info(s"Fine scrittura mongodbs.switch")

     switch
  }

}
