package it.eng.au.portale_consumi_ee.flow.FornitureInfo

import it.eng.au.portale_consumi_ee.common.flow.{FlowUnitOutput}
import it.eng.au.portale_consumi_ee.dao.mongodbs.MongoDbsRcuPodDistrDao
import it.eng.au.portale_consumi_ee.dao.rcu.{RcuAziendaPDao, RcuPodDistrPDao}
import it.eng.au.portale_consumi_ee.model.mongodbs.RcuPodDistrModel
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuPodDistrPSchema}
import it.eng.au.portale_consumi_ee.trasformation.forniture_ele_3_trasformations
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

case class fornitura_ele_3(implicit spark: SparkSession) extends FlowUnitOutput{

  //hdao input tables
  val getRcuPodDistrPDao = new RcuPodDistrPDao
  val getRcuAziendaPDao = new RcuAziendaPDao

  //hdao output tables
  val getRcuPodDistr = new MongoDbsRcuPodDistrDao

   def runRcuPodDistr():Dataset[RcuPodDistrModel] = {

    //dataset input
    def dsRcuPodDistrPDao =  getRcuPodDistrPDao.read().repartition(col(RcuPodDistrPSchema.n_id_distr))
    def dsRcuAziendaPDao = getRcuAziendaPDao.read()

    logger.info(s"Inizio calcolo RCU_POD_DISTR")
    //calcolo forniture
    val rcu_pod_distr = forniture_ele_3_trasformations.calcolo_RcuPodDistr(dsRcuPodDistrPDao,dsRcuAziendaPDao)
    logger.info(s"Fine calcolo RCU_POD_DISTR")
    //todo necessario?
    logger.info(s"Inizio scrittura mongodbs.RCU_POD_DISTR")
    getRcuPodDistr.write(rcu_pod_distr,true)
    logger.info(s"Fine scrittura mongodbs.RCU_POD_DISTR")

     rcu_pod_distr
  }

}
