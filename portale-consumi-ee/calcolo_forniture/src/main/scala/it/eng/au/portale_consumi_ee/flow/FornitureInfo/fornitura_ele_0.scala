package it.eng.au.portale_consumi_ee.flow.FornitureInfo

import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.dao.mongodbs.{FornitureDao, FornitureInfoDao}
import it.eng.au.portale_consumi_ee.dao.rcu.{RcuAziendaPDao, RcuFornituraPDao, RcuPodPDao, RcuPodStatoPDao, RcuResidenzaPDao, RcuTariffaPDao}
import it.eng.au.portale_consumi_ee.dao.rcus.{RcusFornituraDao, RcusPodstatoPDao}
import it.eng.au.portale_consumi_ee.model.mongodbs.{FornitureInfoModel, FornitureModel}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuFornituraPSchema, RcuPodPSchema, RcuPodStatoPSchema, RcuResidenzaPSchema, RcuTariffaPSchema}
import it.eng.au.portale_consumi_ee.schema.rcus.{RcusFornituraPSchema, RcusPodstatoPSchema}
import it.eng.au.portale_consumi_ee.trasformation.forniture_ele_0_trasformations
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

case class fornitura_ele_0(implicit spark: SparkSession)  extends FlowUnitOutput{

  //hdao input tables
  val getRcuAziendaPDao = new RcuAziendaPDao
  val getRcuFornituraPDao = new RcuFornituraPDao
  val getRcuPodPDao = new RcuPodPDao
  val getRcuPodStatoPDao = new RcuPodStatoPDao
  val getRcuResidenzaPDao = new RcuResidenzaPDao
  val getRcuTariffaPDao = new RcuTariffaPDao
  val getRcusFornituraDao = new RcusFornituraDao
  val getRcusPodstatoPDao = new RcusPodstatoPDao
  //hdao output tables
  val getFornitureDao = new FornitureDao
  val getFornitureInfoDao = new FornitureInfoDao

   def runForniture():(Dataset[FornitureModel],Dataset[FornitureInfoModel]) = {

    logger.info("prova")
    logger.info("Starting FornitureInfo run")

    //dataset input
    def dsRcuAziendaP =  getRcuAziendaPDao.read() // rows are not a lot
    def dsRcuFornituraP = getRcuFornituraPDao.read().repartition(col(RcuFornituraPSchema.n_id_pod))
    def dsRcuPodP = getRcuPodPDao.read().repartition(col(RcuPodPSchema.n_id_pod))
    def dsRcuPodStatoP = getRcuPodStatoPDao.read().repartition(col(RcuPodStatoPSchema.n_id_pod))
    def dsRcuResidenzaP = getRcuResidenzaPDao.read().repartition(col(RcuResidenzaPSchema.n_id_fornitura))
    def dsRcuTariffaP = getRcuTariffaPDao.read().repartition(col(RcuTariffaPSchema.n_id_fornitura))
    def dsRcusFornitura = getRcusFornituraDao.read().repartition(col(RcusFornituraPSchema.n_id_pod))
    def dsRcusPodstatoP = getRcusPodstatoPDao.read().repartition(col(RcusPodstatoPSchema.n_id_pod))

    logger.info(s"Inizio calcolo forniture")
    //calcolo forniture
    val forniture = forniture_ele_0_trasformations.calcolo_forniture(dsRcuFornituraP,dsRcuPodP,dsRcuPodStatoP,dsRcusFornitura,dsRcusPodstatoP)
     logger.info(s"Fine calcolo forniture")
    logger.info(s"Inizio scrittura mongodbs.forniture")
    getFornitureDao.write(forniture,true)
    logger.info(s"Fine scrittura mongodbs.forniture")

    //calcolo forniture_info
    logger.info(s"Inizio calcolo forniture info")
    val forniture_info = forniture_ele_0_trasformations.calcolo_forniture_info(forniture,dsRcuResidenzaP,dsRcuTariffaP,dsRcuAziendaP)
    logger.info(s"Fine calcolo forniture info")
    logger.info(s"Inizio scrittura mongodbs.forniture_info")
    getFornitureInfoDao.write(forniture_info,true)
    logger.info(s"Fine scrittura mongodbs.forniture_info")

     (forniture,forniture_info)
  }

}
