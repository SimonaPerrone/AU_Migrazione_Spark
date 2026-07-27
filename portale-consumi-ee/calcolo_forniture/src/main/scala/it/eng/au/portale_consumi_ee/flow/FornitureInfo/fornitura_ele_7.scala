package it.eng.au.portale_consumi_ee.flow.FornitureInfo

import it.eng.au.portale_consumi_ee.common.dao.mongodbs.fornitureElettricheDao
import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.dao.mongodbs.{FornitureDao, FornitureInfoDao, GdmDao, MongoDbsRcuPodDistrDao, SwitchDao, fasceDao, podDao}
import it.eng.au.portale_consumi_ee.dao.rcu.{RcuAziendaPDao, RcuClienteFinalePDao, RcuIndirizzoPDao, RcuPodPDao, rcuCodiceOffertaPDao}
import it.eng.au.portale_consumi_ee.dao.tde.tdeVulnPDao
import it.eng.au.portale_consumi_ee.model.mongodbs.{FornitureInfoModel, FornitureModel, GdmModel, RcuPodDistrModel, SwitchModel, fasceModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.{FornitureInfoSchema, FornitureSchema, GdmSchema, RcuPodDistrSchema, SwitchSchema, fasceSchema, rcuCodiceOffertaPSchema}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuClienteFinalePSchema, RcuIndirizzoPSchema}
import it.eng.au.portale_consumi_ee.trasformation.forniture_ele_7_trasformations
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

case class fornitura_ele_7(implicit spark: SparkSession) extends FlowUnitOutput{

  //hdao input tables
  val getFornitureInfoDao = new FornitureInfoDao
  val getSwitchDao = new SwitchDao
  val getGdmDao = new GdmDao
  val getFornitureDao = new FornitureDao
  val getRcuIndirizzoPDao = new RcuIndirizzoPDao
  val getRcuAziendaPDao = new RcuAziendaPDao

  val getRcuClienteFinalePDao = new RcuClienteFinalePDao
  val getMongoDbsRcuPodDistrDao = new MongoDbsRcuPodDistrDao
  val getfasceDao = new fasceDao
  val getTdeVulnPDao = new tdeVulnPDao
  val getRcuPodPDao = new RcuPodPDao
  val getRcuCodiceOffertaPDao = new rcuCodiceOffertaPDao

  //hdao output tables
  val getPodDao = new podDao
  val getfornitureElettricheDao = new  fornitureElettricheDao

  def runForniture(
                  ) = {
    //dataset input
    def dsFornitureInfoDao =  getFornitureInfoDao.read().repartition(col(FornitureInfoSchema.codice_pod))
    def dsSwitchDao = getSwitchDao.read().repartition(col(SwitchSchema.t_codice_pod))
    def dsGdmDao =  getGdmDao.read().repartition(col(GdmSchema.n_id_pod))
    def dsFornitureDao = getFornitureDao.read().repartition(col(FornitureSchema.n_id_pod))
    def dsRcuIndirizzoPDao =  getRcuIndirizzoPDao.read().repartition(col(RcuIndirizzoPSchema.n_id))
    def dsRcuAziendaPDao = getRcuAziendaPDao.read()

    def dsRcuClienteFinalePDao =  getRcuClienteFinalePDao.read().repartition(col(RcuClienteFinalePSchema.n_id_cliente))
    def dsMongoDbsRcuPodDistrDao = getMongoDbsRcuPodDistrDao.read().repartition(col(RcuPodDistrSchema.n_id_pod))
    def dsFasceDao =  getfasceDao.read().repartition(col(fasceSchema.n_id_pod))
    def dsTdeVulnPDao = getTdeVulnPDao.read()
    def dsRcuPodPDao =  getRcuPodPDao.read()
    def dsRcuCodiceOffertaPDao = getRcuCodiceOffertaPDao.read().repartition(col(rcuCodiceOffertaPSchema.n_id_fornitura))

    logger.info(s"Inizio calcolo pod")
    //calcolo forniture
    val dsPod = forniture_ele_7_trasformations.calcolo_pod(dsFornitureInfoDao,dsSwitchDao,dsGdmDao,dsFornitureDao,dsRcuIndirizzoPDao,dsRcuAziendaPDao)
    logger.info(s"Fine calcolo pod")
    logger.info(s"Inizio scrittura mongodbs.pod")
    getPodDao.write(dsPod,true)
    logger.info(s"Fine scrittura mongodbs.pod")

    logger.info(s"Inizio calcolo forniture_elettriche")
    //calcolo forniture
    val forniture_elettriche = forniture_ele_7_trasformations.calcolo_forniture_elettriche(dsRcuClienteFinalePDao,dsPod,dsMongoDbsRcuPodDistrDao,dsFasceDao,dsFornitureDao,dsTdeVulnPDao,dsRcuPodPDao,dsRcuCodiceOffertaPDao)
    logger.info(s"Fine calcolo forniture_elettriche")
    logger.info(s"Inizio scrittura mongodbs.forniture_elettriche")
    getfornitureElettricheDao.write(forniture_elettriche,true)
    logger.info(s"Fine scrittura mongodbs.forniture_elettriche")

  }

}
