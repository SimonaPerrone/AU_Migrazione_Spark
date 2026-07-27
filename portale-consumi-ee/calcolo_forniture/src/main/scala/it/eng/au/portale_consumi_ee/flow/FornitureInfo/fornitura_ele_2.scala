package it.eng.au.portale_consumi_ee.flow.FornitureInfo

import it.eng.au.portale_consumi_ee.common.flow.{FlowUnitOutput}
import it.eng.au.portale_consumi_ee.dao.mongodbs.{GdmDao}
import it.eng.au.portale_consumi_ee.dao.rcu.{RcuPodMisurePDao, RcuPodPDao, RcuPodTecnPDao}
import it.eng.au.portale_consumi_ee.dao.rcus.RcusPodtecnPDao
import it.eng.au.portale_consumi_ee.model.mongodbs.GdmModel
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuPodMisurePSchema, RcuPodPSchema, RcuPodTecnPSchema}
import it.eng.au.portale_consumi_ee.schema.rcus.RcusPodtecnPSchema
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_2_trasformations}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

case class fornitura_ele_2(implicit spark: SparkSession)  extends FlowUnitOutput{

  //hdao input tables
  val getRcuPodMisurePDao = new RcuPodMisurePDao
  val getRcuPodPDao = new RcuPodPDao
  val getRcuPodTecnPDao = new RcuPodTecnPDao
  val getRcusPodTecnPDao = new RcusPodtecnPDao

  //hdao output tables
  val getGdmDao = new GdmDao

   def runGdm():Dataset[GdmModel] = {


    //dataset input
    def dsRcuPodMisurePDao =  getRcuPodMisurePDao.read().repartition(col(RcuPodMisurePSchema.n_id_pod))
    def dsRcuPodPDao = getRcuPodPDao.read().repartition(col(RcuPodPSchema.n_id_pod))
    def dsRcuPodTecnPDao =  getRcuPodTecnPDao.read().repartition(col(RcuPodTecnPSchema.n_id_pod))
    def dsRcusPodTecnPDao = getRcusPodTecnPDao.read().repartition(col(RcusPodtecnPSchema.n_id_pod))

    logger.info(s"Inizio calcolo gdm")
    //calcolo forniture
    val gdm = forniture_ele_2_trasformations.calcolo_gdm(dsRcuPodMisurePDao,dsRcuPodPDao,dsRcuPodTecnPDao,dsRcusPodTecnPDao)
    logger.info(s"Fine calcolo gdm")
    //todo necessario?
    logger.info(s"Inizio scrittura mongodbs.gdm")
    getGdmDao.write(gdm,true)
    logger.info(s"Fine scrittura mongodbs.gdm")

    gdm
  }

}
