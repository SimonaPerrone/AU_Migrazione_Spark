package it.eng.au.portale_consumi_ee.flow.FornitureInfo

import it.eng.au.portale_consumi_ee.common.flow.{FlowUnitOutput}
import it.eng.au.portale_consumi_ee.dao.mongodbs.{fasceDao}
import it.eng.au.portale_consumi_ee.dao.rcu.{RcuFasceMisuratore2gPDao, RcuMisuratore2gPDao, RcuPodDistrPDao}
import it.eng.au.portale_consumi_ee.model.mongodbs.fasceModel
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuFasceMisuratore2gPSchema, RcuMisuratore2gPSchema}
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_4_trasformations}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

case class fornitura_ele_4(implicit spark: SparkSession) extends FlowUnitOutput{

  //hdao input tables
  val getRcuFasceMisuratore2gPDao = new RcuFasceMisuratore2gPDao
  val getRcuMisuratore2gPDao = new RcuMisuratore2gPDao

  //hdao output tables
  val getfasce = new fasceDao

   def runFasce(): Dataset[fasceModel] = {

    //dataset input
    def dsRcuFasceMisuratore2gPDao =  getRcuFasceMisuratore2gPDao.read().repartition(col(RcuFasceMisuratore2gPSchema.n_id_misuratore))
    def dsRcuMisuratore2gPDao = getRcuMisuratore2gPDao.read().repartition(col(RcuMisuratore2gPSchema.n_id_misuratore_2g))

    logger.info(s"Inizio calcolo fasce")
    //calcolo forniture
    val fasce = forniture_ele_4_trasformations.calcolo_fasce(dsRcuFasceMisuratore2gPDao,dsRcuMisuratore2gPDao)
    logger.info(s"Fine calcolo fasce")
    //todo necessario?
    logger.info(s"Inizio scrittura mongodbs.fasce")
    getfasce.write(fasce,true)
    logger.info(s"Fine scrittura mongodbs.fasce")

    fasce
  }

}
