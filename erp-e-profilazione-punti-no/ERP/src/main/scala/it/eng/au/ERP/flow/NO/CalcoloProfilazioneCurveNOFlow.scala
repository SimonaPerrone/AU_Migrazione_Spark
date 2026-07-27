package it.eng.au.ERP.flow.NO

import it.eng.au.ERP.dao.hive.erp.{ErpConsumptionNODao, ErpDailyDimaDao, ErpDailyNoDao}
import it.eng.au.ERP.dao.hive.rcu.{rcuPodPDao, rcuPodDistrPDao, RcuAziendaPDao}
import it.eng.au.ERP.dao.hive.rcus.rcusPodDistrDao
import it.eng.au.ERP.flow.Flow
import it.eng.au.ERP.trasformations.NO.ProfilazioneCurveNOTransformation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit, monotonically_increasing_id, pmod}
import it.eng.au.ERP.schema.erp.erpConsumptionNoSchema
import scala.util.Try

case class CalcoloProfilazioneCurveNOFlow()(implicit spark: SparkSession) extends Flow {

  private val erpConsumptionNODao = new ErpConsumptionNODao
  private val erpDailyDimaDao = new ErpDailyDimaDao
  private val erpDailyNoDao = new ErpDailyNoDao
  private val rcuPodPDao = new rcuPodPDao
  private val rcuPodDistrPDao = new rcuPodDistrPDao
  private val rcusPodDistrDao = new rcusPodDistrDao
  private val rcuAziendaPDao = new RcuAziendaPDao

  def run(timestamp: Long, podExcluded: List[String], annomese: Option[String]): Unit = {
    import spark.implicits._

    val executionId = timestamp.toString

    // Abilita AQE solo per questa fase per spezzare eventuali partizioni skewate senza impattare il resto del job
    val prevAdaptive = spark.conf.getOption("spark.sql.adaptive.enabled")
    val prevSkew = spark.conf.getOption("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
    val prevTarget = spark.conf.getOption("spark.sql.adaptive.shuffle.targetPostShuffleInputSize")
    val prevCoalesceEnabled = spark.conf.getOption("spark.sql.adaptive.coalescePartitions.enabled")
    spark.conf.set("spark.sql.adaptive.enabled", true)
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", 512 * 1024 * 1024) // 512 MB
    spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", 64 * 1024 * 1024)         // 64 MB
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)                            // evita collasso in 1 task

    logger.info("Inizio profilazione curve quartorarie NO")

    val consumptionDf = erpConsumptionNODao.readByExecutionIdDf(executionId)
    // Redistribuisci i segmenti per chiave di partizione (anno/mese/executionid) per evitare task hot
    val consumptionDfCoalesced = consumptionDf
      .repartition(
        2000,
        col(erpConsumptionNoSchema.annomese),
        col(erpConsumptionNoSchema.pod),
        col(erpConsumptionNoSchema.executionid)
      )

    val dailyDimaDf = erpDailyDimaDao.read()
    val podAnagDf = rcuPodPDao.read().toDF()
    val podDistrPDf = rcuPodDistrPDao.read().toDF()
    val podDistrHistDf = rcusPodDistrDao.read().toDF()
    val aziendaDf = rcuAziendaPDao.read().toDF()

    val profiledDf = ProfilazioneCurveNOTransformation.buildDailyProfiles(
      consumptionDfCoalesced,
      dailyDimaDf,
      podAnagDf,
      podDistrPDf,
      podDistrHistDf,
      aziendaDf,
      podExcluded,
      timestamp
    )

    logger.info(s"Scrittura profili NO su tabella ${erpDailyNoDao.tableName}")
    val saltedProfiles = profiledDf.withColumn(
      "pod_salt",
      concat(col(erpConsumptionNoSchema.pod), lit("_"), pmod(monotonically_increasing_id(), lit(16)))
    )
    val profiledDfSalted = saltedProfiles.repartition(3000, col("pod_salt")).drop("pod_salt")
    erpDailyNoDao.write(profiledDfSalted, overwrite = false)

    logger.info("Fine profilazione curve quartorarie NO")

    // Ripristina conf AQE precedenti per non toccare altre fasi del job
    prevAdaptive.foreach(v => spark.conf.set("spark.sql.adaptive.enabled", v))
    prevSkew.foreach(v => Try(spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", v)))
    prevTarget.foreach(v => Try(spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", v)))
    prevCoalesceEnabled.foreach(v => Try(spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", v)))
  }
}
