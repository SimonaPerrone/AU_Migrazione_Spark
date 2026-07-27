package it.eng.au.portale_consumi_ee.flow.Storico3M

import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.common.schema.mongodbs.fornitureElettricheSchema
import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities
import it.eng.au.portale_consumi_ee.dao.misure.{ConsultazioenDao, MisureStoricDao, MisureStoricF2Dao, MisureStoricF2ErcEriDao, MisureStoricNoraDao, MisureStoricNoraErcEriDao}
import it.eng.au.portale_consumi_ee.dao.mongodbs.{FornitureElettricheDao, FornitureElettricheTmpDao}
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.schema.misure.{MisureStoricF2Schema, MisureStoricNoraSchema, MisureStoricSchema, misureStoricNoraErcEriSchema}
import it.eng.au.portale_consumi_ee.trasformations.{fornitureElettricheTmpTrasfornation, misureStoricoErcEriTrasformation, misureStoricoTrasformation}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class Storico3M (implicit spark: SparkSession)extends FlowUnitOutput{

  //input dao
  val getMisureStoricNoraDao = new MisureStoricNoraDao
  val getFornitureElettricheDao = new FornitureElettricheDao
  val getMisureStoricDao = new MisureStoricDao
  val getMisureStoricErcEri = new MisureStoricF2ErcEriDao //also output
  val getMisureStoricNoraErcEri = new MisureStoricNoraErcEriDao

  //output dao
  val getMisureStoricF2Dao = new MisureStoricF2Dao
  val getConsultazioneDao = new ConsultazioenDao

  val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
  val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")
  val annomeseWindow3M = argumentsUtilities.annomeseDefiniton(windowTimeValue,timeZone)
  val annomesegiornoWindow3M =  argumentsUtilities.annomesegiornoDefiniton(windowTimeValue,timeZone)

  override def run() = {

    logger.info(s"Inizio fase storica flusso 3M")

    def getAnnoMeseWindowMisureStoricNoraDao =
      getMisureStoricNoraDao.read()
        .filter(col(MisureStoricNoraSchema.annomese) > annomeseWindow3M)
        .repartition(col(MisureStoricNoraSchema.annomese))

    def getAnnoMeseWindowMisureStoricNoraErcEriDao =
      getMisureStoricNoraErcEri.read()
        .filter(col(misureStoricNoraErcEriSchema.annomese) > annomeseWindow3M.toString)

    def getAnnoMeseWindowFornitureElettricheDao = getFornitureElettricheDao.read()

    def fornitureTmp = fornitureElettricheTmpTrasfornation.fornitureElettricheTmp(getAnnoMeseWindowFornitureElettricheDao).persist()

    def misureStoricErcEri = getMisureStoricErcEri.read()

    //misure_storic_f2 nora
    logger.info(s"Inizio fase storica per dati non orari")
    logger.info(s"Inizio calcolo dati non orari per finestra di tempo maggiore o uguale a: ${annomesegiornoWindow3M}")

    def storicNora = misureStoricoTrasformation.misureStoricNoraDefinition(getAnnoMeseWindowMisureStoricNoraDao, fornitureTmp)

    logger.info(s"Fine calcolo dati non orari per finestra di tempo maggiore o uguale a: ${annomesegiornoWindow3M} ")
    logger.info(s"Inizio scrittura dati non orari per finestra di tempo maggiore o uguale a:  ${annomesegiornoWindow3M} in tabella: ${getMisureStoricF2Dao.tableName}")

    getMisureStoricF2Dao.write(storicNora,true)

    logger.info(s"Fine scrittura dati non orari per finestra di tempo maggiore o uguale a:  ${annomesegiornoWindow3M} in tabella: ${getMisureStoricF2Dao.tableName}")
    logger.info(s"Fine fase storica per dati non orari")

    //misure_storic_f2_erc_eri nora
    logger.info(s"Fine fase storica erc eri per dati non orari")
    logger.info(s"Inizio calcolo dati storico non orari erc eri per finestra di tempo maggiore o uguale a: ${annomesegiornoWindow3M} per tabella ${getMisureStoricErcEri.tableName}")

    def storicNoraErcEri = misureStoricoErcEriTrasformation.misureStoricNoraF2ErcEriPrepared(getAnnoMeseWindowMisureStoricNoraErcEriDao, fornitureTmp)

    logger.info(s"Fine calcolo dati storico non orari erc eri per finestra di tempo maggiore o uguale a:${annomesegiornoWindow3M} per tabella ${getMisureStoricErcEri.tableName} ")
    logger.info(s"Inizio scrittura dati storico non orari erc eri per finestra di tempo maggiore o uguale a:${annomesegiornoWindow3M} in tabella: ${getMisureStoricErcEri.tableName}")

    getMisureStoricErcEri.write(storicNoraErcEri,true)

    logger.info(s"Fine scrittura dati storico non orari erc eri per finestra di tempo maggiore o uguale a:${annomesegiornoWindow3M} in tabella: ${getMisureStoricErcEri.tableName}")
    logger.info(s"Fine fase storica erc eri per dati non orari")


    //misure_storic_f2_consultazione ora e nora
    val monthAgoLimit = argumentsUtilities.numberMonthAgoToDefine(windowTimeValue)
    var ym = java.time.YearMonth.now()

    logger.info(s"Inizio fase storica per tabella consultazione")

    for (_ <- 0 until monthAgoLimit) {
      val annomese = ym.getYear * 100 + ym.getMonthValue
      logger.info(s"Inizio fase calcolo tabella consultazione  per annomese: ${annomese.toString}")

      def misureStoricF2AnnoMese = getMisureStoricF2Dao.read()
        .filter(col(MisureStoricF2Schema.annomese_riferimento) === annomese)

      def misureStoricF2ErcEriAnnoMese =
        misureStoricoErcEriTrasformation.misureStoricF2ErcEriPrepared(misureStoricErcEri,annomese)

      val consultazione = misureStoricoErcEriTrasformation.consultazioneDefinition(misureStoricF2AnnoMese, misureStoricF2ErcEriAnnoMese)

      logger.info(s"Fine fase calcolo tabella consultazione  per  annomese: ${annomese.toString}")
      logger.info(s"Inizio scrittura dati  consultazione  per finestra di tempo uguale a:  ${annomesegiornoWindow3M} in tabella: ${getConsultazioneDao.tableName}")
      getConsultazioneDao.write(consultazione,true)
      logger.info(s"Fine scrittura dati  consultazione per finestra di tempo uguale a:  ${annomesegiornoWindow3M} in tabella: ${getConsultazioneDao.tableName}")

      ym = ym.minusMonths(1)
    }

    fornitureTmp.unpersist()
    logger.info(s"Fine fase storica per tabella consultazione")

    logger.info(s"Fine fase storica flusso 3M")

  }
}
