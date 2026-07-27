package it.eng.au.portale_consumi_ee.flow.Storico33M

import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.common.schema.mongodbs.fornitureElettricheSchema
import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities
import it.eng.au.portale_consumi_ee.dao.misure.{ConsultazioenDao, MisureStoricDao, MisureStoricF2Dao, MisureStoricF2ErcEriDao, MisureStoricNoraDao, MisureStoricNoraErcEriDao}
import it.eng.au.portale_consumi_ee.dao.mongodbs.FornitureElettricheDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.schema.misure.{ConsultazioneSchema, MisureStoricF2Schema, MisureStoricNoraSchema, MisureStoricSchema, misureStoricNoraErcEriSchema}
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureElettricheTmpSchema
import it.eng.au.portale_consumi_ee.trasformations.{fornitureElettricheTmpTrasfornation, misureStoricoErcEriTrasformation, misureStoricoTrasformation}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class Storico33M(implicit spark: SparkSession)extends FlowUnitOutput{

  //input dao
  val getMisureStoricNoraDao = new MisureStoricNoraDao
  val getFornitureElettricheDao = new FornitureElettricheDao
  val getMisureStoricDao = new MisureStoricDao
  val getMisureStoricErcEri = new MisureStoricF2ErcEriDao  //also output
  val getMisureStoricNoraErcEri = new MisureStoricNoraErcEriDao


  //output dao
  val getMisureStoricF2Dao = new MisureStoricF2Dao
  val getConsultazioneDao = new ConsultazioenDao


  val annomesegiornoWindow33M =  argumentsUtilities.annomese36MonthsAgo()

  override def run() = {

    logger.info(s"Inizio fase storica flusso 36M")

    logger.info(s"Inizio fase storica per dati non orari")

    def getAnnoMeseWindowMisureStoricNoraDao =
      getMisureStoricNoraDao.read().filter(col(MisureStoricNoraSchema.annomese) >= annomesegiornoWindow33M)
        .repartition(col(MisureStoricNoraSchema.annomese))

    def getAnnoMeseWindowMisureStoricNoraErcEriDao =
      getMisureStoricNoraErcEri.read()
        .filter(col(misureStoricNoraErcEriSchema.annomese) >= annomesegiornoWindow33M.toString)

    def getAnnoMeseWindowFornitureElettricheDao = getFornitureElettricheDao.read()

    def fornitureTmp = fornitureElettricheTmpTrasfornation.fornitureElettricheTmp(getAnnoMeseWindowFornitureElettricheDao)
      .persist()

    def misureStoricErcEri = getMisureStoricErcEri.read()

    def storicNora = misureStoricoTrasformation.misureStoricNoraDefinition(getAnnoMeseWindowMisureStoricNoraDao, fornitureTmp)

    //misure_storic_f2 nora
    logger.info(s"Fine calcolo dati non orari per finestra di tempo maggiore o uguale a: ${annomesegiornoWindow33M} ")
    logger.info(s"Inizio scrittura dati non orari per finestra di tempo maggiore o uguale a:  ${annomesegiornoWindow33M} in tabella: ${getMisureStoricF2Dao.tableName}")

    getMisureStoricF2Dao.write(storicNora,true)

    logger.info(s"Fine scrittura dati non orari per finestra di tempo maggiore o uguale a:  ${annomesegiornoWindow33M} in tabella: ${getMisureStoricF2Dao.tableName}")
    logger.info(s"Fine fase storica per dati non orari")

    //misure_storic_f2_erc_eri nora
    logger.info(s"Fine fase storica erc eri per dati non orari")
    logger.info(s"Inizio calcolo dati storico non orari erc eri per finestra di tempo maggiore o uguale a: ${annomesegiornoWindow33M} per tabella ${getMisureStoricErcEri.tableName}")

    def storicNoraErcEri = misureStoricoErcEriTrasformation.misureStoricNoraF2ErcEriPrepared(getAnnoMeseWindowMisureStoricNoraErcEriDao, fornitureTmp)

    logger.info(s"Fine calcolo dati storico non orari erc eri per finestra di tempo maggiore o uguale a:${annomesegiornoWindow33M} per tabella ${getMisureStoricErcEri.tableName} ")
    logger.info(s"Inizio scrittura dati storico non orari erc eri per finestra di tempo maggiore o uguale a:${annomesegiornoWindow33M} in tabella: ${getMisureStoricErcEri.tableName}")

    getMisureStoricErcEri.write(storicNoraErcEri,true)

    logger.info(s"Fine scrittura dati storico non orari erc eri per finestra di tempo maggiore o uguale a:${annomesegiornoWindow33M} in tabella: ${getMisureStoricErcEri.tableName}")
    logger.info(s"Fine fase storica erc eri per dati non orari")

    //misure_storic_f2
    var ymMisureStoricF2Ora = java.time.YearMonth.now()
    logger.info(s"Inizio fase storica per dati orari")

    for (_ <- 0 until 36) {
      val annomeseMisureStoricOra = ymMisureStoricF2Ora.getYear * 100 + ymMisureStoricF2Ora.getMonthValue
      logger.info(s"Inizio fase calcolo storica per dati orari e annomese: ${annomeseMisureStoricOra.toString}")

      def misureStoricAnnoMese = getMisureStoricDao.read().filter(col(MisureStoricSchema.annomese) === annomeseMisureStoricOra)

      val storicOra = misureStoricoTrasformation.misureStoricOraDefinition(misureStoricAnnoMese, fornitureTmp)

      logger.info(s"Fine fase calcolo storica per dati orari e annomese: ${annomeseMisureStoricOra.toString}")
      logger.info(s"Inizio scrittura dati  orari per finestra di tempo uguale a:  ${annomesegiornoWindow33M} in tabella: ${getMisureStoricF2Dao.tableName}")
      getMisureStoricF2Dao.write(storicOra,true)
      logger.info(s"Fine scrittura dati  orari per finestra di tempo uguale a:  ${annomesegiornoWindow33M} in tabella: ${getMisureStoricF2Dao.tableName}")

      ymMisureStoricF2Ora = ymMisureStoricF2Ora.minusMonths(1)
    }

    //37th month to remove
    val annomese37thMisureStoricNora = ymMisureStoricF2Ora.getYear * 100 + ymMisureStoricF2Ora.getMonthValue
    logger.info(s"Inzio eliminazione dati con partizione annomese = ${annomese37thMisureStoricNora} per tabella ${getMisureStoricF2Dao.tableName}")
    //argumentsUtilities.removepartition(annomese37thMisureStoricNora, getMisureStoricF2Dao.tableName, spark, logger)
    argumentsUtilities.dropPartitionsBeforeOrEqualAnnomeseRiferimento( getMisureStoricF2Dao.tableName,MisureStoricF2Schema.annomese_riferimento,annomese37thMisureStoricNora, spark, logger)
    logger.info(s"Fine eliminazione dati con partizione annomese = ${annomese37thMisureStoricNora} per tabella ${getMisureStoricF2Dao.tableName}")
    logger.info(s"Fine fase storica per dati orari")


    //misure_storic_f2_consultazione ora e nora
    var ymConsultazione = java.time.YearMonth.now()

    logger.info(s"Inizio fase storica per tabella consultazione")

    for (_ <- 0 until 36) {
      val annomeseConsultazione = ymConsultazione.getYear * 100 + ymConsultazione.getMonthValue

      logger.info(s"Inizio fase calcolo tabella consultazione  per annomese: ${annomeseConsultazione.toString}")

      def misureStoricF2AnnoMese = getMisureStoricF2Dao.read().filter(col(MisureStoricF2Schema.annomese_riferimento) === annomeseConsultazione)

      def misureStoricF2ErcEriAnnoMese = misureStoricoErcEriTrasformation.misureStoricF2ErcEriPrepared(misureStoricErcEri,annomeseConsultazione)

      val consultazione = misureStoricoErcEriTrasformation.consultazioneDefinition(misureStoricF2AnnoMese, misureStoricF2ErcEriAnnoMese)

      logger.info(s"Fine fase calcolo tabella consultazione  per annomese: ${annomeseConsultazione.toString}")
      logger.info(s"Inizio scrittura dati  consultazione  per finestra di tempo uguale a:${annomesegiornoWindow33M} in tabella: ${getConsultazioneDao.tableName}")
      getConsultazioneDao.write(consultazione,true)
      logger.info(s"Inizio scrittura dati  consultazione  per finestra di tempo uguale a:${annomesegiornoWindow33M} in tabella: ${getConsultazioneDao.tableName}")

      ymConsultazione = ymConsultazione.minusMonths(1)
    }

    fornitureTmp.unpersist()
    //37th month to remove from misure_storic_f2_consultazione
    val annomese37thConsultazione = ymConsultazione.getYear * 100 + ymConsultazione.getMonthValue
    logger.info(s"Inzio eliminazione dati con partizione annomese = ${annomese37thConsultazione} per tabella ${getConsultazioneDao.tableName}")
    //argumentsUtilities.removepartition(annomese37thConsultazione, getConsultazioneDao.tableName, spark, logger)
    argumentsUtilities.dropPartitionsBeforeOrEqualAnnomeseRiferimento( getConsultazioneDao.tableName,ConsultazioneSchema.annomese_riferimento,annomese37thConsultazione, spark, logger)
    logger.info(s"Fine eliminazione dati con partizione annomese = ${annomese37thConsultazione} per tabella ${getConsultazioneDao.tableName}")
    logger.info(s"Fine fase storica per tabella consultazione")

    logger.info(s"Fine fase storica flusso 3M")
  }
}
