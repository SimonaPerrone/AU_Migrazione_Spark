package it.au.misure.commit

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils, TypeDataToElab}
import it.au.misure.commons.cli.CommandLine
import it.au.misure.ingestione.InjectionNew.log
import it.au.misure.util.LoggingSupport
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object TypeCommit {
  sealed trait EnumVal
  case object Ingestione extends EnumVal
  case object Aggregati extends EnumVal
  case object Tutto extends EnumVal
  val typeofCommit = Seq(Ingestione, Aggregati, Tutto)
}

object Commiter extends LoggingSupport {


  def main(args: Array[String],typeCommit: TypeCommit.EnumVal) {

    val commandLineOptions = new CommandLineOptions()
    val commonsCliUtils = new CommonsCliUtils()
    val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
    val argsObjMaster = new CommonsCliUtils().getArgs(commandLine)

    val tipo_flusso_inj =  if (commandLine.hasOption(commandLineOptions.pdo.getOpt)){
      TypeDataToElab.Pdo.toString
    }else if (commandLine.hasOption(commandLineOptions.rfo.getOpt)){
      TypeDataToElab.Rfo.toString
    }else if (commandLine.hasOption(commandLineOptions.smis.getOpt)){
      TypeDataToElab.Smis.toString
    }else if (commandLine.hasOption(commandLineOptions.other_f.getOpt)){
      TypeDataToElab.Other_Data.toString
    }else {
      ""
    }

    log.info("***** Inizio processo " + argsObjMaster.appName + " *****")
    println("***** current user " + System.getProperty("user.name") + "****")

    val conf = new SparkConf()
      .setAppName(argsObjMaster.appName)
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .setMaster(argsObjMaster.master)

    val sc = new SparkContext(conf)
    sc.setLogLevel(argsObjMaster.logLevel)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    hiveContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
    hiveContext.setConf("spark.sql.parquet.binaryAsString", "true")
    hiveContext.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


    //nuove
    hiveContext.setConf("spark.sql.parquet.mergeSchema", "false")
    hiveContext.setConf("spark.sql.parquet.filterPushdown", "true")
    hiveContext.setConf("spark.sql.hive.metastorePartitionPruning", "true")


    val strInjection_quarti: String =
      """
      INSERT INTO TABLE au.flusso_misure_quarti PARTITION (annoquarti,mesequarti,pivadistributorequarti,codcontrdispquarti,areaquarti)
      SELECT coducquarti , podquarti , pivautentequarti , tipodato_e ,tipodato_s ,tensione ,trattamento_o ,potcontrimpl ,potdisp , 
      cifreatt ,cifrerea ,raccolta ,validato ,potmax ,perdita ,nomefile ,annomesegiornodir ,dataelaborazione ,time_stamp ,giornoquarti ,
      e1 ,e2 ,e3 ,e4 ,e5 ,e6 ,e7 ,e8 ,e9 ,e10 ,e11 ,e12 ,e13 ,e14 ,e15 ,e16 ,e17 ,e18 ,e19 ,e20 ,e21 ,e22 ,e23 ,e24 ,e25 ,e26 ,e27 ,e28 ,e29, 
      e30 ,e31 ,e32 ,e33 ,e34 ,e35 ,e36 ,e37 ,e38 ,e39 ,e40 ,e41 ,e42 ,e43 ,e44 ,e45 ,e46 ,e47 ,e48 ,e49 ,e50 ,e51 ,e52 ,e53 ,e54 ,e55 , 
      e56 ,e57 ,e58 ,e59 ,e60 ,e61 ,e62 ,e63 ,e64 ,e65 ,e66 ,e67 ,e68 ,e69 ,e70 ,e71 ,e72 ,e73 ,e74 ,e75 ,e76 ,e77 ,e78 ,e79 ,e80 ,e81 ,e82 , 
      e83 ,e84 ,e85 ,e86 ,e87 ,e88 ,e89 ,e90 ,e91 ,e92 ,e93 ,e94 ,e95 ,e96 ,e97 ,e98 ,e99 ,e100 ,er1 ,er2 ,er3 ,er4 ,er5 ,er6 ,er7 ,er8 , 
      er9 ,er10 ,er11 ,er12 ,er13 ,er14 ,er15 ,er16 ,er17 ,er18 ,er19 ,er20 ,er21 ,er22 ,er23 ,er24 ,er25 ,er26 ,er27 ,er28 ,er29 ,er30 ,er31 , 
      er32 ,er33 ,er34 ,er35 ,er36 ,er37 ,er38 ,er39 ,er40 ,er41 ,er42 ,er43 ,er44 ,er45 ,er46 ,er47 ,er48 ,er49 ,er50 ,er51 ,er52 ,er53 ,er54 , 
      er55 ,er56 ,er57 ,er58 ,er59 ,er60 ,er61 ,er62 ,er63 ,er64 ,er65 ,er66 ,er67 ,er68 ,er69 ,er70 ,er71 ,er72 ,er73 ,er74 ,er75 ,er76 ,er77 , 
      er78 ,er79 ,er80 ,er81 ,er82 ,er83 ,er84 ,er85 ,er86 ,er87 ,er88 ,er89 ,er90 ,er91 ,er92 ,er93 ,er94 ,er95 ,er96 ,er97 ,er98 ,er99 ,er100 , 
      annoquarti,mesequarti,pivadistributorequarti,codcontrdispquarti,areaquarti from au_test.flusso_misure_quarti

      """.stripMargin

    val strInjection_estensione_quarti: String =
      """INSERT INTO TABLE au.flusso_misure_estensione_quarti PARTITION (annoquarti ,mesequarti ,pivadistributorequarti , codcontrdispquarti , areaquarti )
       SELECT podquarti , pivautentequarti ,nomefile ,annomesegiornodir ,dataelaborazione ,time_stamp,  
       tipo_flusso ,tipodato_a ,ka ,kr ,kp ,data_misura ,tipo_rettifica , 
       data_rilevazione ,motivazione ,data_prest ,codprat_sii , 
       gruppomis ,forfait ,motivazione_stima ,data_inizio_periodo ,  
       eaf1  ,eaf2  ,eaf3  ,eaf4  ,eaf5  ,eaf6  ,erf1  ,erf2  ,erf3  ,erf4  ,erf5  ,erf6  , 
       potf1  ,potf2  ,potf3  ,potf4  ,potf5  ,potf6 ,EaM  ,ErM  ,PotM, progr_podsez,
       annoquarti ,mesequarti ,pivadistributorequarti , codcontrdispquarti , areaquarti from au_test.flusso_misure_estensione_quarti

      """.stripMargin

    val strInjection_noaggr: String =
      """
      INSERT INTO TABLE au.flusso_misure_noaggr PARTITION (anno ,mese ,pivadistributore , codcontrdisp ,tipo_flusso ,area )
      SELECT isnew_flusso ,coduc , pod , pivautente  ,
      data_misura ,data_inizio ,data_voltura  ,motivazione ,trattamento ,tensione ,perdita ,
      potcontrimpl ,potimp ,potdisp ,cifreatt ,cifrerea ,cifrepot ,
      cod_tariffa  ,serv_tutela  ,prestazioni  ,ka ,kr ,kp ,
      matr_att  ,matr_rea  ,matr_pot  ,data_inst_misatt ,data_inst_misrea ,data_inst_mispot ,
      gruppomis ,forfait ,raccolta ,tipodato_e ,tipodato_s ,tipodato_a ,
      validato ,potmax ,tipo_rettifica ,data_rilevazione ,data_prest ,
      codprat_att ,codprat_sii ,motivazione_stima ,data_inizio_periodo ,
      nomefile ,annomesegiornodir ,dataelaborazione ,time_stamp ,giorno ,
      e1 ,e2 ,e3 ,e4 ,e5 ,e6 ,e7 ,e8 ,e9 ,e10 ,e11 ,e12 ,e13 ,e14 ,e15 ,e16 ,e17 ,e18 ,
      e19 ,e20 ,e21 ,e22 ,e23 ,e24 ,e25 ,e26 ,e27 ,e28 ,e29 ,e30 ,e31 ,e32 ,e33 ,e34 ,e35 ,
      e36 ,e37 ,e38 ,e39 ,e40 ,e41 ,e42 ,e43 ,e44 ,e45 ,e46 ,e47 ,e48 ,e49 ,e50 ,e51 ,e52 ,
      e53 ,e54 ,e55 ,e56 ,e57 ,e58 ,e59 ,e60 ,e61 ,e62 ,e63 ,e64 ,e65 ,e66 ,e67 ,e68 ,e69 ,
      e70 ,e71 ,e72 ,e73 ,e74 ,e75 ,e76 ,e77 ,e78 ,e79 ,e80 ,e81 ,e82 ,e83 ,e84 ,e85 ,e86 ,
      e87 ,e88 ,e89 ,e90 ,e91 ,e92 ,e93 ,e94 ,e95 ,e96 ,e97 ,e98 ,e99 ,e100 ,
      er1 ,er2 ,er3 ,er4 ,er5 ,er6 ,er7 ,er8 ,er9 ,er10 ,er11 ,er12 ,er13 ,er14 ,er15 ,er16 ,er17 ,er18 ,
      er19 ,er20 ,er21 ,er22 ,er23 ,er24 ,er25 ,er26 ,er27 ,er28 ,er29 ,er30 ,er31 ,er32 ,er33 ,er34 ,er35 ,
      er36 ,er37 ,er38 ,er39 ,er40 ,er41 ,er42 ,er43 ,er44 ,er45 ,er46 ,er47 ,er48 ,er49 ,er50 ,er51 ,er52 ,
      er53 ,er54 ,er55 ,er56 ,er57 ,er58 ,er59 ,er60 ,er61 ,er62 ,er63 ,er64 ,er65 ,er66 ,er67 ,er68 ,er69 ,
      er70 ,er71 ,er72 ,er73 ,er74 ,er75 ,er76 ,er77 ,er78 ,er79 ,er80 ,er81 ,er82 ,er83 ,er84 ,er85 ,er86 ,
      er87 ,er88 ,er89 ,er90 ,er91 ,er92 ,er93 ,er94 ,er95 ,er96 ,er97 ,er98 ,er99 ,er100 ,
      eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM,
      anno ,mese ,pivadistributore , codcontrdisp ,tipo_flusso ,area from au_test.flusso_misure_noaggr
       """.stripMargin

    val strInjection_smis: String =
      """
      INSERT INTO TABLE au.flusso_misure_smis PARTITION (anno_dtms,mese_dtms,pivadistributore, codcontrdisp )
      SELECT pod ,pivautente ,nomefile,annomesegiornodir, 
      dataelaborazione, time_stamp, tipo_misuratore_smn, data_misura_smn, tipo_dato_smn, 
      eaf1_smn, eaf2_smn, eaf3_smn, eaf4_smn, eaf5_smn, eaf6_smn, erf1_smn, erf2_smn, 
      erf3_smn, erf4_smn, erf5_smn, erf6_smn, potf1_smn, potf2_smn, potf3_smn, potf4_smn, 
      potf5_smn, potf6_smn, EaM_smn, ErM_smn, PotM_smn, tipo_misuratore_mn, data_misura_mn, 
      data_messa_regime_mn, tensione_mn, perditatens_mn, ka_mn, kr_mn, kp_mn, matr_att_mn, 
      matr_rea_mn, matr_pot_mn, cifre_att_mn, cifre_rea_mn, cifre_pot_mn, eaf1_mn, eaf2_mn, 
      eaf3_mn, eaf4_mn, eaf5_mn, eaf6_mn, erf1_mn, erf2_mn, erf3_mn, erf4_mn, 
      erf5_mn, erf6_mn, potf1_mn, potf2_mn, potf3_mn, potf4_mn, potf5_mn, potf6_mn,EaM_mn, ErM_mn, PotM_mn ,
      anno_dtms,mese_dtms,pivadistributore, codcontrdisp from au_test.flusso_misure_smis

      """.stripMargin

    val strAggregati_orarie: String =
      """
      INSERT INTO TABLE au.aggregazioni_misure_orarie PARTITION (anno, mese, pivadistributore,versione  )
      SELECT pivautente , pod , giorno , area ,validato ,nomefile , codcontrdisp ,
      coduc ,tipodato_e ,tipodato_s ,tensione ,trattamento_o ,potcontrimpl ,potdisp ,cifreatt ,cifrerea ,raccolta ,potmax ,perdita ,annomesegiornodir ,
      h1 ,h2 ,h3 ,h4 ,h5 ,h6 ,h7 ,h8 ,h9 ,h10 ,h11 ,h12 ,h13 ,h14 ,h15 ,h16 ,h17 ,h18 ,h19 ,h20 ,h21 ,h22 ,h23 ,h24 ,h25 ,
      time_stamp ,dataelaborazione ,flaguddpod , stato , trattamento , flagarea , n_id_udd , t_piva ,n_id_distr , n_id_distr_rif , flag_validazione
      anno, mese, pivadistributore,versione from au_test.aggregazioni_misure_orarie
      """.stripMargin

    val strAggregati_terne: String =
      """
      INSERT INTO TABLE au.aggregazioni_misure_am PARTITION (annoaggr ,meseaggr ,pivadistributoreaggr ,versione )
      SELECT  n_id_udd ,n_id_distr , n_id_distr_rif , area , giornoaggr , dataelaborazione ,versione_orarie ,
      h1 ,h2 ,h3 ,h4 ,h5 ,h6 ,h7 ,h8 ,h9 ,h10 ,h11 ,h12 ,h13 ,h14 ,h15 ,h16 ,h17 ,h18 ,h19 ,h20 ,h21 ,h22 ,h23 ,h24 ,h25,
      annoaggr ,meseaggr ,pivadistributoreaggr ,versione from au_test.aggregazioni_misure_am
      """.stripMargin

    val strReport: String =
      """
      INSERT INTO TABLE au.report_ingestione PARTITION (annomese)
      SELECT  codice, nomefile, messaggio, dataelaborazione,annomese
      from au_test.report_ingestione
      """.stripMargin

    try {
      hiveContext.sql("set hive.exec.dynamic.partition=true")
      hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      hiveContext.sql("set hive.mapred.mode = nonstrict")
      hiveContext.sql("set hive.exec.parallel=true")

      if (typeCommit == TypeCommit.Ingestione || typeCommit == TypeCommit.Tutto) {

        if (tipo_flusso_inj == TypeDataToElab.Pdo.toString || tipo_flusso_inj == TypeDataToElab.Rfo.toString || tipo_flusso_inj == "") {
          log.info("*** Commit quarti")
          hiveContext.sql(strInjection_quarti)
          log.info("*** Commit estensione quarti")
          hiveContext.sql(strInjection_estensione_quarti)
        }
        if (tipo_flusso_inj == TypeDataToElab.Other_Data.toString || tipo_flusso_inj == "") {
          log.info("*** Commit flusso da non aggregare")
          hiveContext.sql(strInjection_noaggr)
        }

        if (tipo_flusso_inj == TypeDataToElab.Smis.toString || tipo_flusso_inj == "") {
          log.info("*** Commit flusso SMIS")
          hiveContext.sql(strInjection_smis)
        }

        log.info("*** Commit tabella di report")
        hiveContext.sql(strReport)

        log.info("*** Avvio aggiornamento partizioni su db di produzione")

        if (tipo_flusso_inj == TypeDataToElab.Pdo.toString || tipo_flusso_inj == TypeDataToElab.Rfo.toString || tipo_flusso_inj == "") {
          hiveContext.sql("MSCK REPAIR TABLE au.flusso_misure_quarti")
          hiveContext.sql("MSCK REPAIR TABLE au.flusso_misure_estensione_quarti")
        }

        if (tipo_flusso_inj == TypeDataToElab.Other_Data.toString || tipo_flusso_inj == "") hiveContext.sql("MSCK REPAIR TABLE au.flusso_misure_noaggr")
        if (tipo_flusso_inj == TypeDataToElab.Smis.toString || tipo_flusso_inj == "") hiveContext.sql("MSCK REPAIR TABLE au.flusso_misure_smis")

        hiveContext.sql("MSCK REPAIR TABLE au.report_ingestione")

        log.info("*** Commit dati di ingestione sul db di produzione completata")

      }
      if (typeCommit == TypeCommit.Aggregati || typeCommit == TypeCommit.Tutto) {
        log.info("*** Commit aggregati orari")
        hiveContext.sql(strAggregati_orarie)
        log.info("*** Commit aggregati terne")
        hiveContext.sql(strAggregati_terne)

        hiveContext.sql("MSCK REPAIR TABLE au.aggregazioni_misure_orarie")
        hiveContext.sql("MSCK REPAIR TABLE au.aggregazioni_misure_am")

        log.info("*** Commit dati aggregati sul db di produzione completato")

      }

      log.info("*** Avvio procedura di pulizia sul db di collaudo ***")

      if (typeCommit == TypeCommit.Ingestione || typeCommit == TypeCommit.Tutto) {

        log.info("*** Avvio pulizia tabelle di ingestione sul db di collaudo")

        if (tipo_flusso_inj == TypeDataToElab.Pdo.toString || tipo_flusso_inj == TypeDataToElab.Rfo.toString) {
          hiveContext.sql("alter table au_test.flusso_misure_quarti drop if exists partition (annoquarti >='0')")
          hiveContext.sql("alter table au_test.flusso_misure_estensione_quarti drop if exists partition (annoquarti >'0')")
        }
        if (tipo_flusso_inj == TypeDataToElab.Other_Data.toString) hiveContext.sql("alter table au_test.flusso_misure_noaggr drop if exists partition (anno >'0')")
        if (tipo_flusso_inj == TypeDataToElab.Smis.toString) hiveContext.sql("alter table au_test.flusso_misure_smis drop if exists partition (anno_dtms >'0')")

        hiveContext.sql("alter table au_test.report_ingestione drop if exists partition (annomese >='0')")

        if (tipo_flusso_inj == TypeDataToElab.Pdo.toString || tipo_flusso_inj == TypeDataToElab.Rfo.toString) {
          hiveContext.sql("MSCK REPAIR TABLE au_test.flusso_misure_quarti")
          hiveContext.sql("MSCK REPAIR TABLE au_test.flusso_misure_estensione_quarti")
        }

        if (tipo_flusso_inj == TypeDataToElab.Other_Data.toString) hiveContext.sql("MSCK REPAIR TABLE au_test.flusso_misure_noaggr")
        if (tipo_flusso_inj == TypeDataToElab.Smis.toString) hiveContext.sql("MSCK REPAIR TABLE au_test.flusso_misure_smis")

        hiveContext.sql("MSCK REPAIR TABLE au_test.report_ingestione")

        log.info("*** Pulizia tabelle di ingestione nel db di collaudo completato")
      }
      if (typeCommit == TypeCommit.Aggregati || typeCommit == TypeCommit.Tutto) {
        log.info("*** Avvio pulizia tabelle di aggregazioni nel db di collaudo")

        hiveContext.sql("alter table au_test.aggregazioni_misure_orarie drop if exists partition (anno >='0')")
        hiveContext.sql("alter table au_test.aggregazioni_misure_am drop if exists partition (annoaggr >='0')")

        hiveContext.sql("MSCK REPAIR TABLE au_test.aggregazioni_misure_orarie")
        hiveContext.sql("MSCK REPAIR TABLE au_test.aggregazioni_misure_am")

        log.info("*** Pulizia tabelle di aggregazioni nel db di collaudo completato")
      }


      log.info(s"***** Fine processo ${argsObjMaster.appName} *****")
    } catch{
      case e: Exception =>  e.printStackTrace()
    }
    finally {sc.stop()}
  }
}
