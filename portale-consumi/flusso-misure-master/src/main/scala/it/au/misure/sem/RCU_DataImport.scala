package it.au.misure.sem

import java.io.FileNotFoundException
import java.sql.{Connection, DriverManager}
import java.util.Properties

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.cli.TypeDataImportRCU
import it.au.misure.commons.cli.CommandLine
import it.au.misure.util.{CreateProperties, LoggingSupport}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode,Row}
import org.apache.spark.sql.hive.HiveContext
import it.au.misure.util.Schemas._
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.JdbcRDD


class RCU_DataImport()extends LoggingSupport {

  var _dbDest:String = ""
  var _basePath:String = ""

  def init(args: Array[String]): Unit = {


      val propertiesC = new CreateProperties(System.getProperty("user.dir"))
      val prop: Properties = propertiesC.prop

       _dbDest = prop.getProperty("spark.app.dbdest")
       _basePath =prop.getProperty("spark.app.basepath")

      val commandLineOptions = new CommandLineOptions()
      val commonsCliUtils = new CommonsCliUtils()
      val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
      val argsObjMaster = new CommonsCliUtils().getArgsImportRCU(commandLine)


      log.info("***** Inizio processo " + argsObjMaster.appName + " *****")
      val nameApp = argsObjMaster.appName


      log.info("***** current user " + System.getProperty("user.name") + "****")
      log.info(propertiesC.printEnvVar)


      val makeImportFromOracle = argsObjMaster.importFromOracleSem


      val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())

      if (makeImportFromOracle == TypeDataImportRCU.NONE)
        log.info("***** Importazione da oracle : NO" + " *****")
      else if (makeImportFromOracle == TypeDataImportRCU.POD_FILTER)
        log.info("***** Importazione da oracle tabelle/viste : POD_ORARI_2018" + " *****")
      else if (makeImportFromOracle == TypeDataImportRCU.MISURE_1718)
        log.info("***** Importazione da oracle tabelle/viste : IMPORT_ORACLE_CLOUDERA" + " *****")
      else if (makeImportFromOracle == TypeDataImportRCU.AGGR_ORA_SEM)
        log.info("***** Importazione da oracle tabelle/viste : PRT_TMO_AGGREGATI_CALCOLATI,PRT_TMO_AGGREGATI_CALC_SEM" + " *****")
      else if (makeImportFromOracle == TypeDataImportRCU.ALL)
        log.info("***** Importazione da oracle tabelle/viste : POD_ORARI_2018,IMPORT_ORACLE_CLOUDERA,PRT_TMO_AGGREGATI_CALCOLATI,PRT_TMO_AGGREGATI_CALC_SEM" + " *****")

      val conf = new SparkConf()
        .setAppName(nameApp)
        .set("spark.shuffle.service.enabled", "false")
        .set("spark.dynamicAllocation.enabled", "false")
        .set("spark.io.compression.codec", "snappy")
        .set("spark.rdd.compress", "true")
//        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

        .setMaster(argsObjMaster.master)


      val sc = new SparkContext(conf)

    try {

      sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
      sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class","org.apache.spark.sql.parquet.DirectParquetOutputCommitter")

      sc.setLogLevel(argsObjMaster.logLevel)

      val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions")

      val hiveCtx = new HiveContext(sc)
      hiveCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      hiveCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
      hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
      hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
      hiveCtx.setConf("hive.exec.dynamic.partition", "true")
      hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


      hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
      hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
      hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")

      val annomesi: Boolean = commandLine.hasOption(commandLineOptions.annomese_sem.getOpt)

      val annomesiList: Array[String] = if (annomesi) {
        val annomesi: String = commandLine.getOptionValue(commandLineOptions.annomese_sem.getOpt)
        log.info("Applicazione import da oracle ai seguenti mesi(se possibile) : " + annomesi)
        val list_annomesi = annomesi.split(",").filter(x => x.length == 6)
        list_annomesi
      } else Array()

      importDataFromOracle(makeImportFromOracle, hiveCtx, prop, sc,annomesiList)
    }
    catch {
      case ex: FileNotFoundException => ex.printStackTrace()
      case e: Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
  //importazione dati da oracle tabelle : POD_ORARI_2018 +
  // TMO_MO + TMO_TMO_FILE + PRT_TMO_AGGREGATI_CALCOLATI + PRT_TMO_AGGREGATI_CALC_SEM

  def recreateTables(tipoImport:TypeDataImportRCU.Value ,db:String ,userdest:String, hvContx :HiveContext): Unit = {

    log.info("*** Rigenerazione tabelle RCU per l'utente : " + userdest + " db schema : " + db + " ***")

    var query = ""
    if (tipoImport == TypeDataImportRCU.MISURE_1718 || tipoImport == TypeDataImportRCU.POD_FILTER || tipoImport == TypeDataImportRCU.ALL) {

      if (tipoImport == TypeDataImportRCU.POD_FILTER || tipoImport == TypeDataImportRCU.ALL) {
        query = s"DROP TABLE IF EXISTS ${db}.pod_orari_2018"
        hvContx.sql(query)

        query =
          s"""CREATE  TABLE ${db}.pod_orari_2018 ( pod14 CHAR(14))
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      STORED AS PARQUET
      LOCATION '${_basePath}/rcu/pod_orari_18'
      """.stripMargin

        hvContx.sql(query)



      }

    }
    if (tipoImport == TypeDataImportRCU.AGGR_ORA_SEM || tipoImport == TypeDataImportRCU.ALL) {
      {


        query = s"DROP TABLE IF EXISTS ${db}.rcu_tmo_aggregati_calc_sem"

        hvContx.sql(query)
        query =
          s"""
        CREATE TABLE ${db}.rcu_tmo_aggregati_calc_sem (giornoaggr INT, data_aggregazione BIGINT,aggr_sottesi CHAR(1),
        uid_elab INT ,  h1 DOUBLE,h2 DOUBLE,h3 DOUBLE,h4 DOUBLE,h5 DOUBLE,h6 DOUBLE,h7 DOUBLE,h8 DOUBLE,h9 DOUBLE,h10 DOUBLE,h11 DOUBLE,h12 DOUBLE,
        h13 DOUBLE,  h14 DOUBLE,h15 DOUBLE,h16 DOUBLE,h17 DOUBLE,h18 DOUBLE,h19 DOUBLE,h20 DOUBLE,h21 DOUBLE,h22 DOUBLE,h23 DOUBLE,h24 DOUBLE,
        h25 DOUBLE,n_id_distr_rif String,n_id_distr String,n_id_udd String,area String )
        PARTITIONED BY (annoaggr INT, meseaggr INT)
        ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        STORED AS PARQUET
        LOCATION '${_basePath}/rcu_tmo_aggregati_calc_sem'
           """.stripMargin


        hvContx.sql(query)
        /*
   PARTITIONED BY (annoaggr INT, meseaggr INT, n_id_distr String,n_id_udd String,area String)
   */

        query = s"DROP TABLE IF EXISTS ${db}.rcu_tmo_aggregati_calcolati"
        hvContx.sql(query)


        query =
          s"""
      CREATE TABLE ${db}.rcu_tmo_aggregati_calcolati ( giornoaggr INT, data_aggregazione BIGINT,aggr_sottesi CHAR(1),
      uid_elab INT ,  h1 DOUBLE,h2 DOUBLE,h3 DOUBLE,h4 DOUBLE,h5 DOUBLE,h6 DOUBLE,h7 DOUBLE,h8 DOUBLE,h9 DOUBLE,h10 DOUBLE,h11 DOUBLE,h12 DOUBLE,
      h13 DOUBLE,  h14 DOUBLE,h15 DOUBLE,h16 DOUBLE,h17 DOUBLE,h18 DOUBLE,h19 DOUBLE,h20 DOUBLE,h21 DOUBLE,h22 DOUBLE,h23 DOUBLE,h24 DOUBLE,
      h25 DOUBLE, n_id_distr_rif String,n_id_distr String,n_id_udd String,area String  )
      PARTITIONED BY (annoaggr INT, meseaggr INT)
      ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
      STORED AS PARQUET
      LOCATION '${_basePath}/rcu_tmo_aggregati_calcolati'
      """.stripMargin

        /*
      PARTITIONED BY (annoaggr INT, meseaggr INT, n_id_distr String,n_id_udd String,area String)
       */
        hvContx.sql(query)

      }
    }
  }

  def verifyannomesipar(anno:Int,mese:Int,annomesi:Array[String]): Int ={

    val annomese_tmp =anno.toString +(("0" + mese) takeRight 2)
    if(annomesi.length > 0){
      log.info("*** verifica anno mese " + annomese_tmp + " tra i mesi passati da riga comando ***")
      if(!annomesi.contains(annomese_tmp))
      {
        log.info("*** anno e mese " + annomese_tmp + " non trovato e quindi skippato **** ")
        return 0
      }
    }
    return 1
  }

  def importDataFromOracle(tipoImport:TypeDataImportRCU.Value , hvContx :HiveContext,props:Properties,spc:SparkContext,annomesiList: Array[String]): Unit ={

    try {

      //tabelle da oracle

      val url: String = props.getProperty("spark.app.url")
      val user: String = props.getProperty("spark.app.user")
      val password: String = props.getProperty("spark.app.password")
      val driver = props.getProperty("spark.app.jdbc.driver")

      val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())

      Class.forName(driver)

      val repartition = 48 //if((spc.defaultParallelism).toInt<48)48 else (spc.defaultParallelism).toInt

      val correctID = udf { (idval: String) =>
        idval.replace(".0000000000", "")
      }

      val rcu_pod_orari = spc.getConf.get("spark.rcu.misure.pod_orari")

      val dbdest = _dbDest

      hvContx.sql("set hive.exec.dynamic.partition=true")
      hvContx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      hvContx.sql("set hive.mapred.mode = nonstrict")
      hvContx.sql("set hive.exec.parallel=true")


       recreateTables(tipoImport, dbdest, System.getProperty("user.name"), hvContx)


      //-ifo_dt
      if (tipoImport == TypeDataImportRCU.AGGR_ORA_SEM || tipoImport == TypeDataImportRCU.ALL) {

        val rcu_cal_sem = spc.getConf.get("spark.rcu.misure.prt_tmo_aggregati_calc_sem")
        log.info("*** rcu prt_tmo_aggregati_calc_sem : " + rcu_cal_sem)

        val rcu_cal_period = spc.getConf.get("spark.rcu.misure.prt_tmo_aggregati_calcolati")
        log.info("*** rcu prt_tmo_aggregati_calcolati : " + rcu_cal_period)

        log.info("*** Avvio lettura vista SEM_AGGR_CALC_SEM da oracle ***")

        val limits = getUpperLowerBound("SEM_AGGR_CALC_SEM", "ROW_N", url, user, password)
        log.info("Lower B:" + limits._1 + " Upper B:" + limits._2)
        if (limits._2 == "0L") return

        val df_rcu_cal_sem = hvContx.read
          .format("jdbc")
          .option("url", url)
          .option("driver", driver)
          .option("dbtable", "SEM_AGGR_CALC_SEM")
          .option("user", user)
          .option("password", password)
          .option("lowerBound", limits._1)
          .option("upperBound", limits._2)
          .option("numPartitions", repartition.toString)
          .option("column", "ROW_N")
          .load()

        val df_cal_sem = df_rcu_cal_sem.drop(df_rcu_cal_sem.col("ROW_N")).toDF()
        df_cal_sem.persist(StorageLevels.MEMORY_ONLY_SER)
        //converto lo schema del dataframe letto da oracle nello schema definito per hive
        //la definizione delle colonne della vista oracle deve corrispondere con le colonne su hive (e quindi nello schema)
        val schemaSEM = schemaSEM_Terna_RCU.map(x => (x.name, x.dataType))
        var dfR_CalcSem = schemaSEM.foldLeft(df_cal_sem) { case (tempdf, x) => tempdf.withColumn(x._1, col(x._1).cast(x._2)) }

        dfR_CalcSem = dfR_CalcSem.withColumn("n_id_distr", correctID(dfR_CalcSem("n_id_distr"))).withColumn("n_id_distr_rif", correctID(dfR_CalcSem("n_id_distr_rif"))).withColumn("n_id_udd", correctID(dfR_CalcSem("n_id_udd")))

        //dfR_CalcSem = dfR_CalcSem.repartition(repartition)

        df_cal_sem.unpersist()


        log.info("*** Avvio scrittura SEM_AGGR_CALC_SEM su hive nel percorso : " + rcu_cal_sem + " ***")
        dfR_CalcSem
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .partitionBy("annoaggr", "meseaggr")
          .save(rcu_cal_sem);

        //  .partitionBy("annoaggr", "meseaggr", "n_id_distr", "n_id_udd", "area")
        hvContx.sql(s"MSCK REPAIR TABLE ${dbdest}.rcu_tmo_aggregati_calc_sem")

        val cc = dfR_CalcSem.count()
        log.info("***** Scrittura SEM_AGGR_CALC_SEM OK , numero di righe scritte : " + cc.toString)


        //**********************************************************************************


        log.info("*** Avvio lettura vista SEM_AGGR_CALC_PERIOD da oracle ***")


        val limits2 = getUpperLowerBound("SEM_AGGR_CALC_PERIOD", "ROW_N", url, user, password)
        log.info("Lower B:" + limits2._1 + " Upper B:" + limits2._2)
        if (limits2._2 == "0L") return

        val df_cal_period = hvContx.read
          .format("jdbc")
          .option("url", url)
          .option("driver", driver)
          .option("dbtable", "SEM_AGGR_CALC_PERIOD")
          .option("user", user)
          .option("password", password)
          .option("lowerBound", limits2._1)
          .option("upperBound", limits2._2)
          .option("numPartitions", repartition.toString)
          .option("column", "ROW_N")
          .load()

        val df_cal_per = df_cal_period.drop(df_cal_period.col("ROW_N")).toDF()
        df_cal_per.persist(StorageLevels.MEMORY_ONLY_SER)

        //converto lo schema del dataframe letto da oracle nello schema definito per hive
        //la definizione delle colonne della vista oracle deve corrispondere con le colonne su hive (e quindi nello schema)
        val schemaPeriod = schemaSEM_Terna_RCU.map(x => (x.name, x.dataType))
        var dfR_CalcPer = schemaPeriod.foldLeft(df_cal_per) { case (tempdf, x) => tempdf.withColumn(x._1, col(x._1).cast(x._2)) }

        df_cal_per.unpersist()

        dfR_CalcPer = dfR_CalcPer.withColumn("n_id_distr", correctID(dfR_CalcPer("n_id_distr"))).withColumn("n_id_distr_rif", correctID(dfR_CalcPer("n_id_distr_rif"))).withColumn("n_id_udd", correctID(dfR_CalcPer("n_id_udd")))

        log.info("*** Avvio scrittura SEM_AGGR_CALC_PERIOD su hive nel percorso : " + rcu_cal_period + " ***")
        dfR_CalcPer
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .partitionBy("annoaggr", "meseaggr")
          .save(rcu_cal_period);

        //.partitionBy("annoaggr", "meseaggr", "n_id_distr", "n_id_udd", "area")


        hvContx.sql(s"MSCK REPAIR TABLE ${dbdest}.rcu_tmo_aggregati_calcolati")
        val cc2 = dfR_CalcPer.count()
        log.info("***** Scrittura SEM_AGGR_CALC_PERIOD OK , numero di righe scritte : " + cc2.toString)


      }
      if (tipoImport == TypeDataImportRCU.MISURE_1718 || tipoImport==TypeDataImportRCU.POD_FILTER || tipoImport == TypeDataImportRCU.ALL) {
        //-ifo_pod_o
        if(tipoImport==TypeDataImportRCU.POD_FILTER || tipoImport == TypeDataImportRCU.ALL ) {
          log.info("*** rcu pod orari da 2018 in poi : " + rcu_pod_orari)

          log.info("*** Avvio lettura vista POD_ORARI_2018 da oracle ***")

          val df_pod_orari = hvContx.read
            .format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("dbtable", "POD_ORARI_2018")
            .option("user", user)
            .option("password", password)
            .load().repartition(spc.defaultMinPartitions)

          log.info("*** Avvio scrittura POD_ORARI_2018 su hive nel percorso : " + rcu_pod_orari + " ***")
          df_pod_orari
            .write
            .format("parquet")
            .mode(SaveMode.Append)
            .save(rcu_pod_orari);


          hvContx.sql(s"MSCK REPAIR TABLE ${dbdest}.pod_orari_2018")
         // val ccx = df_pod_orari.count()
          //log.info("***** Scrittura POD_ORARI_2018 OK , numero di righe scritte : " + ccx.toString)
          log.info("***** Scrittura POD_ORARI_2018 OK ")
        }
        //-ifo_pod_f
        if(tipoImport==TypeDataImportRCU.MISURE_1718 || tipoImport == TypeDataImportRCU.ALL ) {

          val rcu_misure17_18 = spc.getConf.get("spark.rcu.misure.rcu17_18")
          log.info("*** rcu misure da gennaio 2017 a luglio 2018 : " + rcu_misure17_18)

          log.info("*** Avvio lettura vista IMPORT_ORACLE_CLOUDERA da oracle ***")
          //val limits = getUpperLowerBound("IMPORT_ORACLE_CLOUDERA", "ROW_N", url, user, password)
          //log.info("Lower B:" + limits._1 + " Upper B:" + limits._2)

          //if(limits._2=="0L")return


          var cicla=true
          var annostart =2017
          var mesestart =1
          hvContx.sql("set hive.exec.dynamic.partition=true")
          hvContx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
          hvContx.sql("set hive.mapred.mode = nonstrict")
          hvContx.sql("set hive.exec.parallel=true")


          while(cicla) {

            val rtv = verifyannomesipar(annostart, mesestart, annomesiList)
            if (rtv > 0) {

                  try {
                    val q_drop_part: String = s"ALTER TABLE ${dbdest}.rcu_flusso_misure_quarti17_18 DROP IF EXISTS PARTITION(annoquarti=${annostart},mesequarti=${mesestart})"
                    hvContx.sql(q_drop_part)
                  }
                  catch {
                    case e: Exception =>
                      val msg = e.getMessage
                  }

                  hvContx.sql(s"MSCK REPAIR TABLE ${dbdest}.rcu_flusso_misure_quarti17_18")

                  val mese = (("0" + mesestart) takeRight 2)
                  //val query = s"(SELECT * FROM IMPORT_ORACLE_CLOUDERA WHERE annoquarti = '${annostart}' and mesequarti = '${mese}')"
                  val query = s"SELECT * FROM IMPORT_ORACLE_CLOUDERA WHERE annoquarti = '${annostart}' and mesequarti = '${mese}' and (giornoquarti BETWEEN ? AND ?)"


                  val rdd = new JdbcRDD(spc, () => DriverManager.getConnection(url, user, password),
                    query, 1, 31, 10,
                    r => Row(r.getString("coducquarti"), r.getString("podquarti"), r.getString("pivautentequarti"),
                      r.getInt("tipodato_e"), r.getInt("tipodato_s"), r.getDouble("tensione"),
                      r.getString("trattamento_o"), r.getDouble("potcontrimpl"), r.getDouble("potdisp"),
                      r.getInt("cifreatt"), r.getInt("cifrerea"), r.getString("raccolta"),
                      r.getString("validato"), r.getDouble("potmax"), r.getDouble("perdita"),
                      r.getString("nomefile"), r.getString("tipo_pratica"), r.getString("motivazione"),
                      r.getInt("annomesegiornodir"), dataelaborazione, r.getLong("time_stamp"),
                      r.getInt("giornoquarti"), r.getDouble("e1"), r.getDouble("e2"), r.getDouble("e3"),
                      r.getDouble("e4"), r.getDouble("e5"), r.getDouble("e6"), r.getDouble("e7"),
                      r.getDouble("e8"), r.getDouble("e9"), r.getDouble("e10"), r.getDouble("e11"),
                      r.getDouble("e12"), r.getDouble("e13"), r.getDouble("e14"), r.getDouble("e15"),
                      r.getDouble("e16"), r.getDouble("e17"), r.getDouble("e18"), r.getDouble("e19"),
                      r.getDouble("e20"), r.getDouble("e21"), r.getDouble("e22"), r.getDouble("e23"),
                      r.getDouble("e24"), r.getDouble("e25"), r.getDouble("e26"), r.getDouble("e27"),
                      r.getDouble("e28"), r.getDouble("e29"), r.getDouble("e30"), r.getDouble("e31"),
                      r.getDouble("e32"), r.getDouble("e33"), r.getDouble("e34"), r.getDouble("e35"),
                      r.getDouble("e36"), r.getDouble("e37"), r.getDouble("e38"), r.getDouble("e39"),
                      r.getDouble("e40"), r.getDouble("e41"), r.getDouble("e42"), r.getDouble("e43"),
                      r.getDouble("e44"), r.getDouble("e45"), r.getDouble("e46"), r.getDouble("e47"),
                      r.getDouble("e48"), r.getDouble("e49"), r.getDouble("e50"), r.getDouble("e51"),
                      r.getDouble("e52"), r.getDouble("e53"), r.getDouble("e54"), r.getDouble("e55"),
                      r.getDouble("e56"), r.getDouble("e57"), r.getDouble("e58"), r.getDouble("e59"),
                      r.getDouble("e60"), r.getDouble("e61"), r.getDouble("e62"), r.getDouble("e63"),
                      r.getDouble("e64"), r.getDouble("e65"), r.getDouble("e66"), r.getDouble("e67"),
                      r.getDouble("e68"), r.getDouble("e69"), r.getDouble("e70"), r.getDouble("e71"),
                      r.getDouble("e72"), r.getDouble("e73"), r.getDouble("e74"), r.getDouble("e75"),
                      r.getDouble("e76"), r.getDouble("e77"), r.getDouble("e78"), r.getDouble("e79"),
                      r.getDouble("e80"), r.getDouble("e81"), r.getDouble("e82"), r.getDouble("e83"),
                      r.getDouble("e84"), r.getDouble("e85"), r.getDouble("e86"), r.getDouble("e87"),
                      r.getDouble("e88"), r.getDouble("e89"), r.getDouble("e90"), r.getDouble("e91"),
                      r.getDouble("e92"), r.getDouble("e93"), r.getDouble("e94"), r.getDouble("e95"),
                      r.getDouble("e96"), r.getDouble("e97"), r.getDouble("e98"), r.getDouble("e99"),
                      r.getDouble("e100"), r.getDouble("er1"), r.getDouble("er2"), r.getDouble("er3"),
                      r.getDouble("er4"), r.getDouble("er5"), r.getDouble("er6"), r.getDouble("er7"),
                      r.getDouble("er8"), r.getDouble("er9"), r.getDouble("er10"), r.getDouble("er11"),
                      r.getDouble("er12"), r.getDouble("er13"), r.getDouble("er14"), r.getDouble("er15"),
                      r.getDouble("er16"), r.getDouble("er17"), r.getDouble("er18"), r.getDouble("er19"),
                      r.getDouble("er20"), r.getDouble("er21"), r.getDouble("er22"), r.getDouble("er23"),
                      r.getDouble("er24"), r.getDouble("er25"), r.getDouble("er26"), r.getDouble("er27"),
                      r.getDouble("er28"), r.getDouble("er29"), r.getDouble("er30"), r.getDouble("er31"),
                      r.getDouble("er32"), r.getDouble("er33"), r.getDouble("er34"), r.getDouble("er35"),
                      r.getDouble("er36"), r.getDouble("er37"), r.getDouble("er38"), r.getDouble("er39"),
                      r.getDouble("er40"), r.getDouble("er41"), r.getDouble("er42"), r.getDouble("er43"),
                      r.getDouble("er44"), r.getDouble("er45"), r.getDouble("er46"), r.getDouble("er47"),
                      r.getDouble("er48"), r.getDouble("er49"), r.getDouble("er50"), r.getDouble("er51"),
                      r.getDouble("er52"), r.getDouble("er53"), r.getDouble("er54"), r.getDouble("er55"),
                      r.getDouble("er56"), r.getDouble("er57"), r.getDouble("er58"), r.getDouble("er59"),
                      r.getDouble("er60"), r.getDouble("er61"), r.getDouble("er62"), r.getDouble("er63"),
                      r.getDouble("er64"), r.getDouble("er65"), r.getDouble("er66"), r.getDouble("er67"),
                      r.getDouble("er68"), r.getDouble("er69"), r.getDouble("er70"), r.getDouble("er71"),
                      r.getDouble("er72"), r.getDouble("er73"), r.getDouble("er74"), r.getDouble("er75"),
                      r.getDouble("er76"), r.getDouble("er77"), r.getDouble("er78"), r.getDouble("er79"),
                      r.getDouble("er80"), r.getDouble("er81"), r.getDouble("er82"), r.getDouble("er83"),
                      r.getDouble("er84"), r.getDouble("er85"), r.getDouble("er86"), r.getDouble("er87"),
                      r.getDouble("er88"), r.getDouble("er89"), r.getDouble("er90"), r.getDouble("er91"),
                      r.getDouble("er92"), r.getDouble("er93"), r.getDouble("er94"), r.getDouble("er95"),
                      r.getDouble("er96"), r.getDouble("er97"), r.getDouble("er98"), r.getDouble("er99"),
                      r.getDouble("er100"), r.getInt("annoquarti"), r.getInt("mesequarti"),
                      r.getString("pivadistributorequarti"), r.getString("codcontrdispquarti"), r.getString("areaquarti"))
                  ).setName("IMPORTAZIONE MISURE ANNO " + annostart + " - mese " + mese)



                  //val dfM_1718 = hvContx.createDataFrame(rdd, schemaQuarti_Rcu1718)


                  log.info("*** Avvio scrittura IMPORT_ORACLE_CLOUDERA su hive nel percorso : " + rcu_misure17_18 + " ***")


                  //dfM_1718
                  hvContx.createDataFrame(rdd, schemaQuarti_Rcu1718)
                    .write
                    .format("parquet")
                    .mode(SaveMode.Append)
                    .partitionBy("annoquarti", "mesequarti")
                    .save(rcu_misure17_18);


                  hvContx.sql(s"MSCK REPAIR TABLE ${dbdest}.rcu_flusso_misure_quarti17_18")

                  log.info(s"***** Scrittura IMPORT_ORACLE_CLOUDERA anno : ${annostart} mese : ${mesestart} OK ")

            }

            mesestart = mesestart + 1
            if (mesestart == 13) {
              annostart = annostart + 1
              mesestart = 1
            }

            if (annostart == 2018 && mesestart == 8)
              cicla = false
          }


          log.info("*** Importazione da IMPORT_ORACLE_CLOUDERA terminata ***")
        }

      }
    }catch {
      case e: Exception => {
        e.printStackTrace()
        log.error("ERR: " + e.getMessage, e)
      }
    }

  }

  def getUpperLowerBound(tbl:String,clmn:String,url:String,user:String,psw:String): (String,String) = {


    try {
      val conn1:Connection = DriverManager.getConnection(url, user, psw)
      val query =s"select min(${clmn}) as lowerBound, max(${clmn}) as upperBound from ${tbl} "
      val ps1 = conn1.prepareStatement(query)
      val rs1 = ps1.executeQuery()

      val vals = if (rs1.next()) {
        val lw = rs1.getString("lowerBound")+"L"
        val up = rs1.getString("upperBound")+"L"
        rs1.close()
        ps1.close()
        (lw, up)
      } else {
        rs1.close()
        ps1.close()
        ("0L", "0L")
      }
      //  								}

      vals
    } catch {
      case e: Exception => {
        e.printStackTrace()
        log.error("ERR: " + e.getMessage, e)
        ("0L", "0L")
      }
    }
  }
}
