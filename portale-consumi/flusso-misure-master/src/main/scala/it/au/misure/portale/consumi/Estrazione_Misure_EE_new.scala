package it.au.misure.portale.consumi

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import it.au.misure.util.{CreateProperties, LoggingSupport}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

object Estrazione_Misure_EE_new
  extends LoggingSupport {

  val format = new SimpleDateFormat("yyyy-MM-dd")
  val propertiesC =new CreateProperties(System.getProperty("user.dir"))
  val prop:Properties = propertiesC.prop
  val queryProp:Properties = propertiesC.query

  val _dbDest:String = prop.getProperty("spark.app.dbdest")

  val tbl_misurequarti:String =s"${_dbDest}."+prop.getProperty("spark.app.flussoquarti_table")
  val tbl_misurequartiDaOracle:String=s"${_dbDest}."+prop.getProperty("spark.app.flussoquarti_table.oracle")
  val read1G_no_orarie=prop.getProperty("spark.app.portale.usa_misure_noaggr1G").toBoolean


  val url:String = prop.getProperty("spark.app.url")
  val user:String = prop.getProperty("spark.app.user")
  val password:String = prop.getProperty("spark.app.password")
  val driver = prop.getProperty("spark.app.jdbc.driver")
  Class.forName(driver)

 // val connOracle:Connection = DriverManager.getConnection(url, user, password)
  val queryTrattamento = queryProp.getProperty("spark.query.queryPs2")

  var hiveCtx:HiveContext=null

  var elab_36mesi:Boolean=false
  var elab_4mesi:Boolean=false

  var numMesi:Int= 16

  var curr_elab=""

  var tbl_new=""

  var new_mis_orarie=false
  var new_mis_no_orarie=false

  //spark.app.portale.path.base.misureEE=/user/hive/warehouse/acquirente_unico/misure.db/
  val _path_base_misure={
    val tmp = prop.getProperty("spark.app.portale.path.base.misureEE","/acquirente_unico/misure/")
    if(!tmp.endsWith("/"))
      tmp + "/"
    else
      tmp
  }
  //spark.app.portale.path.base.storic.misure=/user/hive/warehouse/acquirente_unico/misure.db/misure_storiche/
  val _path_base_storic_misure={
    val tmp = prop.getProperty("spark.app.portale.path.base.storic.misure.tmp","/portale_consumi/")
    if(!tmp.endsWith("/"))
      tmp + "/"
    else
      tmp
  }





  /**
    * Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
    * @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
    */
  def main(args: Array[String]) {

    val commandLineOptions = new CommandLineOptions()
    val commonsCliUtils = new CommonsCliUtils()
    val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
    val argsObjMaster = new CommonsCliUtils().getArgsPort_Consumi(commandLine)

    val dataelaborazione : java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis())


    val MesiToElab:Int=if(argsObjMaster.mese=="0")numMesi else argsObjMaster.mese.toInt
    numMesi=MesiToElab

    if(numMesi>=36)
      elab_36mesi=true
    if(numMesi <=5)
      elab_4mesi=true

    val trovaPodOrari:Boolean=if((argsObjMaster.PdoRfo.contains("ST") && argsObjMaster.PdoRfo.contains("-TO")) || argsObjMaster.PdoRfo=="TO")true else false
    val force1GCloudera:Boolean=if(argsObjMaster.PdoRfo.contains("ST") && argsObjMaster.PdoRfo.contains("-F1G"))true else false

    val soloOrari:Boolean=if(argsObjMaster.PdoRfo.contains("ST") && argsObjMaster.PdoRfo.contains("-O"))true else false
    val soloNonOrari:Boolean=if(argsObjMaster.PdoRfo.contains("ST") && argsObjMaster.PdoRfo.contains("-NO"))true else false


    val ricreaStoricEE:Boolean=if(argsObjMaster.PdoRfo.contains("ST") && argsObjMaster.PdoRfo.contains("-RSEE"))true else false
    val ricreaStoricGas:Boolean=if(argsObjMaster.PdoRfo.contains("ST") && argsObjMaster.PdoRfo.contains("-RSGAS"))true else false

    val ricalcola_misure_base:Boolean=if(argsObjMaster.PdoRfo.contains("ST") && argsObjMaster.PdoRfo.contains("-RCMS"))true else false


    val tipoEstrazione = argsObjMaster.PdoRfo.replace("-TO","").replace("-F1G","").replace("-O","").replace("-NO","").replace("-RSEE","").replace("-RSGAS","").replace("-RCMS","").replace("-MXM","")

    var descrTipoEstr=if(tipoEstrazione=="CO") "CALCOLO CONSUMI ORARI" else if(tipoEstrazione=="ON") "CALCOLO ORARIE DA NON ORARIE" else if(tipoEstrazione=="CN")"CALCOLO CONSUMI NON ORARI" else if(tipoEstrazione=="CNO")"CALCOLO CONSUMI NON ORARI E ORARI" else if(tipoEstrazione=="SW")"MISURE ANTE SWITCHING" else if(tipoEstrazione=="A") "CALCOLO AUTOLETTURE/VOLTURE" else if(tipoEstrazione=="TO") "RICERCA POD-ORARI" else if(tipoEstrazione=="ST") " " else ""


    if(descrTipoEstr=="")
    {
      log.info("Bisogna indicare una opzione tra TO/CO[-TO]/ON[-TO]/CN[-TO]/CNO[-TO]/SW[-TO]/A[-TO]/ST[-O|-NO|-|-TO] (TROVA POD ORARI/CONSUMI ORARI/ORARIE DA NON ORARIE/CONSUMI NON ORARI-AUTOLETTURE-VOLTURE/CONSUMI NON ORARI,ORARI E ANTE SWITCHING/MISURE ANTE SWITCHING/AUTOLETTURE-VOLTURE/MISURE ORARIE E/O NON ORARIE inserendo -TO a seguire di O/ON/N/A/ST viene eseguita pure la ricerca dei pod orari")
      return
    }
    val consumi_orari_mese_per_mese=(tipoEstrazione=="CO" && argsObjMaster.PdoRfo.contains("-MXM"))


    descrTipoEstr = if(soloOrari) " - RICALCOLO MISURE ORARIE " else if (soloNonOrari) " - RICALCOLO MISURE NON ORARIE " else if (ricreaStoricEE) " - RICALCOLO STORICO EE" else if (ricreaStoricGas) " - RICALCOLO STORICO GAS" else if (ricalcola_misure_base) " - RICALCOLO MISURE BASI"  else descrTipoEstr
    descrTipoEstr = descrTipoEstr + " ( " + (numMesi -1) + " MESI )"

    //if(tipoEstrazione=="O" || tipoEstrazione=="ON" || tipoEstrazione=="A")
     // log.info("RICORDARSI DI ESEGUIRE PRIMA LE MISURE NON ORARIE E POI LE ORARIE")

    if(force1GCloudera)
      log.info("SPECIFICATO PARAMETRO PER FORZARE L'ELABORAZIONE DEGLI 1G NOAGGR UTILIZZANDO LE TABELLE DI TEST CON DATI CONGELATI")


    val nameApp = argsObjMaster.appName + " " + descrTipoEstr.trim + (if(elab_36mesi) " VS 36 MESI" else if(elab_4mesi) " VS 3 MESI"else "" )

    log.info("***** Inizio processo " + nameApp + " *****")

    log.info("***** current user " + System.getProperty("user.name") + "****")
    log.info(propertiesC.printEnvVar)
    val cur_user = System.getProperty("user.name")

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val current_isilon:String= "hdfs://" + hdfs.getUri().getHost() + ":8020"

   // if(current_isilon=="hdfs://hdfs.fsisilon.siiau.local:8020")
    //  tbl_new="_new"

    log.info(s"***** Lettura misure 1G da tabella flusso_misure_noaggr${tbl_new} : " + read1G_no_orarie.toString)


    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))

    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

    curr_elab = anno + mese + giorno
    log.info(s"current elab : ${curr_elab}")
    log.info(s"Numero mesi da elaborare : ${(numMesi -1)}")


    val annomesiL: Boolean = commandLine.hasOption(commandLineOptions.annomese_sem.getOpt)
    val annomesiList: Array[String] = if (annomesiL) {
      val annomesi: String = commandLine.getOptionValue(commandLineOptions.annomese_sem.getOpt)
      log.info("Applicazione dell'estrazione misure ai seguenti mesi(se possibile) : " + annomesi)
      val list_annomesi = annomesi.split(",").filter(x => x.length == 6)
      list_annomesi
    } else Array()

    val d_max = anno + mese + giorno

    var mese_init = mese.toInt
    var anno_init = anno.toInt


    for (i <- 1 to numMesi) {
      mese_init = if (mese_init - 1 == 0) 12 else (mese_init - 1)
      anno_init = if (mese_init == 12) (anno_init - 1) else anno_init
    }

    val mese_init_str:String=("0" + (mese_init.toString)) takeRight 2


    val d_min = anno_init.toString + mese_init_str + "01"
    val d_min_annomese = anno_init.toString + mese_init_str



    val conf = new SparkConf()
      .setAppName(nameApp)

      .set("spark.shuffle.service.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024")

      .setMaster(argsObjMaster.master)

    val sc: SparkContext = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


    sc.hadoopConfiguration.setInt("parquet.block.size", blocksize)
    sc.hadoopConfiguration.setInt("dfs.blocksize", blocksize)

    sc.setLogLevel("ERROR")

    val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions")


    hiveCtx = new HiveContext(sc)
    hiveCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    //if(ricreaStoric)
      hiveCtx.setConf("spark.sql.parquet.compression.codec", "snappy")
    //else
     // hiveCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")

    hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
    hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    hiveCtx.setConf("hive.exec.dynamic.partition", "true")
    hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
    hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
    hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")
    hiveCtx.setConf("spark.sql.hive.convertMetastoreParquet", "false")


    log.info(s"Path base misure EE : ${_path_base_misure}")
    log.info(s"Path base misure storiche : ${_path_base_storic_misure}")

    log.info(s"Estrazione pod con relative forniture per il periodo : ${d_min} - ${d_max}")


    try {


      var mese_tmp = mese.toInt
      var anno_tmp = anno.toInt


      var d_min_mese_tmp = anno_init.toString + (("0" + mese_init.toString) takeRight 2)
      var d_max_mese_tmp = anno.toString + mese.toString

     /* if(tipoEstrazione!="TO" && tipoEstrazione!="ON")
      {
        log.info(s"Estrazione dati gdm per il periodo ${d_min_mese_tmp} - ${d_max_mese_tmp}")
        val dtgdm = hiveCtx.sql(
          s"""SELECT distinct codice_pod,CAST(SUBSTR(d_inst_misurator_att,1,6) AS INT)annomese FROM mongodbs.gdm
                     WHERE t_tipo_misuratore <> 'G' AND (nvl(d_inst_misurator_att,0)=0 or CAST(SUBSTR(d_inst_misurator_att,1,6) AS INT) <= ${d_max_mese_tmp}) DISTRIBUTE BY codice_pod """)//.persist(StorageLevels.MEMORY_ONLY_SER)
        dtgdm.cache()
        dtgdm.registerTempTable("tbl_gdm")

      }*/


      if(trovaPodOrari){


        var mese_tmp2 = mese.toInt
        var anno_tmp2 = anno.toInt


        hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")

        //L'AGGIORNAMENTO DEI POD CON TRATTAMENTO ORARIO LO RESTRINGO AGLI ULTIMI 6 MESI
       // val numMesiTrovaOrari=numMesi
        val numMesiTrovaOrari=if(numMesi == 16) 7 else numMesi

        for (i <- 1 to numMesiTrovaOrari) {


          val annomese_cur = anno_tmp2.toString + (("0" + mese_tmp2.toString) takeRight 2)



          val rtv = /*if(elab_34mesi){
            //salto i primi 2 mesi
            if(i==1 || i==2) 0
            else verifyannomesipar(anno_tmp2, mese_tmp2, annomesiList, true)
          }
          else */ verifyannomesipar(anno_tmp2, mese_tmp2, annomesiList, true)

          if (rtv > 0) {
            log.info(s"Individuazione pod con trattamento orario per annomese ${annomese_cur}")
            val tblmisure = if ((anno_tmp2 == 2018 && mese_tmp2 <= 7)|| anno_tmp2 < 2018) tbl_misurequartiDaOracle else tbl_misurequarti

            val annomesegiornoelabArr = hiveCtx.sql(s"SELECT nvl(min(annomesegiornoelab),19990101) annomesegiornoelab  FROM misure.last_elab_ee WHERE annomese = ${annomese_cur} and mis_ora = '1'").collect()
            val annomesegiornoelab = if (annomesegiornoelabArr.length == 0) "19990101" else annomesegiornoelabArr(0).get(0).toString
            log.info(s"Tabella quarti : ${tblmisure} - annomesegiornoelab : ${annomesegiornoelab}")

            val qu_orari =
              s"""
                  SELECT DISTINCT quarti.podquarti  FROM
                 (select  substr(podquarti,1,14)podquarti
                 from ${tblmisure} where annoquarti =${anno_tmp2} AND mesequarti = ${mese_tmp2} AND
                 CAST(date_format(dataelaborazione,'yyyyMMdd') as int) >= ${annomesegiornoelab}  DISTRIBUTE BY podquarti ) quarti
                 LEFT OUTER JOIN
                  (
                    SELECT codice_pod t_pod
                    FROM misure.pods_orari
                    where annomese =${annomese_cur} DISTRIBUTE BY t_pod
                   ) pods ON pods.t_pod = quarti.podquarti
                   WHERE pods.t_pod IS NULL
              """.stripMargin

            val pods_to_check = hiveCtx.sql(qu_orari)
            pods_to_check.cache()
            val canElabPO = !(pods_to_check.rdd.isEmpty())

            if ( canElabPO ) {

              pods_to_check.registerTempTable("pods_orari_new")

              TrovaValidazioneStatoPod(hiveCtx, annomese_cur)

              log.info(s"Consolidamento pod con trattamento orario per annomese ${annomese_cur}")


              //hiveCtx.sql(s"ALTER TABLE misure.pods_orari DROP IF EXISTS PARTITION(annomese=${annomese_cur})")


              hiveCtx.sql(
                s"""INSERT INTO misure.pods_orari PARTITION(annomese)
                     SELECT  podquarti codice_pod, nvl(v_pods.IS_T_TRATTAMENTO,'0') is_orario ,'${annomese_cur}' annomese
                     FROM (select podquarti from pods_orari_new DISTRIBUTE BY podquarti) quarti
                     LEFT OUTER JOIN (SELECT * FROM validazione_pod DISTRIBUTE BY T_CODICE_POD) v_pods ON T_CODICE_POD = podquarti

                  """)

              log.info(s"Consolidamento pod con trattamento orario per annomese ${annomese_cur} effettuato")

              hiveCtx.sql("DROP TABLE IF EXISTS validazione_pod")

              hiveCtx.dropTempTable("pods_orari_new")
              //hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")
            }
            pods_to_check.unpersist(true)

          }





          mese_tmp2 = if (mese_tmp2 - 1 == 0) 12 else (mese_tmp2 - 1)
          anno_tmp2 = if (mese_tmp2 == 12) (anno_tmp2 - 1) else anno_tmp2

        }

        hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")
      }
     //,SUBSTR(cf_piva,7,2)cifra_cfpiva,substr(codice_pod,14,1)cifra_pod
      log.info(s"Estrazione forniture per il periodo ${d_min_mese_tmp} - ${d_max_mese_tmp}")
      //hiveCtx.sql("DROP TABLE IF EXISTS tbl_forniture")

      val dt_forniture = hiveCtx.sql(
        s"""
             SELECT cf_piva , n_id_fornitura,codice_pod,inizio, fine
              from
               ( SELECT distinct t_cf cf_piva ,codice_fornitura n_id_fornitura,codice_pod,
                  CAST(data_inizio_fornitura_num AS BIGINT)inizio,CAST(data_fine_fornitura_num AS BIGINT) fine
                  from mongodbs.forniture_elettriche
               )forns DISTRIBUTE  BY codice_pod """)
      dt_forniture.registerTempTable("tbl_forniture")
      hiveCtx.cacheTable("tbl_forniture")

      //if (tipoEstrazione == "O" || tipoEstrazione == "N" || tipoEstrazione=="ST") {
      if (tipoEstrazione=="ST") {


        if(annomesiList.length== 0) {

          val num_elabs=hiveCtx.sql(s"select count(*) from misure.last_elab_ee").collect()(0).get(0).toString
          if(num_elabs =="0") {
            log.info(s"Non sono state trovate elaborazioni nella tabella misure.last_elab_ee eseguo un azzeramento massivo")
            log.info(s"Cancellazioni partizioni misure non orarie , orarie ,storiche   ")

            val q_drop_part3: String = s"ALTER TABLE misure.misure_storic DROP IF EXISTS PARTITION(annomese>=200001)"
            hiveCtx.sql(q_drop_part3)

            val q_drop_part4: String = s"ALTER TABLE misure.misure_storic_nora DROP IF EXISTS PARTITION(annomese>=200001)"
            hiveCtx.sql(q_drop_part4)


          }

          /*if(trovaPodOrari) {
            log.info(s"Cancellazione partizione dati misure orarie e non orarie relativa all'annomese ${d_min_annomese}")
            hiveCtx.sql(s"ALTER TABLE misure.last_elab_ee DROP IF EXISTS PARTITION(annomese<${d_min_annomese})")
            hiveCtx.sql(s"ALTER TABLE misure.pods_orari DROP IF EXISTS PARTITION(annomese<${d_min_annomese})")
            hiveCtx.sql(s"ALTER TABLE misure.misure_storic DROP IF EXISTS PARTITION(annomese<${d_min_annomese})")
            hiveCtx.sql(s"ALTER TABLE misure.misure_storic_nora DROP IF EXISTS PARTITION(annomese<${d_min_annomese})")
            hiveCtx.sql(s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese<${d_min_annomese})")
            hiveCtx.sql(s"ALTER TABLE misure.misure_non_orarie_base DROP IF EXISTS PARTITION(annomese<${d_min_annomese})")
          }*/
        }



        log.info(s"*** Avvio elaborazione misure nel periodo maggiore o uguale all'annomese :${d_min_annomese} ")


        for (i <- 1 to numMesi) {


          val rtv = /*if(elab_34mesi){
            //salto i primi 2 mesi
            if(i==1 || i==2) 0
            else verifyannomesipar(anno_tmp, mese_tmp, annomesiList, true)
          }
          else */verifyannomesipar(anno_tmp, mese_tmp, annomesiList, true)

          if (rtv > 0) {
            if(force1GCloudera)
              calcMisure_storic_test(anno_tmp, mese_tmp)
            else {
              if(!soloOrari && !soloNonOrari && !ricreaStoricEE && !ricreaStoricGas && !ricalcola_misure_base)
              calcMisure_storic(anno_tmp, mese_tmp,true,true)
              else if(soloOrari)
                calcMisure_storic(anno_tmp, mese_tmp,true,false)
              else if(soloNonOrari)
                calcMisure_storic(anno_tmp, mese_tmp,false,true)
              else if(ricalcola_misure_base)
                {
                  val annomese = anno_tmp.toString + (("0" + mese_tmp.toString) takeRight 2)
                  write_misure_nora_base(annomese,false)
                  write_misure_ora_base(annomese)
                }

            }
          }


          d_max_mese_tmp = d_min_mese_tmp

          mese_tmp = if (mese_tmp - 1 == 0) 12 else (mese_tmp - 1)
          anno_tmp = if (mese_tmp == 12) (anno_tmp - 1) else anno_tmp

          d_min_mese_tmp = anno_tmp.toString + (("0" + mese_tmp.toString) takeRight 2)
          cleanMemory()
        }

        if(elab_36mesi) {
          val annomese_x = anno_tmp.toString + (("0" + mese_tmp.toString) takeRight 2)
          //cancello le partizioni che non servono piu
          log.info(s"Cancellazione partizioni vecchie dove annomese <= ${annomese_x}")

          hiveCtx.sql(s"ALTER TABLE misure.last_elab_ee DROP IF EXISTS PARTITION(annomese<=${annomese_x})")
          hiveCtx.sql(s"ALTER TABLE misure.pods_orari DROP IF EXISTS PARTITION(annomese<=${annomese_x})")
          hiveCtx.sql(s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese<=${annomese_x})")
          hiveCtx.sql(s"ALTER TABLE misure.misure_non_orarie_base DROP IF EXISTS PARTITION(annomese<=${annomese_x})")
          hiveCtx.sql(s"ALTER TABLE misure.misure_storic DROP IF EXISTS PARTITION(annomese<=${annomese_x})")
          hiveCtx.sql(s"ALTER TABLE misure.misure_storic_nora DROP IF EXISTS PARTITION(annomese<=${annomese_x})")
        }

        if(ricreaStoricEE)
          write_misure_storiche(annomesiList)
        else if(ricreaStoricGas)
          write_misure_storicheGAS()


        //hiveCtx.dropTempTable(tblpodorari)
        //dt_pod_orari.unpersist(true)

        //hiveCtx.sql("DROP TABLE IF EXISTdfS quarti_ext")


      }


      //if (tipoEstrazione == "O" || tipoEstrazione == "ON" || tipoEstrazione == "ST") {
      if (tipoEstrazione == "ON" || (tipoEstrazione == "ST" && (!soloOrari && !soloNonOrari && !ricreaStoricEE && !ricreaStoricGas ) )) {

        var prev_mese_tmp = mese.toInt
        var prev_anno_tmp = anno.toInt
        mese_tmp = mese.toInt
        anno_tmp = anno.toInt

        hiveCtx.sql("MSCK REPAIR TABLE misure.misure_non_orarie_base")
        hiveCtx.sql("MSCK REPAIR TABLE misure.pods_orari")



        for (i <- 1 to numMesi) {

          prev_mese_tmp = if (prev_mese_tmp - 1 == 0) 12 else (prev_mese_tmp - 1)
          prev_anno_tmp = if (prev_mese_tmp == 12) (prev_anno_tmp - 1) else prev_anno_tmp

          val rtv = /*if(elab_34mesi){
            //salto i primi 2 mesi
            if(i==1 || i==2) 0
            else verifyannomesipar(anno_tmp, mese_tmp, annomesiList, false)
          }
          else */verifyannomesipar(anno_tmp, mese_tmp, annomesiList, false)

          if (rtv > 0) {
            calcMisure_Orarie_DaNonOrarie(anno_tmp, mese_tmp, prev_anno_tmp, prev_mese_tmp)
          }



          d_max_mese_tmp = d_min_mese_tmp


          mese_tmp = if (mese_tmp - 1 == 0) 12 else (mese_tmp - 1)
          anno_tmp = if (mese_tmp == 12) (anno_tmp - 1) else anno_tmp

          d_min_mese_tmp = anno_tmp.toString + (("0" + mese_tmp.toString) takeRight 2)
          cleanMemory()
        }
      }







      if (tipoEstrazione == "CN" || tipoEstrazione == "CNO" )
          calMisure_NoOrarieGG_Mese_Delta(d_max, d_min)
        if (tipoEstrazione == "CO" || tipoEstrazione == "CNO"){
          if(consumi_orari_mese_per_mese)
            {
              mese_tmp = mese.toInt
              anno_tmp = anno.toInt

              for (i <- 1 to numMesi) {

                calMisureOrarieGG_Mese_DeltaAnnoMese(anno_tmp,mese_tmp)

                mese_tmp = if (mese_tmp - 1 == 0) 12 else (mese_tmp - 1)
                anno_tmp = if (mese_tmp == 12) (anno_tmp - 1) else anno_tmp
                cleanMemory()
              }
             // val q_drop_parts: String = s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese<>200000)"
              //hiveCtx.sql(q_drop_parts)

              hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")
            }
          else
          calMisureOrarieGG_Mese_Delta(d_max, d_min)
        }
        if (tipoEstrazione == "CN" || tipoEstrazione == "A" || tipoEstrazione == "CNO" )
          calMisure_Autoletture_Volture(d_max, d_min)
        if(tipoEstrazione == "SW" || tipoEstrazione == "CNO" )
          calcMisureAnteSwitching()



     /* if(tipoEstrazione!="TO" && tipoEstrazione!="ON")
      {
        hiveCtx.dropTempTable("tbl_gdm")

      }*/

      //dt_forniture.unpersist()
      if(hiveCtx.isCached("tbl_forniture"))
       hiveCtx.uncacheTable("tbl_forniture")

      hiveCtx.dropTempTable("tbl_forniture")
      //hiveCtx.sql("DROP TABLE IF EXISTS tbl_forniture")

      log.info(s"***** Fine processo ${nameApp} *****")

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sc.clearCallSite()
      sc.clearJobGroup()
      sc.stop()
      log.info(s"***** Terminazione contesto spark *****")
    }




  }

  def cleanMemory(): Unit ={

    return
    hiveCtx.clearCache()
    for ((k,v) <- hiveCtx.sparkContext.getPersistentRDDs) {
      v.unpersist()
    }

  }

  /*def creazioneVistaStatoPod(annomese:String) : String = {
    val vieName=s"IS_T_TRATT_PC_${annomese}_STATO_POD"

    val queryJdbc = queryProp.getProperty("spark.query.createview").replaceAll("annomese", annomese).replaceAll("IS_T_TRATTAMENTO_STATO_POD",vieName)
    val ps = connOracle.createStatement()
    ps.executeUpdate(queryJdbc)
    vieName
  }

  def validazioneStatoPod(pod:String,view:String) : Boolean = {
    try{

      val query=s"SELECT STATO_POD,IS_T_TRATTAMENTO FROM ${view} WHERE T_CODICE_POD= '${pod}'"
      val ps2 = connOracle.prepareStatement(query)
      //val ps2 = connOracle.prepareStatement(queryTrattamento.replaceAll("IS_T_TRATTAMENTO_STATO_POD",view))
      //ps2.setString(1, pod)

      val rs = ps2.executeQuery()
      if(rs.next() ){
        val statopod:String = rs.getString("STATO_POD")
        val isttrat:String = rs.getString("IS_T_TRATTAMENTO")
        rs.close()
        ps2.close()
        isttrat.toUpperCase.equals("Y")
      }else{
        rs.close()
        ps2.close()
        false
      }
    }catch{
      case e: Exception => {e.printStackTrace()
        log.error("ERR: " + e.getMessage, e)
        false
      }
    }
  }*/

  def verifyannomesipar(anno:Int,mese:Int,annomesi:Array[String],showMsg:Boolean): Int ={

    val annomese_tmp =anno.toString +(("0" + mese) takeRight 2)
    if(annomesi.length > 0){
      if(showMsg) log.info("*** verifica anno mese " + annomese_tmp + " tra i mesi passati da riga comando ***")

      if(!annomesi.contains(annomese_tmp))
      {
        if(showMsg)log.info("*** anno e mese " + annomese_tmp + " non trovato e quindi skippato **** ")

        return 0
      }
    }
    return 1
  }

  def calcMisure_Orarie_DaNonOrarie(anno:Int,mese:Int,prev_anno:Int,prev_mese:Int): Unit = {

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    if(prev_mese!=0 && prev_anno!=0)
    {
      val prev_annomese=prev_anno.toString + (("0" + prev_mese.toString) takeRight 2)

      val q_drop_part:String=s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese=${prev_annomese},da_no_ora=1)"
      hiveCtx.sql(q_drop_part)
      hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")

      log.info(s"Reperimento misure per il periodo minore o uguale ${prev_annomese} da misure.misure_non_orarie_base nel caso di cambio da misure non orarie a orarie nel periodo ${annomese}")

      hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_prevorarie")
      val prevOrarie=
        s"""
         CREATE TABLE misure.tbl_prevorarie STORED AS PARQUET AS
         SELECT DISTINCT podquarti codice_pod
         FROM (select podquarti from misure.misure_orarie_base where annomese =${annomese} and da_no_ora='0' DISTRIBUTE BY podquarti)  ora
         LEFT OUTER JOIN
         (
          SELECT DISTINCT podquarti codice_pod2
          FROM misure.misure_orarie_base ora
          WHERE ora.annomese < ${annomese} and da_no_ora='0' DISTRIBUTE BY codice_pod2
         )A on ora.podquarti = A.codice_pod2
         WHERE  codice_pod2 is null DISTRIBUTE BY codice_pod

       """.stripMargin
      val dtprev_orarie=hiveCtx.sql(prevOrarie)//.persist(StorageLevels.MEMORY_ONLY_SER)
      //dtprev_orarie.registerTempTable("tbl_prevorarie")



      val strInsert=
        s"""
              SELECT DISTINCT NO_BASE.pod podquarti, NO_BASE.giorno giornoquarti,
              CAST(SUBSTR(annomese,1,4) AS INT) annoquarti,CAST(SUBSTR(annomese,5,2) AS INT) mesequarti,
              CAST(CONCAT(annomese,LPAD(NO_BASE.giorno,2,0)) AS BIGINT) annomesegiorno,0.0 consumo,
               NO_BASE.eaf1,NO_BASE.eaf2,NO_BASE.eaf3,NO_BASE.eaf4,NO_BASE.eaf5,NO_BASE.eaf6,
               0.0 perdita,0.0 potmax,
              '0' Is2G,NO_BASE.tipo_flusso, annomese,'1'  da_no_ora
              from
                (
                 select *
                 from
                 (
                      select misure_non_orarie_base.*,concat(annomese,(CASE tipo_flusso WHEN 'PNO_RNO' THEN 1 ELSE 2 END))annomese_tp ,
                      max(concat(annomese,(CASE tipo_flusso WHEN 'PNO_RNO' THEN 1 ELSE 2 END))) over ( partition by pod)max_annomese_tp
                      from misure.misure_non_orarie_base
                      INNER JOIN misure.tbl_prevorarie as pods ON pods.codice_pod = misure_non_orarie_base.pod
                      where annomese < ${annomese}
                  )A where A.annomese_tp = A.max_annomese_tp
                 ) NO_BASE DISTRIBUTE BY podquarti




            """.stripMargin

      hiveCtx.sql(strInsert)
        .write.option("parquet.block.size", blocksize.toString)
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("annomese","da_no_ora")
        .save(s"${_path_base_misure}misure_orarie_base")

      hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")

      hiveCtx.sql(strInsert)

     // dtprev_orarie.unpersist(true)
      //hiveCtx.dropTempTable("tbl_prevorarie")
      hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_prevorarie")

    }

  }

  /*
   PROCEDURA DI TEST FLUSSI NON ORARI PER L'INGESTIONE DEI FLUSSI NOAGGR 1G IN SOSTITUZIONE DEI FLUSSI SQOOP MN E MV
  */
  def calcMisure_storic_test(anno:Int,mese:Int): Unit ={

    val updateStoric:Boolean=false
    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)
    val meseanno_str= (("0" + mese.toString) takeRight 2) + "/" +  anno.toString

    log.info(s"Elaborazione misure con forzatura 1G noaggr cloudera utilizzando le tabelle di test per il periodo : ${annomese}")

    new_mis_orarie=false
    new_mis_no_orarie=false


    //log.info(s"Estrazione pod orari per il periodo : ${annomese}" )
    val tblmisure=if((anno==2018 && mese <=7) || anno < 2018) tbl_misurequartiDaOracle else tbl_misurequarti


    //val q_drop_part:String=s"ALTER TABLE misure.misure_storic DROP IF EXISTS PARTITION(annomese=${annomese})"
    //hiveCtx.sql(q_drop_part)
    //hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_storic")

    hiveCtx.sql("SET hive.auto.convert.join=false")

    val strpod_orari=
      s"""
        SELECT DISTINCT codice_pod, ${annomese} annomese_ora FROM (
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN ( SELECT codice_pod FROM tbl_gdm WHERE annomese <= ${annomese} ) gdm
        ON p_ora.codice_pod=gdm.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        UNION ALL
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN
        ( SELECT SUBSTR(podquarti,1,14)podquarti FROM ${_dbDest}.flusso_misure_estensione_quarti${tbl_new}
          WHERE CONCAT(annoquarti,LPAD(mesequarti,2,0)) = ${annomese}
         ) quarti_ext
        on podquarti=p_ora.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        ) AS TBL
      """.stripMargin


    val dt_pod_orari=hiveCtx.sql(strpod_orari)//.persist(StorageLevels.MEMORY_ONLY_SER)
    dt_pod_orari.cache()
    val tblpodorari="list_pod_orari"
    dt_pod_orari.registerTempTable(tblpodorari)


    //il test deve essere effettuato solo per le misure non orarie
    calcMisure_storic_no_ora(anno, mese, tblpodorari,true)



    hiveCtx.dropTempTable(tblpodorari)
    dt_pod_orari.unpersist(true)

    //hiveCtx.dropTempTable("tbl_misure_storic_tmp")
    hiveCtx.dropTempTable("tbl_misure_storic_nora_tmp")

    //hiveCtx.sql("DROP TABLE IF EXISTS misure.misure_storic_tmp")
    hiveCtx.sql("DROP TABLE IF EXISTS misure.misure_storic_nora_tmp")



  }





  def calcMisure_storic(anno:Int,mese:Int,elabOra:Boolean,elabNoOra:Boolean): Unit ={

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)
    val meseanno_str= (("0" + mese.toString) takeRight 2) + "/" +  anno.toString

    new_mis_orarie=false
    new_mis_no_orarie=false


    //log.info(s"Estrazione pod orari per il periodo : ${annomese}" )
    val tblmisure=if((anno==2018 && mese <=7) || anno < 2018) tbl_misurequartiDaOracle else tbl_misurequarti



    val strpod_orari=
      s"""
        SELECT DISTINCT p_ora.codice_pod , ${annomese} annomese_ora
        from misure.pods_orari p_ora
        WHERE annomese =${annomese}  and is_orario='1' DISTRIBUTE BY codice_pod
      """.stripMargin


    val dt_pod_orari=hiveCtx.sql(strpod_orari)//.persist(StorageLevels.MEMORY_ONLY_SER)
    //dt_pod_orari.cache()
    val tblpodorari="list_pod_orari"
    dt_pod_orari.registerTempTable(tblpodorari)


    val th:Thread = if(elabNoOra) {
      //misure non orarie
      val t=new Thread {

        override def run {
          calcMisure_storic_no_ora(anno, mese, tblpodorari, false)
        }
      }
      t.start()
      t
    }else null

  if(elabOra) {

    val annomesegiornoelabArr = hiveCtx.sql(s"SELECT annomesegiornoelab  FROM misure.last_elab_ee WHERE annomese = ${annomese} and mis_ora = '1'").collect()
    val annomesegiornoelab = if (annomesegiornoelabArr.length == 0) "19990101" else annomesegiornoelabArr(0).get(0).toString


    log.info(s"annomesegiornoelab orari : ${annomesegiornoelab}")


    var str_quarti =
      s"""
         CREATE TABLE misure.tmp_quarti_base STORED AS PARQUET AS
         SELECT CONCAT(e1, ';' , e2 , ';' , e3 , ';' , e4 , ';' , e5 , ';' , e6 , ';' , e7 , ';' , e8 , ';' , e9 , ';' , e10 , ';' , e11 , ';' , e12 , ';' , e13 , ';' , e14 , ';' , e15 , ';' , e16 , ';' , e17 , ';' , e18 , ';' , e19 , ';' , e20 , ';' ,
               e21 , ';' , e22 , ';' , e23 , ';' , e24 , ';' , e25 , ';' , e26 , ';' , e27 , ';' , e28 , ';' , e29 , ';' , e30 , ';' , e31 , ';' , e32 , ';' , e33 , ';' , e34 , ';' , e35 , ';' , e36 , ';' , e37 , ';' , e38 , ';' ,
               e39 , ';' , e40 , ';' , e41 , ';' , e42 , ';' , e43 , ';' , e44 , ';' , e45 , ';' , e46 , ';' , e47 , ';' , e48 , ';' , e49 , ';' , e50 , ';' , e51 , ';' , e52 , ';' , e53 , ';' , e54 , ';' , e55 , ';' , e56 , ';' ,
               e57 , ';' , e58 , ';' , e59 , ';' , e60 , ';' , e61 , ';' , e62 , ';' , e63 , ';' , e64 , ';' , e65 , ';' , e66 , ';' , e67 , ';' , e68 , ';' , e69 , ';' , e70 , ';' , e71 , ';' , e72 , ';' , e73 , ';' , e74 , ';' ,
               e75 , ';' , e76 , ';' , e77 , ';' , e78 , ';' , e79 , ';' , e80 , ';' , e81 , ';' , e82 , ';' , e83 , ';' , e84 , ';' , e85 , ';' , e86 , ';' , e87 , ';' , e88 , ';' , e89 , ';' , e90 , ';' , e91 , ';' , e92 , ';' ,
               e93 , ';' , e94 , ';' , e95 , ';' , e96 )EA,
               CONCAT(er1, ';' , er2 , ';' , er3 , ';' , er4 , ';' , er5 , ';' , er6 , ';' , er7 , ';' , er8 , ';' , er9 , ';' , er10 , ';' , er11 , ';' , er12 , ';' , er13 , ';' , er14 , ';' , er15 , ';' , er16 , ';' , er17 , ';' , er18 , ';' , er19 , ';' , er20 , ';' ,
               er21 , ';' , er22 , ';' , er23 , ';' , er24 , ';' , er25 , ';' , er26 , ';' , er27 , ';' , er28 , ';' , er29 , ';' , er30 , ';' , er31 , ';' , er32 , ';' , er33 , ';' , er34 , ';' , er35 , ';' , er36 , ';' , er37 , ';' , er38 , ';' ,
               er39 , ';' , er40 , ';' , er41 , ';' , er42 , ';' , er43 , ';' , er44 , ';' , er45 , ';' , er46 , ';' , er47 , ';' , er48 , ';' , er49 , ';' , er50 , ';' , er51 , ';' , er52 , ';' , er53 , ';' , er54 , ';' , er55 , ';' , er56 , ';' ,
               er57 , ';' , er58 , ';' , er59 , ';' , er60 , ';' , er61 , ';' , er62 , ';' , er63 , ';' , er64 , ';' , er65 , ';' , er66 , ';' , er67 , ';' , er68 , ';' , er69 , ';' , er70 , ';' , er71 , ';' , er72 , ';' , er73 , ';' , er74 , ';' ,
               er75 , ';' , er76 , ';' , er77 , ';' , er78 , ';' , er79 , ';' , er80 , ';' , er81 , ';' , er82 , ';' , er83 , ';' , er84 , ';' , er85 , ';' , er86 , ';' , er87 , ';' , er88 , ';' , er89 , ';' , er90 , ';' , er91 , ';' , er92 , ';' ,
               er93 , ';' , er94 , ';' , er95 , ';' , er96 )ER,
               (e1 + e2 + e3 + e4 + e5 + e6 + e7 + e8 + e9 + e10 + e11 + e12 + e13 + e14 + e15 + e16 + e17 + e18 + e19 + e20 +
               e21 + e22 + e23 + e24 + e25 + e26 + e27 + e28 + e29 + e30 + e31 + e32 + e33 + e34 + e35 + e36 + e37 + e38 +
               e39 + e40 + e41 + e42 + e43 + e44 + e45 + e46 + e47 + e48 + e49 + e50 + e51 + e52 + e53 + e54 + e55 + e56 +
               e57 + e58 + e59 + e60 + e61 + e62 + e63 + e64 + e65 + e66 + e67 + e68 + e69 + e70 + e71 + e72 + e73 + e74 +
               e75 + e76 + e77 + e78 + e79 + e80 + e81 + e82 + e83 + e84 + e85 + e86 + e87 + e88 + e89 + e90 + e91 + e92 +
               e93 + e94 + e95 + e96 + e97 + e98 + e99 + e100)consumo , substr(podquarti,1,14) podquarti14,
               CAST(concat('${annomese}',LPAD(giornoquarti,2,0)) AS BIGINT) data_lettura,
               concat(LPAD(giornoquarti,2,0),'/${meseanno_str}')data_lettura_str,
               CAST(annomesegiornodir AS BIGINT) data_ricezione,
               concat( SUBSTR(annomesegiornodir,7,2),'/',SUBSTR(annomesegiornodir,5,2),'/',SUBSTR(annomesegiornodir,1,4)) data_ricezione_str,
               case (split(nomefile,"_")[3]) when 'PDO' THEN 1 when 'PDO2G' then 2 when 'RFO' then 3 when 'RFO2G' then 4 else 0 END tipo_flusso_num,(split(nomefile,"_")[3])tipo_flusso,
               CONCAT(time_stamp,annomesegiornodir,unix_timestamp(dataelaborazione)) times_elab,
               max(CONCAT(time_stamp,annomesegiornodir,unix_timestamp(dataelaborazione))) over ( partition by CONCAT('${annomese}',LPAD(giornoquarti,2,0),podquarti,split(nomefile,"_")[3]))max_times_elab,
               CONCAT('${annomese}',cifrerea,dataelaborazione,substr(podquarti,1,14),nomefile) KEY_QUARTI,
               perdita,potmax
               FROM ${tblmisure}
               WHERE  annoquarti = ${anno} AND mesequarti =${mese} AND VALIDATO ='S'
               AND ((tipodato_e = 1 and nomefile like '%PDO%') OR  nomefile like '%RFO%') and
               CAST(date_format(dataelaborazione,'yyyyMMdd') as int) >= ${annomesegiornoelab}


      """.stripMargin
    //from ${tblmisure} A WHERE  A.annoquarti = ${anno} AND A.mesequarti =${mese} AND VALIDATO ='S' AND  tipodato_e = 1


    log.info(s"Estrazione misure orarie per il periodo : ${annomese}")

    var tbl_base = "tbl_misure_quarti_1"
    hiveCtx.sql(s"DROP TABLE IF EXISTS misure.tmp_quarti_base");
    hiveCtx.sql(str_quarti)

    str_quarti="select * from misure.tmp_quarti_base"
    //CREATE TABLE ${tbl_base} STORED AS PARQUET AS


    val strtmp_table =
      s"""

        SELECT A2.podquarti14 pod,A2.annomese,
        A2.data_lettura ,A2.data_lettura_str,
        A2.data_ricezione ,A2.data_ricezione_str,
        A2.tipo_flusso_num,A2.descr_tipoflusso,A2.tipo_flusso,'PDO_RFO' cod_flusso,
        CONCAT(A2.podquarti14,A2.data_lettura,A2.data_ricezione,A2.tipo_flusso_num) as KK_CHECK,
        MAX(A2.perdita)perdita,MAX(A2.potmax)potmax,MAX(A2.EA)ea,MAX(A2.ER)er,MAX(A2.consumo)consumo,
        MAX(A2.KEY_QUARTI)KEY_QUARTI
        FROM
        ( 
        SELECT EA,ER,consumo,perdita,potmax,podquarti14,${annomese} annomese,data_lettura,data_lettura_str,
        data_ricezione,data_ricezione_str,tipo_flusso_num,tipo_flusso,KEY_QUARTI,
        case
        WHEN (tipo_flusso_num = 1 or tipo_flusso_num = 2) THEN  'Lettura Periodica'
        WHEN (tipo_flusso_num = 3 or tipo_flusso_num = 4) THEN  'Lettura di Rettifica'
        ELSE '' END descr_tipoflusso
        FROM (${str_quarti} DISTRIBUTE BY podquarti14) A
        INNER JOIN ${tblpodorari} as pods ON pods.codice_pod = A.podquarti14
        WHERE times_elab = max_times_elab
        ) A2
        GROUP BY A2.data_lettura,A2.data_ricezione,A2.annomese,A2.tipo_flusso_num,A2.tipo_flusso,
        A2.podquarti14,A2.data_lettura_str,A2.data_ricezione_str,A2.descr_tipoflusso
        DISTRIBUTE BY KEY_QUARTI
      """.stripMargin

    log.info(s"""Estrazione quarti da ${tblmisure} dove A.annoquarti = ${anno} AND A.mesequarti =${mese} AND VALIDATO ='S' AND  tipodato_e = 1 """)


    //hiveCtx.sql(strtmp_table)
    val dtmisure_o_quarti_1 = hiveCtx.sql(strtmp_table) //.persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_o_quarti_1.registerTempTable(tbl_base)




    //CAST(CONCAT(YEAR(dataelaborazione),LPAD(month(dataelaborazione),2,0),LPAD(day(dataelaborazione),2,0)) AS INT) >= ${annomesegiornoelab}
    val str_misure_o_ext_quarti =
      s"""
        SELECT eam,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         CONCAT(annomese,progr_podsez,dataelaborazione,substr(podquarti,1,14),nomefile)KEY_QUARTI,
         motivazione,
         case motivazione
         when '1' then 'misura che sostituisce una stima precedente'
         when '2' then 'misura che sostituisce una misura fornita precedentemente errata'
         when '3' then 'misura fornita precedentemente per errore'
         when '4' then 'ricostruzione per frode'
         when '5' then 'ricostruzione per malfunzionamento misuratore'
         else motivazione end descr_motivazione
        FROM
         (
              SELECT eam,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
              podquarti,nomefile,dataelaborazione ,MAX(progr_podsez)progr_podsez ,
              NVL(motivazione,'')motivazione,CAST(${annomese} AS INT) annomese
              FROM ${_dbDest}.flusso_misure_estensione_quarti${tbl_new}
              where annoquarti =${anno} AND mesequarti =${mese} and
              CAST(date_format(dataelaborazione,'yyyyMMdd') as int) >= ${annomesegiornoelab}
              GROUP BY dataelaborazione,eam,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
              podquarti,nomefile,tipo_flusso,motivazione

         )tbl_extquarti DISTRIBUTE BY KEY_QUARTI
      """.stripMargin


    log.info(s"""Estrazione estensione_quarti B.annoquarti =${anno} AND B.mesequarti =${mese} da tabella ${_dbDest}.flusso_misure_estensione_quarti${tbl_new}""")


    val tblquartiext = "tbl_misure_ext_quarti"
    val dtmisure_o_ext_quarti = hiveCtx.sql(str_misure_o_ext_quarti) //.persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_o_ext_quarti.registerTempTable(tblquartiext)
    //dtmisure_o_ext_quarti.cache()

    log.info(s""" Elaborazione misure orarie storiche per il periodo ${annomese}""")


    hiveCtx.sql("DROP TABLE IF EXISTS misure.misure_storic_tmp");

    //LA TABELLA misure.misure_storic_tmp CONTERRA SOLAMENTEW LE MISURE ORARIE INGERITE CON DATA ELABORAZIONE UGUALE O SUCCESSIVA ALL'ULTIMA ELABORAZIONE PER L'ANNO E MESE
    //CREATE TABLE misure.misure_storic_tmp STORED AS PARQUET AS
    //SELECT DISTINCT

    val str_misure_orarie_storic =
    s"""
         CREATE TABLE misure.misure_storic_tmp STORED AS PARQUET AS
         SELECT DISTINCT QUARTI.pod ,QUARTI.data_lettura ,QUARTI.data_ricezione,
         nvl(EXT_QUARTI.motivazione,'')motivazione ,EXT_QUARTI.eam,
         EXT_QUARTI.eaf1 , EXT_QUARTI.eaf2  , EXT_QUARTI.eaf3  ,
         EXT_QUARTI.eaf4 , EXT_QUARTI.eaf5  , EXT_QUARTI.eaf6  ,
         QUARTI.ea,QUARTI.er,
         QUARTI.consumo,QUARTI.perdita,QUARTI.potmax,
         case when nvl(EXT_QUARTI.KEY_QUARTI,'')='' then '0' else '1' END is2g,
         QUARTI.tipo_flusso_num,QUARTI.cod_flusso,QUARTI.tipo_flusso,QUARTI.descr_tipoflusso,
         nvl(EXT_QUARTI.descr_motivazione,'')descr_motivazione ,
         QUARTI.data_lettura_str,QUARTI.data_ricezione_str,QUARTI.annomese,
         QUARTI.KK_CHECK
         FROM ${tbl_base} QUARTI
         LEFT OUTER JOIN ${tblquartiext} EXT_QUARTI ON QUARTI.KEY_QUARTI = EXT_QUARTI.KEY_QUARTI
      """.stripMargin

    val dt_misure_storic = hiveCtx.sql(str_misure_orarie_storic)

    //dt_misure_storic.registerTempTable("misure_storic_tmp")

    hiveCtx.sql(s"DROP TABLE IF EXISTS misure.tmp_quarti_base");
    log.info("Join quarti + estensione quarti")


    val query_misure_orarie_storic =if(annomesegiornoelab!="19990101") {

      log.info(s"Estrazione misure orarie nuove non ancora inserite nell'eleborazione per l'annomese : ${annomese}")


      //VENGONO PRESE SOLAMENTE LE MISURE NUOVE CHE NON SIANO STATE GIA INGERITE

      s""" SELECT pod ,data_lettura ,data_ricezione ,motivazione ,eam ,eaf1 ,eaf2 ,eaf3 ,eaf4 ,eaf5 ,eaf6 ,ea ,er ,consumo ,perdita ,potmax ,
                   is2g ,tipo_flusso_num ,cod_flusso ,tipo_flusso ,descr_tipoflusso ,descr_motivazione ,data_lettura_str ,data_ricezione_str ,
                   annomese  from (SELECT * FROM misure.misure_storic_tmp DISTRIBUTE BY KK_CHECK )A
                   LEFT OUTER JOIN (select CONCAT(pod,data_lettura,data_ricezione,tipo_flusso_num) KK_CHECK from misure.misure_storic
                   where annomese =${annomese}  DISTRIBUTE BY KK_CHECK) B
                   ON A.KK_CHECK = B.KK_CHECK WHERE B.KK_CHECK IS NULL
               """


    }else {

      log.info(s"Estrazione misure orarie per l'intero annomese : ${annomese}")

      s""" SELECT  pod ,data_lettura ,data_ricezione ,motivazione ,eam ,eaf1 ,eaf2 ,eaf3 ,eaf4 ,eaf5 ,eaf6 ,ea ,er ,consumo ,perdita ,potmax ,
                   is2g ,tipo_flusso_num ,cod_flusso ,tipo_flusso ,descr_tipoflusso ,descr_motivazione ,data_lettura_str ,data_ricezione_str ,
                   annomese from  misure.misure_storic_tmp
               """
    }

    val dtOra_Diff = hiveCtx.sql(query_misure_orarie_storic)
    dtOra_Diff.registerTempTable("tbl_misure_storic_tmp")


    //val cc2 =hiveCtx.sql("SELECT COUNT(*) FROM tbl_misure_storic_tmp").collect()(0).getAs[Long](0)
    //log.info("NUMERO DI MISURE ORARIE NUOVE IN TBL_MISURE_STORIC_TMP : " + cc2.toString )


    //if(cc2>0) {

    val numPartPiene: org.apache.spark.Accumulator[Int] = hiveCtx.sparkContext.accumulator(0, "Partitioni_piene")
    dtOra_Diff.foreachPartition(f=> if(f.length > 0)numPartPiene.add(1))
    //numPartPiene.add(1)

    if (numPartPiene.value>0) {

      log.info(s"NumPartPiene : ${numPartPiene.value}")


          val threadSt =new Thread {

            override def run {
              if(elab_4mesi) {
                val strStoricOrari =
                  """

                SELECT  F.cf_piva,MIS.pod ,MIS.data_lettura_str data_lettura ,MIS.data_ricezione_str data_ricezione ,
                MIS.descr_motivazione motivazione ,CAST(null as DOUBLE) lettura_monoraria,
                MIS.eaf1  lettura_f1, MIS.eaf2 lettura_f2 , MIS.eaf3 lettura_f3 , MIS.eaf4 lettura_f4 , MIS.eaf5 lettura_f5 ,
                MIS.eaf6 lettura_f6, MIS.EA , MIS.ER  ,MIS.descr_tipoflusso tipo_flusso ,
                MIS.annomese annomese_riferimento ,MIS.data_lettura data_lettura_num,
                MIS.cod_pod,'1' is_mis_oraria FROM  (SELECT *,SUBSTR(pod,7,2) cod_pod FROM tbl_misure_storic_tmp) MIS
                INNER JOIN tbl_forniture F ON MIS.pod = F.codice_pod
                WHERE  MIS.data_lettura >= F.inizio  AND MIS.data_lettura <= F.fine
                DISTRIBUTE BY MIS.cod_pod,is_mis_oraria
            """.stripMargin

                log.info(s"SCRITTURA MISURE ORARIE STORICHE su tabella  misure.misure_storic_f2 per anno =${anno} e mese=${mese}")

                val dtOrari = hiveCtx.sql(strStoricOrari)

                val path_misure_storiche_hdfs=_path_base_storic_misure.replaceAll("misure_storiche/","misure_storic_f2")

                dtOrari
                  .write.option("parquet.block.size", blocksize.toString)
                  .format("parquet")
                  .partitionBy("cod_pod", "is_mis_oraria")
                  .mode(SaveMode.Append)
                  .save(s"${path_misure_storiche_hdfs}")

                hiveCtx.sql("MSCK REPAIR TABLE misure.misure_storic_f2")

                log.info(s"SCRITTURA MISURE ORARIE STORICHE su tabella  misure.misure_storic_f2 per anno =${anno} e mese=${mese} completata")
              }
            }
          }

          threadSt.start()

      log.info(s"SCRITTURA MISURE ORARIE su tabella  misure.misure_storic per anno =${anno} e mese=${mese}")

      new_mis_orarie = true
      dtOra_Diff
        .write.option("parquet.block.size", blocksize.toString)
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("annomese")
        .save(s"${_path_base_misure}misure_storic")


      if(threadSt.isAlive)
      threadSt.join()

      hiveCtx.sql("MSCK REPAIR TABLE misure.misure_storic")
      hiveCtx.refreshTable(s"misure.misure_storic")

      hiveCtx.sql(s"ALTER TABLE misure.last_elab_ee DROP IF EXISTS PARTITION(annomese=${annomese},mis_ora='1')")
      hiveCtx.sql(s"insert into misure.last_elab_ee PARTITION(annomese,mis_ora) select ${curr_elab} annomesegiornoelab , ${annomese} annomese , '1' mis_ora")

    }

    //dtmisure_o_quarti_1.unpersist()
    //dtmisure_o_quarti.unpersist(true)


    hiveCtx.dropTempTable(tbl_base)
    //hiveCtx.dropTempTable(tbl_misure_quarti)
    hiveCtx.dropTempTable(tblquartiext)


    hiveCtx.dropTempTable("tbl_misure_storic_tmp")

    hiveCtx.sql(s"DROP TABLE IF EXISTS ${tbl_base}");
    hiveCtx.sql("DROP TABLE IF EXISTS misure.misure_storic_tmp");
   // hiveCtx.dropTempTable("misure_storic_tmp")
    dt_misure_storic.unpersist()
    if (numPartPiene.value>0)
      write_misure_ora_base(annomese)
    else
      log.info(s"ELaborazione misure orarie per l'anno e mese : ${annomese} completata!")
  }


    if(elabNoOra) {
      if (th.isAlive)
        th.join()


      hiveCtx.dropTempTable("tbl_misure_storic_nora_tmp")
      hiveCtx.sql("DROP TABLE IF EXISTS misure.misure_storic_nora_tmp")
    }


    hiveCtx.dropTempTable(tblpodorari)
    dt_pod_orari.unpersist(true)


  }

  def write_misure_nora_base(annomese:String,must_elab_1G:Boolean): Unit ={

    // if (new_mis_no_orarie) {
    log.info("Avvio estrazione e scrittura misure su tabella misure.misure_non_orarie_base")

    val suffix_must_elab_1G: String = if (must_elab_1G) "_test" else ""

    val q_drop_partsx: String = s"ALTER TABLE misure.misure_non_orarie_base DROP IF EXISTS PARTITION(annomese=${annomese})"
    hiveCtx.sql(q_drop_partsx)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_non_orarie_base")

    //la query seguente predilige prima l'ultima misura pervenuta nel mese per pod con distinzione tra pno e vno
    // e a parita di annomesegiorno ,pod le rettifiche
    /*val str_misure_nora_base =
    s"""
        SELECT pod,giorno,lettura_monoraria,
        eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6,'E' tipo_dato,
        cod_flusso tipo_flusso,tipo_flusso tipo_flusso2,annomese
        from
        (
        SELECT pod,CAST(SUBSTR(data_lettura,7,2) AS INT)giorno,eam lettura_monoraria,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,cod_flusso ,
        tipo_flusso ,max(CONCAT(data_lettura,data_ricezione,tipo_flusso_num)) over(partition by annomese,pod,cod_flusso)max_dt_lett_ric ,tipo_flusso_num,annomese,
        data_lettura ,CONCAT(data_lettura,data_ricezione,tipo_flusso_num) dt_lett_ric from misure.misure_storic_nora${suffix_must_elab_1G} where  annomese = ${annomese}  and motivazione <> '3'
        ) as tbl where dt_lett_ric = max_dt_lett_ric
      """.stripMargin */

    val str_misure_nora_base =
      s"""
        SELECT pod,giorno,lettura_monoraria,
        eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6,'E' tipo_dato,
        cod_flusso tipo_flusso,tipo_flusso tipo_flusso2,potf1,potf2,potf3,potm,annomese
        from
        (
        SELECT pod,CAST(SUBSTR(data_lettura,7,2) AS INT)giorno,eam lettura_monoraria,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,cod_flusso ,
        tipo_flusso ,tipo_flusso_num,annomese,data_lettura,potf1,potf2,potf3,potm,
        max(CONCAT(data_lettura,data_ricezione,tipo_flusso_num)) over(partition by annomese,pod,cod_flusso)max_dt_lett_ric,
        CONCAT(data_lettura,data_ricezione,tipo_flusso_num) dt_lett_ric
        from misure.misure_storic_nora${suffix_must_elab_1G} where  annomese = ${annomese}  and motivazione <> '3'
        ) as tbl where dt_lett_ric = max_dt_lett_ric
      """.stripMargin

    val tbl_nora_base = "tbl_nora_base"

    hiveCtx.sql(str_misure_nora_base)
      .write.option("parquet.block.size", blocksize.toString)
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("annomese")
      .save(s"${_path_base_misure}misure_non_orarie_base")

    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_non_orarie_base")
    log.info(s"Scrittura misure su tabella misure.misure_non_orarie_base per annomese : ${annomese} completata")
  //}
  }

  def write_misure_ora_base(annomese:String): Unit ={
    //scrittura su tabella misure.misure_orarie_base
    //if (new_mis_orarie) {
    val q_drop_parts: String = s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese=${annomese})"
    hiveCtx.sql(q_drop_parts)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")


    log.info("Avvio estrazione e scrittura misure su tabella misure.misure_orarie_base")

    //CAST(SUBSTR(data_lettura,7,2) AS INT)giornoquarti,CAST(SUBSTR(data_lettura,1,4) AS INT)annoquarti,
    //CAST(SUBSTR(data_lettura,5,2) AS INT)mesequarti,data_lettura annomesegiorno,

    //a parita di pod e annomesegiorno misura viene prediletta la misura di rettifica con data di ricezione piu recente
    val q_misure_orarie_base =
      s"""
        SELECT podquarti ,giornoquarti,annoquarti,mesequarti,annomesegiorno,
        consumo,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,perdita,potmax,Is2G,tipo_flusso,
        annomese,da_no_ora
        from
        (
        SELECT pod podquarti,CAST(SUBSTR(data_lettura,7,2) AS INT)giornoquarti,CAST(SUBSTR(data_lettura,1,4) AS INT)annoquarti,
        CAST(SUBSTR(data_lettura,5,2) AS INT)mesequarti,data_lettura annomesegiorno,consumo,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,perdita,potmax,Is2G,
        tipo_flusso,max(concat(tipo_flusso_num,data_ricezione)) over(partition by pod,data_lettura)max_dtric_tipo_flusso_num,
        concat(tipo_flusso_num,data_ricezione)dtric_tipo_flusso_num ,annomese,'0' da_no_ora
        from misure.misure_storic where annomese = ${annomese} and motivazione <> '3'
        ) as tbl where dtric_tipo_flusso_num = max_dtric_tipo_flusso_num
      """.stripMargin


    hiveCtx.sql(q_misure_orarie_base)
      .write.option("parquet.block.size", blocksize.toString)
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("annomese", "da_no_ora")
      .save(s"${_path_base_misure}misure_orarie_base")


    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")
    log.info(s"Scrittura misure su tabella misure.misure_orarie_base per annomese : ${annomese} completata")
    // }

  }

  // LE MISURE STORICHE SI DIVIDONO IN ORARIE E NON ORARIE
  // QUELLE NON ORARIE VENGONO SEMPRE AGGIORNATE
  // MENTRE QUELLE ORARIE VENGONO AGGIORNATE SOLAMENTE QUANTO VIENE ESEGUITA
  // L'ELABORAZIONE COMPLETA (36 MESI)
  def write_misure_storiche(annomesiL:Array[String]): Unit ={
    //tbl_misure_storic_tmp
    log.info(s"Avvio estrazione misure storiche su tabella di appoggio storic_tmp ")


    if(annomesiL.length== 0) {

      log.info(s"RESET TABELLA storic_tmp ")

      hiveCtx.sql("DROP TABLE IF EXISTS storic_tmp");
      hiveCtx.sql(
        s"""
                  CREATE TABLE storic_tmp(
                    cf_piva string,
                    pod string,
                    data_lettura string,
                    data_ricezione string,
                    motivazione string,
                    lettura_monoraria double,
                    lettura_f1 double,
                    lettura_f2 double,
                    lettura_f3 double,
                    lettura_f4 double,
                    lettura_f5 double,
                    lettura_f6 double,
                    ea string,
                    er string,
                    tipo_flusso string,
                    annomese_riferimento int,
                    data_lettura_num bigint)
                    PARTITIONED BY (
                    cod_pod char(2),
                    is_mis_oraria char(1)
                    )
                  ROW FORMAT SERDE
                    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                  STORED AS INPUTFORMAT
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                  LOCATION '${_path_base_storic_misure}storic_tmp'
                  TBLPROPERTIES("parquet.compression" = "SNAPPY")
                """)
    }

    //tbl_misure_storic_nora_tmp


    val partitions=hiveCtx.sql("show partitions misure.misure_storic_nora").collect()
    var j=0
    val numParts=if(elab_4mesi)numMesi else 3

    var whereAll=""
    val i_1=if((partitions.length - numMesi)>0 && (partitions.length - numMesi) < (partitions.length-1))partitions.length - numMesi else 0
    var part_init:String=""

    for (i <- i_1 to partitions.length-1){
      val par=partitions(i).getAs[String](0).substring(0,15)
      val where =  par
      val annomese=where.replace("annomese=","")
      if(part_init=="")
        part_init=annomese

      val anno=annomese.substring(0,4)
      val mese=annomese.substring(4,6)
      j=j+1



      val res=verifyannomesipar(anno.toInt, mese.toInt, annomesiL, true)

      if(res > 0) {
        whereAll= s" ${whereAll} ${where} OR"
        if (j%numParts==0 || i ==partitions.length -1) {
          whereAll = whereAll.substring(0, whereAll.length - 2)
          whereAll = s"( ${whereAll} )"

          if(!elab_4mesi) {

            log.info(s"Processamento misure storiche orarie utilizzando la partizione : ${whereAll}")
            val query =
              s"""
                SELECT  F.cf_piva,MIS.pod ,MIS.data_lettura_str data_lettura ,MIS.data_ricezione_str data_ricezione ,
                MIS.descr_motivazione motivazione ,CAST(null as DOUBLE) lettura_monoraria,
                MIS.eaf1  lettura_f1, MIS.eaf2 lettura_f2 , MIS.eaf3 lettura_f3 , MIS.eaf4 lettura_f4 , MIS.eaf5 lettura_f5 ,
                MIS.eaf6 lettura_f6, MIS.EA ea, MIS.ER er ,MIS.descr_tipoflusso tipo_flusso ,
                MIS.annomese annomese_riferimento ,MIS.data_lettura data_lettura_num,
                MIS.cod_pod,'1' is_mis_oraria FROM  (SELECT *,SUBSTR(pod,7,2) cod_pod FROM misure.misure_storic
                WHERE ${whereAll} DISTRIBUTE BY pod) MIS
                INNER JOIN tbl_forniture F ON MIS.pod = F.codice_pod
                WHERE  MIS.data_lettura >= F.inizio  AND MIS.data_lettura <= F.fine
                DISTRIBUTE BY MIS.cod_pod,is_mis_oraria
                """


            val dtOrari = hiveCtx.sql(query)

            dtOrari
              .write.option("parquet.block.size", blocksize.toString)
              .format("parquet")
              .partitionBy("cod_pod", "is_mis_oraria")
              .mode(SaveMode.Append)
              .save(s"${_path_base_storic_misure}storic_tmp")

            log.info(s"Processamento misure storiche orarie utilizzando la partizione : ${whereAll}  completata")
          }

          log.info(s"Processamento misure storiche non orarie utilizzando la partizione : ${whereAll}")
          val storic_f_nora =
            s"""
              select F.cf_piva,pod ,data_lettura_str data_lettura ,data_ricezione_str data_ricezione ,
              descr_motivazione motivazione ,
              case  when  nvl(eaf1,0) <> 0  then CAST(null as DOUBLE) else eam end lettura_monoraria,
		          eaf1  lettura_f1, eaf2 lettura_f2 , eaf3 lettura_f3 , eaf4 lettura_f4 , eaf5 lettura_f5 ,
              eaf6 lettura_f6, EA ea, ER er ,descr_tipoflusso tipo_flusso ,
              annomese annomese_riferimento ,data_lettura data_lettura_num,
              cod_pod,'0' is_mis_oraria
              FROM (SELECT * ,SUBSTR(pod,7,2) cod_pod
                    from misure.misure_storic_nora  WHERE ${whereAll} DISTRIBUTE BY pod)
              M_NORA INNER JOIN tbl_forniture F ON M_NORA.pod = F.codice_pod
              WHERE  M_NORA.data_lettura >= F.inizio  AND M_NORA.data_lettura <= F.fine
              DISTRIBUTE BY M_NORA.cod_pod,is_mis_oraria
            """.stripMargin


          val dtALL = hiveCtx.sql(storic_f_nora)//.unionAll(dtOrari)

          dtALL
            .write.option("parquet.block.size", blocksize.toString)
            .format("parquet")
            .partitionBy("cod_pod","is_mis_oraria")
            .mode(SaveMode.Append)
            .save(s"${_path_base_storic_misure}storic_tmp")

          log.info(s"Processamento misure storiche utilizzando la partizione : ${whereAll} completato")

          cleanMemory()

          whereAll = ""
          j = 0
        }
      }
    }

      hiveCtx.sql("MSCK REPAIR TABLE storic_tmp")
      if(elab_4mesi)
        {

          log.info("SCRITTURA MISURE NON ORARIE STORICHE NELLA TABELLA misure.misure_storic_f2")


          log.info(s"Partizione massima(esclusa) entro cui leggere le misure non orarie : ${part_init}")

          val query_tmp=
            s"""
              SELECT cf_piva,pod,data_lettura,data_ricezione,motivazione,lettura_monoraria,lettura_f1,lettura_f2,
               lettura_f3,lettura_f4,lettura_f5,lettura_f6,ea,er,tipo_flusso,annomese_riferimento,data_lettura_num,cod_pod,is_mis_oraria
               FROM misure.misure_storic_f2 WHERE cod_pod <> '' and is_mis_oraria='0' AND annomese_riferimento < ${part_init}
            """.stripMargin

          hiveCtx.sql(query_tmp)
            .write.option("parquet.block.size", blocksize.toString)
            .format("parquet")
            .partitionBy("cod_pod","is_mis_oraria")
            .mode(SaveMode.Append)
            .save(s"${_path_base_storic_misure}storic_tmp")

          log.info("Lettura effettuata")

          hiveCtx.sql(s"ALTER TABLE misure.misure_storic_f2 DROP IF EXISTS PARTITION(cod_pod <> '',is_mis_oraria='0')")

          val query1=s"""SELECT cf_piva,pod,data_lettura,data_ricezione,motivazione,lettura_monoraria,lettura_f1,lettura_f2,
          lettura_f3,lettura_f4,lettura_f5,lettura_f6,ea,er,tipo_flusso,annomese_riferimento,data_lettura_num,cod_pod,is_mis_oraria
          FROM storic_tmp WHERE cod_pod <> '' and is_mis_oraria='0'
         """
          log.info("Avvio Scrittura misure non orarie storiche ")

          hiveCtx.sql(query1)
            .write.option("parquet.block.size", blocksize.toString)
            .format("parquet")
            .partitionBy("cod_pod","is_mis_oraria")
            .mode(SaveMode.Append)
            .save(s"${_path_base_misure}misure_storic_f2")

          log.info("Scrittura misure non orarie storiche completata")
          hiveCtx.sql("DROP TABLE storic_tmp")
          hiveCtx.sql("MSCK REPAIR TABLE misure.misure_storic_f2")

          val ConnectionURL: String = prop.getProperty("spark.app.impala.url") + ";auth=PLAIN"
          val conImpala = DriverManager.getConnection(ConnectionURL)
          val stm = conImpala.createStatement()

          stm.execute("use misure")
          stm.execute("INVALIDATE METADATA")
          stm.execute("REFRESH misure_storic_f2")

          stm.close()
          conImpala.close()

          return
        }

      log.info("SCRITTURA MISURE STORICHE NELLA TABELLA MISURE_STORIC_F")
      hiveCtx.sql("DROP TABLE IF EXISTS misure_storic_f");
      
      hiveCtx.sql(
        s"""
        CREATE TABLE misure_storic_f (
                  cf_piva STRING,                                                                   
                  pod STRING,
                  data_lettura STRING,
                  data_ricezione STRING,
                  motivazione STRING,
                  lettura_monoraria DOUBLE,
                  lettura_f1 DOUBLE,
                  lettura_f2 DOUBLE,
                  lettura_f3 DOUBLE,
                  lettura_f4 DOUBLE,
                  lettura_f5 DOUBLE,
                  lettura_f6 DOUBLE,
                  ea STRING,
                  er STRING,
                  tipo_flusso STRING,
                  annomese_riferimento INT,
                  data_lettura_num BIGINT
                 )
                  PARTITIONED BY (cod_pod char(2),is_mis_oraria char(1))
                  ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                  STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                  LOCATION '${_path_base_storic_misure}misure/misure_storic_f'
                  TBLPROPERTIES ('parquet.compression'='SNAPPY')
        """.stripMargin)

      val query1="""SELECT cf_piva,pod,data_lettura,data_ricezione,motivazione,lettura_monoraria,lettura_f1,lettura_f2,
      lettura_f3,lettura_f4,lettura_f5,lettura_f6,ea,er,tipo_flusso,annomese_riferimento,data_lettura_num,cod_pod,is_mis_oraria
      FROM storic_tmp WHERE cod_pod <> '' and is_mis_oraria='0' """

      log.info("Avvio Scrittura misure non orarie storiche ")
      hiveCtx.sql(query1)
        .write.option("parquet.block.size", blocksize.toString)
        .format("parquet")
        .partitionBy("cod_pod","is_mis_oraria")
        .mode(SaveMode.Append)
        .save(s"${_path_base_storic_misure}misure/misure_storic_f")

      log.info("Scrittura misure non orarie storiche completata")


      val query2 =
          """SELECT cf_piva,pod,data_lettura,data_ricezione,motivazione,lettura_monoraria,lettura_f1,lettura_f2,
             lettura_f3,lettura_f4,lettura_f5,lettura_f6,ea,er,tipo_flusso,annomese_riferimento,data_lettura_num,cod_pod,is_mis_oraria
             FROM storic_tmp WHERE cod_pod <> '' and is_mis_oraria='1' """


    log.info("Avvio Scrittura misure orarie storiche ")

      hiveCtx.sql(query2)
        .write.option("parquet.block.size", blocksize.toString)
        .format("parquet")
        .partitionBy("cod_pod", "is_mis_oraria")
        .mode(SaveMode.Append)
        .save(s"${_path_base_storic_misure}misure/misure_storic_f")

    log.info("Scrittura misure orarie storiche completata")


      hiveCtx.sql("MSCK REPAIR TABLE misure_storic_f")
      log.info(s"SCRITTURA MISURE STORICHE  COMPLETATA")


  }

  def write_misure_storicheGAS(): Unit = {
    //tbl_misure_storic_tmp
    log.info(s"Avvio estrazione misure storiche su tabella di appoggio storic_tmp ")


    log.info(s"RESET TABELLA storic_tmp_gas ")

    hiveCtx.sql("DROP TABLE IF EXISTS storic_tmp_gas");
    hiveCtx.sql(
      s"""
                  CREATE TABLE storic_tmp_gas(
                    cf_piva string,
                    pdr string,
                    annomese_riferimento string,
                    data_lettura string,
                    dt_caricamento string,
                    flusso string,
                    motivazione string,
                    let_tot_prel string)
                    PARTITIONED BY (
                    cod_pdr char(3)
                    )
                  ROW FORMAT SERDE
                    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                  STORED AS INPUTFORMAT
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                  LOCATION '${_path_base_storic_misure}storic_tmp_gas'
                  TBLPROPERTIES("parquet.compression" = "SNAPPY")
                """)

    //tbl_misure_storic_nora_tmp
    log.info(s"Processamento misure storiche gas ")


    val query1 =
      s"""
        select cod_pdr pdr,mese_comp as    annomese_riferimento,
        dt_caricamento,let_tot_prel,
        case when (tipo_flusso ='TGL' OR tipo_flusso ='RGL' OR tipo_flusso ='RMV') then data_comp
             when (tipo_flusso ='TML' OR tipo_flusso ='RML' OR tipo_flusso ='VTG6') then data_racc
             else data_lettura end data_lettura,
        (case when motivazione = '1' then 'Misura che sostituisce una stima precedente.'
         when motivazione = '2' then 'Misura che sostituisce una misura fornita precedentemente errata.'
         when motivazione = '3' then 'Misura fornita precedentemente per errore.'
         when motivazione = '4' then 'Ricostruzione per frode.'
         when motivazione = '5' then 'Ricostruzione per malfunzionamento misuratore.'
         else motivazione
         end) as motivazione,
         case when (tipo_flusso ='TGL' OR tipo_flusso ='TML') then 'Lettura Periodica'
              when (tipo_flusso ='RGL' OR tipo_flusso ='RML' ) then 'Lettura di Rettifica'
              when (tipo_flusso ='TAL' OR tipo_flusso ='TAV' ) then 'Autolettura'
              when (tipo_flusso ='RMV') then 'Lettura di Rettifica Voltura'
              when (tipo_flusso ='VTG6') then 'Lettura Voltura'
             else CONCAT('Lettura flusso : ',tipo_flusso) end as flusso,
         codice_fornitura,SUBSTR(cod_pdr,7,3) cod_pdr
         from misuregas.misure_storic DISTRIBUTE BY codice_fornitura
       """
    val dtALL = hiveCtx.sql(query1)
    dtALL.registerTempTable("all_misure_gas")


    //A PARITA DI VALORE DELLE MISURA (PER TIPO FLUSSO,PDR,COD_FISC EMESE) PRENDO L'ULTIMA MISURA CARICATA
    //IN QUESTO MODO PRENDO TUTTE LE MISURE E NEL CASO DI MISURE GIORNALIERE CON DATA LETTURA DIVERSA MA
    //AVENTE LO STESSO VALORE PRENDO L'ULTIMA MISURA CARICATA
    val queryS =
    s"""
        select t_codice_fiscale cf_piva,pdr, annomese_riferimento, max(data_lettura) data_lettura, max(dt_caricamento)dt_caricamento, flusso,
        motivazione, let_tot_prel,cod_pdr
        from  all_misure_gas INNER JOIN
        (SELECT t_codice_fiscale,codice_fornitura from misuregas.forniture_gas DISTRIBUTE BY codice_fornitura)forniture_gas
        on all_misure_gas.codice_fornitura= forniture_gas.codice_fornitura
        group by t_codice_fiscale,pdr, annomese_riferimento, flusso,motivazione, let_tot_prel,cod_pdr
      """.stripMargin

    hiveCtx.sql(queryS)
      .write.option("parquet.block.size", blocksize.toString)
      .format("parquet")
      .partitionBy("cod_pdr")
      .mode(SaveMode.Append)
      .save(s"${_path_base_storic_misure}storic_tmp_gas")


    log.info(s"SCRITTURA MISURE STORICHE GAS COMPLETATA")

    hiveCtx.sql("MSCK REPAIR TABLE storic_tmp_gas")
    hiveCtx.dropTempTable("all_misure_gas")

    val ConnectionURL: String = prop.getProperty("spark.app.impala.url") + ";auth=PLAIN"
    val conImpala = DriverManager.getConnection(ConnectionURL)
    val stm = conImpala.createStatement()

    stm.execute("use default")
    stm.execute("INVALIDATE METADATA")
    stm.execute("REFRESH storic_tmp_gas")

    stm.close()
    conImpala.close()

    log.info(s"REFRESH TABELLA storic_tmp_gas COMPLETATO")

  }



  def calcMisure_storic_no_ora(anno:Int,mese:Int,tblpodorari:String,must_elab_1G:Boolean): Unit ={

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    log.info(s"Estrazione misure non orarie storiche per il mese ${mese} e anno ${anno}")

    //hiveCtx.sql("SET hive.auto.convert.join=false")

    val suffix_must_elab_1G:String = if(must_elab_1G)"_test" else ""
    val suffix_m_tmp:String = if(must_elab_1G)"_test" else ""


    val annomesegiornoelabArr=hiveCtx.sql(s"SELECT annomesegiornoelab  FROM misure.last_elab_ee${suffix_m_tmp} WHERE annomese = ${annomese} and mis_ora = '0'").collect()
    val annomesegiornoelab = if(annomesegiornoelabArr.length==0)"19990101" else annomesegiornoelabArr(0).get(0).toString



    log.info(s"tabella misure.last_elab_ee${suffix_m_tmp} - annomesegiornoelab no orari : ${annomesegiornoelab}")
    val where1G=if(read1G_no_orarie || must_elab_1G) "((tipo_flusso in('PNO','PNO2G','VNO','VNO2G') AND tipodato_e=1) OR tipo_flusso IN ('RNO','RNO2G','RNV','RNV2G'))" else "((tipo_flusso in('PNO2G','VNO2G') AND tipodato_e=1) OR tipo_flusso IN ('RNO2G','RNV2G'))"

    if(must_elab_1G || read1G_no_orarie) {
      if(must_elab_1G)
        log.info("Estrazione forzata/di test per le misure 1G noaggr ")
      else
        log.info("Estrazione  misure 1G noaggr da proprietà impostata su job.properties")

      log.info(s"Where applicata : ${where1G}")
    }

    //AND CAST(CONCAT(YEAR(dataelaborazione),LPAD(month(dataelaborazione),2,0),LPAD(day(dataelaborazione),2,0)) AS INT) >= ${annomesegiornoelab} DISTRIBUTE BY pod14
    val noaggr=
      s"""
        SELECT pod, eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,data_rilevazione,EaM,annomesegiornodir,motivazione,
                 giorno,SUBSTR(tipo_flusso,1,3)tipo_flusso,data_voltura,data_misura,
                 case tipo_flusso when 'PNO' THEN 1 when 'PNO2G' then 2 when 'RNO' then 3 when 'RNO2G' then 4
                 when 'VNO' THEN 5 when 'VNO2G' then 6 when 'RNV' then 7 when 'RNV2G' then 8
                 else 0 END tipo_flusso_num ,anno,mese,time_stamp,dataelaborazione,SUBSTR(X.pod,1,14) pod14,
                 potf1,potf2,potf3,potm
                  from ${_dbDest}.flusso_misure_noaggr${tbl_new} X where  ${where1G} and
                      (anno = ${anno} AND mese =${mese} AND VALIDATO ='S' AND
                       (eaf1 IS NOT NULL OR eaf2 IS NOT NULL OR eaf3 IS NOT NULL OR eaf4 IS NOT NULL OR eaf5 IS NOT NULL OR eaf6 IS NOT NULL OR EaM IS NOT NULL )
                       )
                  AND CAST(date_format(dataelaborazione,'yyyyMMdd') as int) >= ${annomesegiornoelab} DISTRIBUTE BY pod14

      """.stripMargin

    val tbl_misure_nora_cl_base_storic="tbl_misure_no_orarie_cloudera_base_storic"

    //LEFT OUTER JOIN ${tblpodorari} as pods on codice_pod = noaggr.pod14
   // where  pods.codice_pod is null

   //CREATE TABLE ${tbl_misure_nora_cl_base_storic} STORED AS PARQUET AS
    val tmp =
      s"""
          SELECT EaM as lettura_monoraria ,
          TBKX.pod14 as pod,annomese,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,CAST(annomesegiornodir AS BIGINT) as data_ricezione,
          CASE WHEN tipo_flusso ='VNO' OR tipo_flusso ='RNV' THEN 'VNO_RNV' ELSE 'PNO_RNO' END tipo_flusso,
          CAST(concat(annomese,LPAD(giorno,2,0) ) AS BIGINT) annomesegiorno,times_elab ,
          data_rilevazione data_rilevazione_str,tipo_flusso_num,nvl(motivazione,'')motivazione,EA,ER,
          potf1,potf2,potf3,potm
          FROM
          (
           SELECT '' EA,'' ER,
           pod,pod14, eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,potf1,potf2,potf3,potm,
           data_rilevazione,EaM,annomesegiornodir,motivazione,
           CAST( (CASE WHEN nvl(giorno,0)<>0 THEN giorno WHEN tipo_flusso ='VNO' OR tipo_flusso ='RNV' THEN substr(data_voltura,1,2) ELSE substr(data_misura,1,2) END) AS INT) giorno,
           tipo_flusso,tipo_flusso_num ,
           CONCAT(time_stamp,unix_timestamp(dataelaborazione)) times_elab ,
           ${annomese} annomese
           from (${noaggr}) noaggr

           ) AS TBKX
      """.stripMargin



   // hiveCtx.sql(s"DROP TABLE IF EXISTS ${tbl_misure_nora_cl_base_storic}")
    //hiveCtx.sql(tmp)
    val dtmisure_no_orarie_cloudera_base = hiveCtx.sql(tmp) //.persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_no_orarie_cloudera_base.cache()
    dtmisure_no_orarie_cloudera_base.registerTempTable(tbl_misure_nora_cl_base_storic)



    val tbl_misure_nora_cloud_2="tbl_misure_no_orarie_cloudera_2"
    val str_misure_norarie_cloudera=
      s"""
        SELECT pod,annomese,lettura_monoraria,
		    eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,tipo_flusso,
		    data_lettura,data_ricezione,tipo_flusso_num,data_rilevazione,motivazione,EA,ER,
        potf1,potf2,potf3,potm
        FROM (
        SELECT lettura_monoraria ,A.pod,annomese,
        eaf1,eaf2,eaf3,
		    eaf4,eaf5,eaf6,
		    tipo_flusso,annomesegiorno data_lettura, times_elab, data_ricezione,
        max(A.times_elab) over ( partition by A.annomesegiorno,A.pod,A.tipo_flusso_num) max_times_elab,
        CAST(CONCAT(SUBSTR(data_rilevazione_str,7,4),SUBSTR(data_rilevazione_str,4,2),SUBSTR(data_rilevazione_str,1,2)) AS BIGINT)data_rilevazione,
        tipo_flusso_num,motivazione,EA,ER,potf1,potf2,potf3,potm
        FROM ${tbl_misure_nora_cl_base_storic} AS A
        ) XX WHERE times_elab =  max_times_elab

      """.stripMargin


    val dtmisure_no_orarie_cloudera=hiveCtx.sql(str_misure_norarie_cloudera)//.persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_no_orarie_cloudera.registerTempTable(tbl_misure_nora_cloud_2)




    //LEFT OUTER JOIN ${tblpodorari}  as pods
    //on pods.codice_pod = prt_tmo_mn_f.codice_pod
    //where  pods.codice_pod is null

    val strmisure_norarie_novno_all=s"""
                              SELECT pod,data_lettura,CAST(data_ricezione AS BIGINT)data_ricezione,motivazione,
                              max(lettura_monoraria)eam,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,max(eaf6)eaf6,EA,ER,
                              0.0 consumo,0.0 perdita,0.0 potmax,'0' Is2G,tipo_flusso_num,tipo_flusso cod_flusso,
                              case max(tipo_flusso_num) when 1 THEN 'PNO'  when 2 then 'PNO2G'  when 3 then 'RNO'  when 4 then 'RNO2G'
                              when 5 then 'VNO' when 6 then 'VNO2G' when 7 then 'RNV' when 8 then 'RNV2G' else '' end tipo_flusso,
                              max(potf1)potf1,max(potf2)potf2,max(potf3)potf3,max(potm)potm,annomese
                              FROM
                              (
                              select *,max(dt_filter) over(partition by KEY_PART) max_dt_filter
                              from(
                              SELECT pod,annomese,lettura_monoraria,
                              eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,tipo_flusso,
                              data_lettura,data_ricezione,
                              potf1,potf2,potf3,potm,
                              concat(data_lettura,data_ricezione)dt_filter,tipo_flusso_num,
                              motivazione,EA,ER,CONCAT(data_lettura,pod,tipo_flusso_num)KEY_PART
                              from ${tbl_misure_nora_cloud_2} where tipo_flusso <> 'VNO_RNV'
                              UNION ALL
                              SELECT prt_tmo_mn_f.codice_pod pod,annomese, eam lettura_monoraria,
                              eaf1, eaf2, eaf3, CAST(NULL AS DOUBLE) eaf4, CAST(NULL AS DOUBLE) eaf5, CAST(NULL AS DOUBLE) eaf6,'PNO_RNO' tipo_flusso,
                              data_lettura,data_ricezione,
                              cast(null as double) potf1,cast(null as double) potf2, cast(null as double)potf3, cast(null as double)potm,
                              concat(data_lettura,data_ricezione)dt_filter,tipo_flusso_num,
                              nvl(motivazione,'')motivazione,'' EA, '' ER,CONCAT(data_lettura,prt_tmo_mn_f.codice_pod,tipo_flusso_num)KEY_PART
                              from (select *,case cod_flusso when 'PNO' THEN 1 when 'RNO' then 3 else 0 end tipo_flusso_num from misure.prt_tmo_mn_f${suffix_must_elab_1G}
                              where annomese =${annomese} and d_upload  >= ${annomesegiornoelab}
                              AND (eaf1 IS NOT NULL OR eaf2 IS NOT NULL OR eaf3 IS NOT NULL  OR eam IS NOT NULL ) DISTRIBUTE BY codice_pod) prt_tmo_mn_f

                              ) as X
                              ) AS TBL
                              WHERE dt_filter = max_dt_filter
                              GROUP BY pod,data_lettura,data_ricezione,motivazione,tipo_flusso_num,tipo_flusso,annomese,EA,ER
                              """

    log.info(s"""Estrazione misure non orarie oracle/cloudera """)

    val tbl_misure_nora_c1="tbl_misure_nora_c1"
    val dtmisure_no_orarie_novno_all=hiveCtx.sql(strmisure_norarie_novno_all)

    //LEFT OUTER JOIN ${tblpodorari}  as pods
    //on pods.codice_pod = prt_tmo_mv_f.codice_pod
    //where pods.codice_pod is null

    //la data_lettura corrisponde dalla data_voltura
    val strmisure_norarie_vno_all=s"""
                              SELECT pod,data_lettura,CAST(data_ricezione AS BIGINT)data_ricezione,motivazione,
                              max(lettura_monoraria)eam,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,max(eaf6)eaf6,EA,ER,
                              0.0 consumo,0.0 perdita,0.0 potmax,'0' Is2G,tipo_flusso_num,tipo_flusso cod_flusso,
                              case max(tipo_flusso_num) when 1 THEN 'PNO'  when 2 then 'PNO2G'  when 3 then 'RNO'  when 4 then 'RNO2G'
                              when 5 then 'VNO' when 6 then 'VNO2G' when 7 then 'RNV' when 8 then 'RNV2G' else '' end tipo_flusso,
                              max(potf1)potf1,max(potf2)potf2,max(potf3)potf3,max(potm)potm,annomese
                              FROM
                              (
                              select *,max(dt_filter) over(partition by KEY_PART) max_dt_filter
                              from(
                              SELECT pod,annomese,lettura_monoraria,
                              eaf1,eaf2,eaf3,eaf4,eaf5,eaf6 ,tipo_flusso,
                              data_lettura,nvl(data_rilevazione,data_lettura) data_ricezione,
                              potf1,potf2,potf3,potm,
                              concat(data_lettura,nvl(data_rilevazione,data_lettura))dt_filter,tipo_flusso_num,
                              motivazione,EA,ER,CONCAT(data_lettura,pod,tipo_flusso_num)KEY_PART
                              from ${tbl_misure_nora_cloud_2} where tipo_flusso = 'VNO_RNV'
                              UNION ALL
                              SELECT prt_tmo_mv_f.codice_pod pod,annomese, eam lettura_monoraria,
                              eaf1, eaf2, eaf3,CAST(NULL AS DOUBLE) eaf4,CAST(NULL AS DOUBLE) eaf5,CAST(NULL AS DOUBLE) eaf6,'VNO_RNV' tipo_flusso,
                              data_voltura data_lettura,nvl(d_rilevazione,data_voltura) data_ricezione,
                              cast(null as double) potf1,cast(null as double) potf2, cast(null as double)potf3, cast(null as double)potm,
                              concat(data_voltura,nvl(d_rilevazione,data_voltura))dt_filter,tipo_flusso_num,
                              '' motivazione,'' EA, '' ER, CONCAT(data_voltura,prt_tmo_mv_f.codice_pod,tipo_flusso_num)KEY_PART
                              from (select *,case cod_flusso when 'VNO' THEN 5 when 'RNV' then 7 else 0 end tipo_flusso_num
                              from misure.prt_tmo_mv_f${suffix_must_elab_1G}
                               where annomese =${annomese} and d_upload  >= ${annomesegiornoelab}
                                AND (eaf1 IS NOT NULL OR eaf2 IS NOT NULL OR eaf3 IS NOT NULL  OR eam IS NOT NULL ) DISTRIBUTE BY codice_pod) prt_tmo_mv_f

                              ) as X
                              ) AS TBL
                              WHERE dt_filter = max_dt_filter
                              GROUP BY pod,data_lettura,data_ricezione,motivazione,tipo_flusso_num,tipo_flusso,annomese,EA,ER
                              """

    log.info(s"""Estrazione volture oracle/cloudera """)
    val dtmisure_no_orarie_vno_all= hiveCtx.sql(strmisure_norarie_vno_all)

    val tbl_nora_all ="tbl_all_nora"
    val dtmisure_no_orarie_all = dtmisure_no_orarie_novno_all.unionAll(dtmisure_no_orarie_vno_all)
    dtmisure_no_orarie_all.registerTempTable(tbl_nora_all)


    log.info(s"Elaborazione misure non orarie su tabella  misure.misure_storic_nora_tmp per anno =${anno} e mese=${mese}")


    hiveCtx.sql("DROP TABLE IF EXISTS misure.misure_storic_nora_tmp")

    val q_nora_forn=
      s"""
        CREATE TABLE misure.misure_storic_nora_tmp STORED AS PARQUET AS
        SELECT DISTINCT pod,data_lettura,data_ricezione,motivazione,
        eam,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,EA,ER,
        potf1,potf2,potf3,potm,
        consumo, perdita, potmax,Is2G,tipo_flusso_num,cod_flusso,tipo_flusso,
        case
        WHEN tipo_flusso='PNO' OR tipo_flusso='PNO2G' THEN 'Lettura Periodica'
        WHEN tipo_flusso='VNO' OR tipo_flusso='VNO2G' THEN 'Lettura Voltura'
        WHEN tipo_flusso='RNO' OR tipo_flusso='RNO2G' THEN 'Lettura di Rettifica'
        WHEN tipo_flusso='RNV' OR tipo_flusso='RNV2G' THEN 'Lettura di rettifica Voltura'
        ELSE tipo_flusso END descr_tipoflusso,
        case motivazione
        when '1' then 'misura che sostituisce una stima precedente'
        when '2' then 'misura che sostituisce una misura fornita precedentemente errata'
        when '3' then 'misura fornita precedentemente per errore'
        when '4' then 'ricostruzione per frode'
        when '5' then 'ricostruzione per malfunzionamento misuratore'
        else motivazione end descr_motivazione,
        concat( SUBSTR(data_lettura,7,2),'/',SUBSTR(data_lettura,5,2),'/',SUBSTR(data_lettura,1,4)) data_lettura_str,
        concat( SUBSTR(data_ricezione,7,2),'/',SUBSTR(data_ricezione,5,2),'/',SUBSTR(data_ricezione,1,4)) data_ricezione_str,
        annomese ,CONCAT(pod,data_lettura,data_ricezione,tipo_flusso_num) as KK_CHECK
        FROM (SELECT * , CONCAT(data_lettura,data_ricezione,tipo_flusso_num) dt_lett_ric,
               max(CONCAT(data_lettura,data_ricezione,tipo_flusso_num)) over(partition by annomese,pod,cod_flusso)max_dt_lett_ric
              FROM ${tbl_nora_all})  TBL_STOR
        WHERE dt_lett_ric = max_dt_lett_ric

      """.stripMargin

    hiveCtx.sql(q_nora_forn)


    val query_nora_forn=if(annomesegiornoelab!="19990101") {
      //VENGONO PRESE SOLAMENTE LE MISURE NUOVE CHE NON SIANO STATE GIA INGERITE

      log.info(s"Estrazione misure non orarie nuove non ancora inserite nell'eleborazione per l'annomese : ${annomese}")

      s""" SELECT DISTINCT pod ,data_lettura ,data_ricezione ,motivazione ,eam ,eaf1 ,eaf2 ,eaf3 ,eaf4 ,eaf5 ,eaf6 ,ea ,er ,consumo ,perdita ,potmax ,
                   is2g ,tipo_flusso_num ,cod_flusso ,tipo_flusso ,descr_tipoflusso ,descr_motivazione ,data_lettura_str ,data_ricezione_str ,
                   potf1,potf2,potf3,potm,
                   annomese  from (SELECT * FROM misure.misure_storic_nora_tmp DISTRIBUTE BY KK_CHECK )A
                   LEFT OUTER JOIN (select CONCAT(pod,data_lettura,data_ricezione,tipo_flusso_num) KK_CHECK from misure.misure_storic_nora${suffix_must_elab_1G} where annomese =${annomese} DISTRIBUTE BY KK_CHECK) B
                   ON A.KK_CHECK = B.KK_CHECK WHERE B.KK_CHECK IS NULL
               """
    }else{
      s""" SELECT pod ,data_lettura ,data_ricezione ,motivazione ,eam ,eaf1 ,eaf2 ,eaf3 ,eaf4 ,eaf5 ,eaf6 ,ea ,er ,consumo ,perdita ,potmax ,
                   is2g ,tipo_flusso_num ,cod_flusso ,tipo_flusso ,descr_tipoflusso ,descr_motivazione ,data_lettura_str ,data_ricezione_str ,
                   potf1,potf2,potf3,potm,
                   annomese  from misure.misure_storic_nora_tmp
               """
    }

    val dtNo_Ora_Diff= hiveCtx.sql(query_nora_forn)
    dtNo_Ora_Diff.registerTempTable("tbl_misure_storic_nora_tmp")


    //val cc =hiveCtx.sql("SELECT COUNT(*) FROM tbl_misure_storic_nora_tmp").collect()(0).getAs[Long](0)
    //log.info("NUMERO DI MISURE NON ORARIE NUOVE IN TBL_MISURE_STORIC_NORA_TMP : " + cc.toString )


   // if(cc >0) {
   val numPartPiene: org.apache.spark.Accumulator[Int] = hiveCtx.sparkContext.accumulator(0, "Partitioni_piene")
    dtNo_Ora_Diff.foreachPartition(f=> if(f.length > 0)numPartPiene.add(1))
    //numPartPiene.add(1)

    if(numPartPiene.value >0) {
      log.info(s"NumPartPiene Nora : ${numPartPiene.value}")

    //NELLE MISURE NON ORARIE NON POSSO SALVARE I NUOVI DATI COME STORICO
    //ESSENDO MISURE MENSILI E QUINDI POTREBBERO ESSERE INSERITE PIU MISURE NELL'ARCO DELLO STESSO MESE
    //PER UNO O PIU POD . LO STORICO DELLE MISURE NON ORARIE VIENE LANCIATO DALLA PROCEDURA DI POPOLAMENTO
    //MISURE STORICHE
      log.info(s"SCRITTURA MISURE NON ORARIE su tabella  misure.misure_storic_nora${suffix_must_elab_1G} per anno =${anno} e mese=${mese}")

      new_mis_no_orarie=true

      dtNo_Ora_Diff
        .write.option("parquet.block.size", blocksize.toString)
        .format("parquet")
        .partitionBy("annomese")
        .mode(SaveMode.Append)
        .save(s"${_path_base_misure}misure_storic_nora${suffix_must_elab_1G}")


       hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_storic_nora${suffix_must_elab_1G}")
       hiveCtx.refreshTable(s"misure.misure_storic_nora${suffix_must_elab_1G}")

       hiveCtx.sql(s"ALTER TABLE misure.last_elab_ee${suffix_m_tmp} DROP IF EXISTS PARTITION(annomese=${annomese},mis_ora='0')")
       hiveCtx.sql(s"insert into misure.last_elab_ee${suffix_m_tmp} PARTITION(annomese,mis_ora) select ${curr_elab} annomesegiornoelab , ${annomese} annomese , '0' mis_ora")


    }


      //hiveCtx.sql(s"DROP TABLE IF EXISTS ${tbl_misure_nora_cl_base_storic}")
      dtmisure_no_orarie_cloudera_base.unpersist(true)
      hiveCtx.dropTempTable(tbl_misure_nora_cl_base_storic)


      hiveCtx.dropTempTable(tbl_misure_nora_cloud_2)
      hiveCtx.dropTempTable(tbl_nora_all)

    if(numPartPiene.value >=0)
      write_misure_nora_base(annomese,false)
    else
      log.info(s"ELaborazione misure non orarie per l'anno e mese : ${annomese} completata!")



  }

  def calMisureOrarieGG_Mese_DeltaAnnoMese(anno:Int,mese:Int): Unit ={

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)
    val annomeseprev = if(mese==1)(anno-1).toString +"12" else anno.toString + (("0" + (mese -1).toString) takeRight 2)

    hiveCtx.sql("set hive.exec.dynamic.partition=true")
    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveCtx.sql("set hive.mapred.mode = nonstrict")
    hiveCtx.sql("set hive.exec.parallel=true")

    val q_drop_part:String=s"ALTER TABLE misure.misure_orarie_c DROP IF EXISTS PARTITION(competenza_consumi=${annomese})"
    hiveCtx.sql(q_drop_part)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_c")
    hiveCtx.refreshTable(s"misure.misure_orarie_c")


    val q_drop_part2:String=s"ALTER TABLE misure.misure_mensili_c DROP IF EXISTS PARTITION(competenza_consumi=${annomese})"
    hiveCtx.sql(q_drop_part2)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_mensili_c")
    hiveCtx.refreshTable(s"misure.misure_mensili_c")


    log.info(s"Avvio calcolo delta consumi giornalieri , fasce + incrocio con forniture per l'annomese: ${annomese} considerando l'annomese precedente : ${annomeseprev}")

   // hiveCtx.sql(s"DROP TABLE IF EXISTS tbl_orarie_tmp")
    //CREATE TABLE tbl_orarie_tmp STORED AS PARQUET AS
    val tmp=s"""

            SELECT cf_piva,n_id_fornitura,podquarti,giornoquarti,annomese,annomesegiorno,
             consumo,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,perdita,potmax,is2g,
             CAST(prev_dati_arr[0] AS DOUBLE)prev_consumo,
             CAST(prev_dati_arr[1] AS DOUBLE)prev_eaf1,
             CAST(prev_dati_arr[2] AS DOUBLE)prev_eaf2,
             CAST(prev_dati_arr[3] AS DOUBLE)prev_eaf3,
             CAST(prev_dati_arr[4] AS DOUBLE)prev_eaf4,
             CAST(prev_dati_arr[5] AS DOUBLE)prev_eaf5,
             CAST(prev_dati_arr[6] AS DOUBLE)prev_eaf6,tipo_flusso,da_no_ora
            FROM
            (
             SELECT cf_piva,n_id_fornitura,podquarti,giornoquarti,annomese,annomesegiorno,
             consumo,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,perdita,potmax,is2g,
             split(prev_dati,'\b')prev_dati_arr,tipo_flusso,da_no_ora
             FROM
             (
              SELECT M.cf_piva,M.n_id_fornitura,M.podquarti,M.giornoquarti,M.annomese,M.annomesegiorno,
              M.consumo,M.eaf1  , M.eaf2  , M.eaf3  , M.eaf4  , M.eaf5  , M.eaf6 ,M.perdita,M.potmax,M.is2g,
              LAG(CONCAT(NVL(M.consumo,'NULL'),'\b',NVL(M.eaf1,'NULL'),'\b',NVL(M.eaf2,'NULL'),'\b',NVL(M.eaf3,'NULL'),'\b',NVL(M.eaf4,'NULL'),'\b',NVL(M.eaf5,'NULL'),'\b',NVL(M.eaf6,'NULL')))
              over (partition by CONCAT(M.cf_piva,M.podquarti) order by M.annomesegiorno) prev_dati,
              tipo_flusso,da_no_ora
              FROM (
               SELECT cf_piva,n_id_fornitura,MO.podquarti,MO.giornoquarti,MO.annomese,MO.annomesegiorno,
                MO.consumo,MO.eaf1  , MO.eaf2  , MO.eaf3  , MO.eaf4  , MO.eaf5  , MO.eaf6 ,MO.perdita,MO.potmax,
                MO.Is2G is2g,MO.tipo_flusso,MO.da_no_ora
                FROM (SELECT * FROM  misure.misure_orarie_base where annomese in( ${annomese},${annomeseprev}) DISTRIBUTE BY podquarti ) MO
                INNER JOIN tbl_forniture F ON MO.podquarti = F.codice_pod
                WHERE (MO.annomesegiorno >= F.inizio AND MO.annomesegiorno <= F.fine )
               )M
              ) TBL_SPLITTED
             )TBL_F WHERE annomese = ${annomese}
         """.stripMargin

    val dtx =hiveCtx.sql(tmp)//.persist(StorageLevels.MEMORY_AND_DISK_SER)
    //dtx.cache()
    dtx.registerTempTable("tbl_orarie_tmp")


    val str_misure_orarie_delta=
      s"""
          INSERT INTO misure.misure_orarie_c PARTITION(competenza_consumi)
          SELECT cf_piva,n_id_fornitura, pod, giorno,tipo_misura,potenza_max_erogata,lettura_giornaliero_f1,
          lettura_giornaliero_f2,lettura_giornaliero_f3,lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,
          delta_misure_f1,delta_misure_f2,delta_misure_f3,delta_misure_f4,delta_misure_f5,delta_misure_f6,consumo_giornaliero_gg,
          is2g,tipo_flusso,data_lettura,competenza_consumi
          FROM
          (
           SELECT cf_piva,n_id_fornitura,podquarti pod,giornoquarti giorno,'E' tipo_misura,
           round(potmax,2) potenza_max_erogata,round(eaf1,2) lettura_giornaliero_f1 ,round(eaf2,2) lettura_giornaliero_f2,
           round(eaf3,2) lettura_giornaliero_f3,round(eaf4,2) lettura_giornaliero_f4 ,round(eaf5,2) lettura_giornaliero_f5,
           round(eaf6,2) lettura_giornaliero_f6,
		       case when prev_eaf1 > eaf1 then 0 else round((eaf1-prev_eaf1),2) end delta_misure_f1,
		       case when prev_eaf2 > eaf2 then 0 else round((eaf2-prev_eaf2),2) end delta_misure_f2,
		       case when prev_eaf3 > eaf3 then 0 else round((eaf3-prev_eaf3),2) end delta_misure_f3,
		       case when prev_eaf4 > eaf4 then 0 else round((eaf4-prev_eaf4),2) end delta_misure_f4,
		       case when prev_eaf5 > eaf5 then 0 else round((eaf5-prev_eaf5),2) end delta_misure_f5,
		       case when prev_eaf6 > eaf6 then 0 else round((eaf6-prev_eaf6),2) end delta_misure_f6,
           case when round(consumo,2) < 0 or cast(nvl(eaf1,0) as int) <> 0 then 0 else round(consumo,2) end consumo_giornaliero_gg ,is2g,
           annomese competenza_consumi,tipo_flusso,annomesegiorno data_lettura,da_no_ora
           FROM tbl_orarie_tmp AS TBL
           ) AS TTX WHERE da_no_ora ='0'
      """.stripMargin


    val dtmisure_o_delta= hiveCtx.sql(str_misure_orarie_delta)
    log.info(s"Scrittura misure orarie  effettuata su tabella  misure.misure_orarie_c ")
    //dtx.unpersist(true)
    hiveCtx.dropTempTable("tbl_orarie_tmp")
    //hiveCtx.sql(s"DROP TABLE IF EXISTS tbl_orarie_tmp")



    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_c")
    hiveCtx.refreshTable(s"misure.misure_orarie_c")


    log.info(s"Estrazione misure orarie mensili   per l'annomese: ${annomese} considerando l'annomese precedente : ${annomeseprev} " )


    /*
    DA VERIFICARE LA MISURA MONORARIA CON DELTA PER LE MISURE ORARIE

     sum(consumo_giornaliero_gg) over(partition by n_id_fornitura,competenza_consumi)lettura_misura_monoraria,

    */



    val tbl_tmm= "misure.tbl_mm1"
    hiveCtx.sql(s"DROP TABLE IF EXISTS ${tbl_tmm}")

    val strorarie_mensili1=
      s"""
         CREATE TABLE ${tbl_tmm} STORED AS PARQUET AS
         select sum(CASE WHEN Is2G='0' THEN consumo_giornaliero_gg ELSE 0 END) delta_misura_monoraria,
         sum(consumo_giornaliero_gg)delta_misura_monoraria_2,
         sum(delta_misure_f1)delta_misure_f1,sum(delta_misure_f2)delta_misure_f2,sum(delta_misure_f3)delta_misure_f3,
         sum(delta_misure_f4)delta_misure_f4,sum(delta_misure_f5)delta_misure_f5,sum(delta_misure_f6)delta_misure_f6,
         max(data_lettura)data_lettura_max,
         concat(n_id_fornitura,pod,competenza_consumi)KK_JOIN
         from misure.misure_orarie_c where competenza_consumi =${annomese} and n_id_fornitura <> ''
         group by n_id_fornitura,pod,competenza_consumi
         DISTRIBUTE  BY KK_JOIN
      """.stripMargin

    hiveCtx.sql(strorarie_mensili1)

    val strorarie_mensili =
      s"""
         INSERT INTO misure.misure_mensili_c PARTITION(competenza_consumi,da_antswitch)
         select distinct cf_piva,n_id_fornitura,tipo_misura,lettura_misura_monoraria,
         CASE WHEN (NVL(lettura_misura_f1,0.0)+NVL(lettura_misura_f2,0.0)+NVL(lettura_misura_f3,0.0)+NVL(lettura_misura_f4,0.0)+NVL(lettura_misura_f5,0.0)+NVL(lettura_misura_f6,0.0))=0.0 THEN
         round(delta_misura_monoraria_2,2) ELSE round(delta_misura_monoraria,2) END delta_misura_monoraria,
         lettura_misura_f1,lettura_misura_f2,lettura_misura_f3,lettura_misura_f4,lettura_misura_f5,lettura_misura_f6,
         round(delta_misure_f1,2)delta_misure_f1,round(delta_misure_f2,2)delta_misure_f2,round(delta_misure_f3,2)delta_misure_f3,
         round(delta_misure_f4,2)delta_misure_f4,round(delta_misure_f5,2)delta_misure_f5,round(delta_misure_f6,2)delta_misure_f6,
         pod,tipo_flusso,data_lettura,competenza_consumi,'0' da_antswitch
         from
         (
         SELECT cf_piva,n_id_fornitura,tipo_misura,0 lettura_misura_monoraria,
         tbl_mm1.delta_misura_monoraria,tbl_mm1.delta_misura_monoraria_2,
         lettura_giornaliero_f1 lettura_misura_f1,lettura_giornaliero_f2 lettura_misura_f2,
         lettura_giornaliero_f3 lettura_misura_f3,lettura_giornaliero_f4 lettura_misura_f4,
         lettura_giornaliero_f5 lettura_misura_f5,lettura_giornaliero_f6 lettura_misura_f6,
         tbl_mm1.delta_misure_f1,tbl_mm1.delta_misure_f2,tbl_mm1.delta_misure_f3,
         tbl_mm1.delta_misure_f4,tbl_mm1.delta_misure_f5,tbl_mm1.delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,data_lettura
         FROM
           (select *,concat(n_id_fornitura,pod,competenza_consumi)KK_JOIN
            from misure.misure_orarie_c where competenza_consumi =${annomese} DISTRIBUTE  BY KK_JOIN
            ) m_orari_c
         INNER JOIN ${tbl_tmm} tbl_mm1 ON CONCAT(m_orari_c.KK_JOIN,data_lettura) = CONCAT(tbl_mm1.KK_JOIN,data_lettura_max)
        )AS TBL
      """.stripMargin

    log.info(s"Calcolo Delta misure orarie mensili con scrittura su tabella misure.misure_mensili_c")

    val dtmisure_o_mensili= hiveCtx.sql(strorarie_mensili)//.persist(StorageLevels.MEMORY_ONLY_SER)

    hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_mm1")

    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_mensili_c")
    hiveCtx.refreshTable(s"misure.misure_mensili_c")


  }
 //DA SISTEMARE LE ISTRUZIONI CON LA CLAUSOLA IN
  def calcMisureAnteSwitching(): Unit ={

    log.info("AVVIO ELABORAZIONE MISURE ANTE SWITCHING")
    /*
    INSERIMENTO MISURE MANCANTI NEL MESE DI INIZIO FORNITURA
    lA SCRITTURA AVVIENE SULLA TABELLA DELLE MISURE NON ORARIE POICHE
    LA PROCEDURA DI SCRITTURA SU MONHODB LI RIPORTA ANCHE NELLE MISURE MENSILI
    */

    hiveCtx.sql("DROP TABLE IF EXISTS forniture_ee_iniziali_senza_misure")

    hiveCtx.sql(
      s"""
        CREATE TABLE forniture_ee_iniziali_senza_misure STORED AS PARQUET as
        SELECT KK_KEY , t_cf,codice_pod,codice_fornitura,annomese_iniziof from
        (
          select CONCAT(FF.cf_piva,FF.codice_pod,FF.n_id_fornitura) KK_KEY  , cf_piva t_cf,codice_pod,n_id_fornitura codice_fornitura,
          cast(substr(cast(FF.inizio as string),1,6) as int)annomese_iniziof
          from tbl_forniture FF
          where inizio <> 0
        ) FO
        left outer join misure.misure_non_orarie_c M_NORA
        ON CONCAT(FO.KK_KEY,FO.annomese_iniziof)=CONCAT(M_NORA.cf_piva,M_NORA.pod,M_NORA.n_id_fornitura,M_NORA.competenza_consumi)
        LEFT OUTER JOIN misure.misure_mensili_c M_ORA
        ON CONCAT(FO.KK_KEY,FO.annomese_iniziof)=CONCAT(M_ORA.cf_piva,M_ORA.pod,M_ORA.n_id_fornitura,M_ORA.competenza_consumi)
        WHERE M_NORA.competenza_consumi IS NULL AND  M_ORA.competenza_consumi IS NULL
      """.stripMargin)

    hiveCtx.sql(
      s"""
      INSERT INTO misure.misure_non_orarie_c
      select t_cf cf_piva  ,codice_fornitura n_id_fornitura ,annomese_iniziof competenza_consumi ,
      codice_pod pod ,'' tipo_misura ,
      cast(null as double) lettura_misura_monoraria ,cast(null as double) lettura_misura_f1 ,cast(null as double) lettura_misura_f2 ,
      cast(null as double) lettura_misura_f3 ,cast(null as double) lettura_misura_f4 ,cast(null as double) lettura_misura_f5 ,
      cast(null as double) lettura_misura_f6 ,cast(null as double) delta_misure_f1 ,cast(null as double) delta_misure_f2 ,
      cast(null as double) delta_misure_f3 ,cast(null as double) delta_misure_f4 ,cast(null as double) delta_misure_f5 ,
      cast(null as double) delta_misure_f6 ,cast(null as double) delta_misura_monoraria ,'' tipo_flusso2 ,cast(null as bigint) data_lettura,
      cast(null as double) potf1,cast(null as double) potf2,cast(null as double) potf3,cast(null as double) potm
      from forniture_ee_iniziali_senza_misure
      """.stripMargin)

    hiveCtx.sql("DROP TABLE IF EXISTS forniture_ee_iniziali_senza_misure")

    //misure ante switching

    hiveCtx.sql("DROP TABLE IF EXISTS forniture_switching")
    hiveCtx.sql(
      s"""
    CREATE TABLE forniture_switching stored as parquet as
    select * from (
    select CONCAT(FF.cf_piva,FF.codice_pod,FF.n_id_fornitura) KK_KEY  , FF.fine data_fine_fornitura_num,
    cast(substr(cast(FF.fine as string),1,6) as int)annomese_finef ,
    max(cast(substr(cast(FF.fine as string),1,6) as int))  over ( partition by FF.cf_piva,FF.codice_pod) max_annomese_fornitura,
    count(CONCAT(FF_2.cf_piva,FF_2.codice_pod)) over ( partition by FF.cf_piva,FF.codice_pod) count_forniture
    from tbl_forniture FF
    ) TBL
    WHERE count_forniture > 1
    """)

    hiveCtx.sql(
      s"""
      INSERT INTO misure.misure_non_orarie_c
      select cf_piva  ,n_id_fornitura ,annomese_finef competenza_consumi ,
      pod ,tipo_misura ,
      cast(null as double) lettura_misura_monoraria ,cast(null as double) lettura_misura_f1 ,cast(null as double) lettura_misura_f2 ,
      cast(null as double) lettura_misura_f3 ,cast(null as double) lettura_misura_f4 ,cast(null as double) lettura_misura_f5 ,
      cast(null as double) lettura_misura_f6 ,cast(null as double) delta_misure_f1 ,cast(null as double) delta_misure_f2 ,
      cast(null as double) delta_misure_f3 ,cast(null as double) delta_misure_f4 ,cast(null as double) delta_misure_f5 ,
      cast(null as double) delta_misure_f6 ,cast(null as double) delta_misura_monoraria ,CONCAT('SW_',tipo_flusso2)tipo_flusso2 ,data_fine_fornitura_num data_lettura,
      cast(null as double) potf1,cast(null as double) potf2,cast(null as double) potf3,cast(null as double) potm
      from
      (
        select *
        from
        (
         SELECT cf_piva ,n_id_fornitura,competenza_consumi,pod ,tipo_misura,tipo_flusso2, CONCAT(cf_piva,pod,n_id_fornitura)KK_MS,MAX(competenza_consumi) over ( partition by cf_piva,pod,n_id_fornitura) max_misura_fornitura from misure.misure_non_orarie_c where data_lettura is not null
        ) MS inner join forniture_switching  forniture ON KK_MS = KK_KEY
        WHERE max_misura_fornitura <> annomese_finef
        and annomese_finef <> max_annomese_fornitura
       )TF
       where max_misura_fornitura = competenza_consumi and CONCAT(TF.cf_piva,TF.pod,TF.n_id_fornitura,TF.annomese_finef) not in
       (
        SELECT CONCAT(misure_mensili_c.cf_piva,misure_mensili_c.pod,misure_mensili_c.n_id_fornitura,misure_mensili_c.competenza_consumi)xx
        FROM misure.misure_mensili_c
        )
       """)

    hiveCtx.sql(
      s"""
      INSERT INTO misure.misure_mensili_c
      SELECT cf_piva  ,n_id_fornitura ,tipo_misura ,
      cast(null as double) lettura_misura_monoraria ,cast(null as double) delta_misura_monoraria ,
      cast(null as double) lettura_misura_f1 ,cast(null as double) lettura_misura_f2 ,
      cast(null as double) lettura_misura_f3 ,cast(null as double) lettura_misura_f4 ,cast(null as double) lettura_misura_f5 ,
      cast(null as double) lettura_misura_f6 ,cast(null as double) delta_misure_f1 ,cast(null as double) delta_misure_f2 ,
      cast(null as double) delta_misure_f3 ,cast(null as double) delta_misure_f4 ,cast(null as double) delta_misure_f5 ,
      cast(null as double) delta_misure_f6 ,pod ,
      annomese_finef competenza_consumi ,CONCAT('SW_',tipo_flusso)tipo_flusso ,data_fine_fornitura_num data_lettura
      from
      (
       select * from
       (
        SELECT cf_piva ,n_id_fornitura,competenza_consumi,pod ,tipo_misura,tipo_flusso, CONCAT(cf_piva,pod,n_id_fornitura)KK_MS,MAX(competenza_consumi) over ( partition by cf_piva,pod,n_id_fornitura) max_misura_fornitura from misure.misure_mensili_c
       ) MS inner join forniture_switching  forniture ON KK_MS = KK_KEY
       WHERE max_misura_fornitura <> annomese_finef and annomese_finef <> max_annomese_fornitura
      )TF where max_misura_fornitura = competenza_consumi
      and CONCAT(TF.cf_piva,TF.pod,TF.n_id_fornitura,TF.annomese_finef) not in
      (
       SELECT CONCAT(misure_non_orarie_c.cf_piva,misure_non_orarie_c.pod,misure_non_orarie_c.n_id_fornitura,misure_non_orarie_c.competenza_consumi)xx
       FROM misure.misure_non_orarie_c
       )""")

    hiveCtx.sql("DROP TABLE IF EXISTS forniture_switching")


    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_non_orarie_c")
    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_mensili_c")

    cleanMemory()

    log.info("ELABORAZIONE MISURE ANTE SWITCHING COMPLETATA")
  }
  def calMisureOrarieGG_Mese_Delta(d_max:String,d_min:String): Unit ={


    hiveCtx.sql("set hive.exec.dynamic.partition=true")
    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveCtx.sql("set hive.mapred.mode = nonstrict")
    hiveCtx.sql("set hive.exec.parallel=true")

    // PULIZIA TABELLA misure_orarie_c
    //hiveCtx.sql(s"TRUNCATE TABLE misure.misure_orarie_c")
    val q_drop_partsc: String = s"ALTER TABLE misure.misure_orarie_c DROP IF EXISTS PARTITION(competenza_consumi<>200000)"
    hiveCtx.sql(q_drop_partsc)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_c")
    hiveCtx.refreshTable(s"misure.misure_orarie_c")


    // PULIZIA TABELLA misure_mensili_c
    //hiveCtx.sql(s"TRUNCATE TABLE misure.misure_mensili_c")
    val q_drop_partsm: String = s"ALTER TABLE misure.misure_mensili_c DROP IF EXISTS PARTITION(competenza_consumi<>200000)"
    hiveCtx.sql(q_drop_partsm)

    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_mensili_c")
    hiveCtx.refreshTable(s"misure.misure_mensili_c")

    log.info(s"Avvio calcolo delta consumi giornalieri , fasce + incrocio con forniture per il periodo ${d_min} - ${d_max}")

    hiveCtx.sql(s"DROP TABLE IF EXISTS tbl_orarie_tmp")
    val tmp=s"""
            CREATE TABLE tbl_orarie_tmp STORED AS PARQUET AS
            SELECT cf_piva,n_id_fornitura,podquarti,giornoquarti,annomese,annomesegiorno,
             consumo,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,perdita,potmax,is2g,
             CAST(prev_dati_arr[0] AS DOUBLE)prev_consumo,
             CAST(prev_dati_arr[1] AS DOUBLE)prev_eaf1,
             CAST(prev_dati_arr[2] AS DOUBLE)prev_eaf2,
             CAST(prev_dati_arr[3] AS DOUBLE)prev_eaf3,
             CAST(prev_dati_arr[4] AS DOUBLE)prev_eaf4,
             CAST(prev_dati_arr[5] AS DOUBLE)prev_eaf5,
             CAST(prev_dati_arr[6] AS DOUBLE)prev_eaf6,tipo_flusso,da_no_ora,
             SUBSTR(podquarti,7,2) cod_pod
            FROM
            (
             SELECT cf_piva,n_id_fornitura,podquarti,giornoquarti,annomese,annomesegiorno,
             consumo,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,perdita,potmax,is2g,
             split(prev_dati,'\b')prev_dati_arr,tipo_flusso,da_no_ora
             FROM
             (
              SELECT M.cf_piva,M.n_id_fornitura,M.podquarti,M.giornoquarti,M.annomese,M.annomesegiorno,
              M.consumo,M.eaf1  , M.eaf2  , M.eaf3  , M.eaf4  , M.eaf5  , M.eaf6 ,M.perdita,M.potmax,M.is2g,
              LAG(CONCAT(NVL(M.consumo,'NULL'),'\b',NVL(M.eaf1,'NULL'),'\b',NVL(M.eaf2,'NULL'),'\b',NVL(M.eaf3,'NULL'),'\b',NVL(M.eaf4,'NULL'),'\b',NVL(M.eaf5,'NULL'),'\b',NVL(M.eaf6,'NULL')))
              over (partition by CONCAT(M.cf_piva,M.podquarti) order by M.annomesegiorno) prev_dati,
              tipo_flusso,da_no_ora
              FROM (
               SELECT cf_piva,n_id_fornitura,MO.podquarti,MO.giornoquarti,MO.annomese,MO.annomesegiorno,
                MO.consumo,MO.eaf1  , MO.eaf2  , MO.eaf3  , MO.eaf4  , MO.eaf5  , MO.eaf6 ,MO.perdita,MO.potmax,
                MO.Is2G is2g,MO.tipo_flusso,MO.da_no_ora
                FROM (SELECT * FROM  misure.misure_orarie_base DISTRIBUTE BY podquarti ) MO
                INNER JOIN tbl_forniture F ON MO.podquarti = F.codice_pod
                WHERE (MO.annomesegiorno >= F.inizio AND MO.annomesegiorno <= F.fine )
               )M
              ) TBL_SPLITTED
             )TBL_F
         """.stripMargin

    val dtx =hiveCtx.sql(tmp)//.persist(StorageLevels.MEMORY_AND_DISK_SER)
    //dtx.cache()
    //dtx.registerTempTable("tbl_orarie_tmp")


    val str_misure_orarie_delta=
      s"""
          INSERT INTO misure.misure_orarie_c PARTITION(competenza_consumi)
          SELECT cf_piva,n_id_fornitura, pod, giorno,tipo_misura,potenza_max_erogata,lettura_giornaliero_f1,
          lettura_giornaliero_f2,lettura_giornaliero_f3,lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,
          delta_misure_f1,delta_misure_f2,delta_misure_f3,delta_misure_f4,delta_misure_f5,delta_misure_f6,consumo_giornaliero_gg,
          is2g,tipo_flusso,data_lettura,competenza_consumi
          FROM
          (
           SELECT cf_piva,n_id_fornitura,podquarti pod,giornoquarti giorno,'E' tipo_misura,
           round(potmax,2) potenza_max_erogata,round(eaf1,2) lettura_giornaliero_f1 ,round(eaf2,2) lettura_giornaliero_f2,
           round(eaf3,2) lettura_giornaliero_f3,round(eaf4,2) lettura_giornaliero_f4 ,round(eaf5,2) lettura_giornaliero_f5,
           round(eaf6,2) lettura_giornaliero_f6,
		       case when prev_eaf1 > eaf1 then 0 else round((eaf1-prev_eaf1),2) end delta_misure_f1,
		       case when prev_eaf2 > eaf2 then 0 else round((eaf2-prev_eaf2),2) end delta_misure_f2,
		       case when prev_eaf3 > eaf3 then 0 else round((eaf3-prev_eaf3),2) end delta_misure_f3,
		       case when prev_eaf4 > eaf4 then 0 else round((eaf4-prev_eaf4),2) end delta_misure_f4,
		       case when prev_eaf5 > eaf5 then 0 else round((eaf5-prev_eaf5),2) end delta_misure_f5,
		       case when prev_eaf6 > eaf6 then 0 else round((eaf6-prev_eaf6),2) end delta_misure_f6,
           case when round(consumo,2) < 0 or cast(nvl(eaf1,0) as int) <> 0 then 0 else round(consumo,2) end consumo_giornaliero_gg ,is2g,
           annomese competenza_consumi,tipo_flusso,annomesegiorno data_lettura,da_no_ora
           FROM tbl_orarie_tmp AS TBL DISTRIBUTE BY cod_pod
           ) AS TTX WHERE da_no_ora ='0'
      """.stripMargin


    val dtmisure_o_delta= hiveCtx.sql(str_misure_orarie_delta)
    log.info(s"Scrittura misure orarie  effettuata su tabella  misure.misure_orarie_c ")
    //dtx.unpersist(true)
    //hiveCtx.dropTempTable("tbl_orarie_tmp")
    hiveCtx.sql(s"DROP TABLE IF EXISTS tbl_orarie_tmp")



    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_c")
    hiveCtx.refreshTable(s"misure.misure_orarie_c")


    log.info(s"Estrazione misure orarie mensili  per il periodo ${d_min} - ${d_max}" )


    /*
    DA VERIFICARE LA MISURA MONORARIA CON DELTA PER LE MISURE ORARIE

     sum(consumo_giornaliero_gg) over(partition by n_id_fornitura,competenza_consumi)lettura_misura_monoraria,

    */



    val tbl_tmm= "misure.tbl_mm1"
    hiveCtx.sql(s"DROP TABLE IF EXISTS ${tbl_tmm}")

    val strorarie_mensili1=
      s"""
         CREATE TABLE ${tbl_tmm} STORED AS PARQUET AS
         select sum(CASE WHEN Is2G='0' THEN consumo_giornaliero_gg ELSE 0 END) delta_misura_monoraria,
         sum(consumo_giornaliero_gg)delta_misura_monoraria_2,
         sum(delta_misure_f1)delta_misure_f1,sum(delta_misure_f2)delta_misure_f2,sum(delta_misure_f3)delta_misure_f3,
         sum(delta_misure_f4)delta_misure_f4,sum(delta_misure_f5)delta_misure_f5,sum(delta_misure_f6)delta_misure_f6,
         max(data_lettura)data_lettura_max,
         concat(n_id_fornitura,pod,competenza_consumi)KK_JOIN
         from misure.misure_orarie_c where n_id_fornitura <> ''
         group by n_id_fornitura,pod,competenza_consumi
         DISTRIBUTE  BY KK_JOIN
      """.stripMargin

    hiveCtx.sql(strorarie_mensili1)
    //val dtmm1=hiveCtx.sql(strorarie_mensili1)//.persist(StorageLevels.MEMORY_ONLY_SER)
    //dtmm1.registerTempTable("tbl_mm1")

    /*val strorarie_mensili =
      s"""
         INSERT INTO misure.misure_mensili_c
         select distinct cf_piva,n_id_fornitura,tipo_misura,lettura_misura_monoraria,
         CASE WHEN (NVL(lettura_misura_f1,0.0)+NVL(lettura_misura_f2,0.0)+NVL(lettura_misura_f3,0.0)+NVL(lettura_misura_f4,0.0)+NVL(lettura_misura_f5,0.0)+NVL(lettura_misura_f6,0.0))=0.0 THEN
         round(delta_misura_monoraria_2,2) ELSE round(delta_misura_monoraria,2) END delta_misura_monoraria,
         lettura_misura_f1,lettura_misura_f2,lettura_misura_f3,lettura_misura_f4,lettura_misura_f5,lettura_misura_f6,
         round(delta_misure_f1,2)delta_misure_f1,round(delta_misure_f2,2)delta_misure_f2,round(delta_misure_f3,2)delta_misure_f3,
         round(delta_misure_f4,2)delta_misure_f4,round(delta_misure_f5,2)delta_misure_f5,round(delta_misure_f6,2)delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,data_lettura
         from
         (
         SELECT cf_piva,n_id_fornitura,tipo_misura,0 lettura_misura_monoraria,
         tbl_mm1.delta_misura_monoraria,tbl_mm1.delta_misura_monoraria_2,
         lettura_giornaliero_f1,lettura_giornaliero_f2,lettura_giornaliero_f3,
         lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,
         CAST(split(letture_misure_f,'\b')[0] AS DOUBLE)lettura_misura_f1,
         CAST(split(letture_misure_f,'\b')[1] AS DOUBLE)lettura_misura_f2,
         CAST(split(letture_misure_f,'\b')[2] AS DOUBLE)lettura_misura_f3,
         CAST(split(letture_misure_f,'\b')[3] AS DOUBLE)lettura_misura_f4,
         CAST(split(letture_misure_f,'\b')[4] AS DOUBLE)lettura_misura_f5,
         CAST(split(letture_misure_f,'\b')[5] AS DOUBLE)lettura_misura_f6,
         tbl_mm1.delta_misure_f1,tbl_mm1.delta_misure_f2,tbl_mm1.delta_misure_f3,
         tbl_mm1.delta_misure_f4,tbl_mm1.delta_misure_f5,tbl_mm1.delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,
         CAST(split(letture_misure_f,'\b')[0] AS BIGINT)data_lettura
         FROM
           (select *,concat(n_id_fornitura,pod,competenza_consumi)KK_JOIN,
            last_value(CONCAT(NVL(lettura_giornaliero_f1,'NULL'),'\b',NVL(lettura_giornaliero_f2,'NULL'),'\b',NVL(lettura_giornaliero_f3,'NULL'),'\b',NVL(lettura_giornaliero_f4,'NULL'),'\b',NVL(lettura_giornaliero_f5,'NULL'),'\b',NVL(lettura_giornaliero_f6,'NULL'),'\b',NVL(data_lettura,'NULL')))
            over(partition by concat(n_id_fornitura,pod,competenza_consumi) order by CAST(CONCAT(competenza_consumi,LPAD(giorno,2,0)) AS BIGINT) rows between unbounded preceding and unbounded following) letture_misure_f
            from misure.misure_orarie_c where n_id_fornitura <> '' DISTRIBUTE  BY KK_JOIN
            ) m_orari_c
         INNER JOIN ${tbl_tmm} tbl_mm1 ON m_orari_c.KK_JOIN = tbl_mm1.KK_JOIN
        )AS TBL
      """.stripMargin*/

    val strorarie_mensili =
      s"""
         INSERT INTO misure.misure_mensili_c PARTITION(competenza_consumi,da_antswitch)
         select distinct cf_piva,n_id_fornitura,tipo_misura,lettura_misura_monoraria,
         CASE WHEN (NVL(lettura_misura_f1,0.0)+NVL(lettura_misura_f2,0.0)+NVL(lettura_misura_f3,0.0)+NVL(lettura_misura_f4,0.0)+NVL(lettura_misura_f5,0.0)+NVL(lettura_misura_f6,0.0))=0.0 THEN
         round(delta_misura_monoraria_2,2) ELSE round(delta_misura_monoraria,2) END delta_misura_monoraria,
         lettura_misura_f1,lettura_misura_f2,lettura_misura_f3,lettura_misura_f4,lettura_misura_f5,lettura_misura_f6,
         round(delta_misure_f1,2)delta_misure_f1,round(delta_misure_f2,2)delta_misure_f2,round(delta_misure_f3,2)delta_misure_f3,
         round(delta_misure_f4,2)delta_misure_f4,round(delta_misure_f5,2)delta_misure_f5,round(delta_misure_f6,2)delta_misure_f6,
         pod,tipo_flusso,data_lettura,competenza_consumi,'0' da_antswitch
         from
         (
         SELECT cf_piva,n_id_fornitura,tipo_misura,0 lettura_misura_monoraria,
         tbl_mm1.delta_misura_monoraria,tbl_mm1.delta_misura_monoraria_2,
         lettura_giornaliero_f1 lettura_misura_f1,lettura_giornaliero_f2 lettura_misura_f2,
         lettura_giornaliero_f3 lettura_misura_f3,lettura_giornaliero_f4 lettura_misura_f4,
         lettura_giornaliero_f5 lettura_misura_f5,lettura_giornaliero_f6 lettura_misura_f6,
         tbl_mm1.delta_misure_f1,tbl_mm1.delta_misure_f2,tbl_mm1.delta_misure_f3,
         tbl_mm1.delta_misure_f4,tbl_mm1.delta_misure_f5,tbl_mm1.delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,data_lettura
         FROM
           (select *,concat(n_id_fornitura,pod,competenza_consumi)KK_JOIN
            from misure.misure_orarie_c  DISTRIBUTE  BY KK_JOIN
            ) m_orari_c
         INNER JOIN ${tbl_tmm} tbl_mm1 ON CONCAT(m_orari_c.KK_JOIN,data_lettura) = CONCAT(tbl_mm1.KK_JOIN,data_lettura_max)
        )AS TBL
      """.stripMargin

    log.info(s"Calcolo Delta misure orarie mensili con scrittura su tabella misure.misure_mensili_c")

    val dtmisure_o_mensili= hiveCtx.sql(strorarie_mensili)//.persist(StorageLevels.MEMORY_ONLY_SER)

    //dtmm1.unpersist(true)
    //hiveCtx.dropTempTable("tbl_mm1")
    hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_mm1")

    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_mensili_c")
    hiveCtx.refreshTable(s"misure.misure_mensili_c")


    //val q_drop_parts: String = s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese<>200000)"
    //hiveCtx.sql(q_drop_parts)

    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")
  }

  def calMisure_NoOrarieGG_Mese_Delta(d_max:String,d_min:String): Unit =
  {

    // PULIZIA TABELLA misure_mensili_c
    hiveCtx.sql(s"TRUNCATE TABLE misure.misure_non_orarie_c")
    val q_drop_parts: String = s"ALTER TABLE misure.misure_non_orarie_c DROP IF EXISTS PARTITION(da_antswitch<>'2')"
    hiveCtx.sql(q_drop_parts)

    hiveCtx.sql("TRUNCATE TABLE misure.misure_non_orarie_base_volture")
    hiveCtx.sql("TRUNCATE TABLE misure.misure_non_orarie_base_volture_c")


    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_non_orarie_c")
    hiveCtx.refreshTable(s"misure.misure_non_orarie_c")
    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_non_orarie_base_volture")
    hiveCtx.refreshTable(s"misure.misure_non_orarie_base_volture")
    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_non_orarie_base_volture_c")
    hiveCtx.refreshTable(s"misure.misure_non_orarie_base_volture_c")

    log.info(s"Avvio calcolo delta consumi mensili per misure non orarie nel periodo ${d_min} - ${d_max}")

    hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_misure_volture")

    val strmisure_con_volture=
      """
         CREATE TABLE misure.tbl_misure_volture STORED AS PARQUET AS
        SELECT NO_ORA.pod ,NO_ORA.giorno ,NO_ORA.annomese,
         NO_ORA.lettura_monoraria ,NO_ORA.eaf1  , NO_ORA.eaf2  , NO_ORA.eaf3  , NO_ORA.eaf4  , NO_ORA.eaf5  , NO_ORA.eaf6,
         NO_ORA.tipo_dato ,NO_ORA.tipo_flusso,NO_ORA.tipo_flusso2,
         NO_ORA.potf1,NO_ORA.potf2,NO_ORA.potf3,NO_ORA.potm
         FROM  (SELECT *,CONCAT(pod,annomese)pod_periodo FROM misure.misure_non_orarie_base DISTRIBUTE BY pod_periodo)NO_ORA
         INNER JOIN (
          select DISTINCT CONCAT(pod,annomese)pod_periodo
         from misure.misure_non_orarie_base where tipo_flusso='VNO_RNV' DISTRIBUTE BY pod_periodo
         ) AS POD_VOLTURE ON NO_ORA.pod_periodo = POD_VOLTURE.pod_periodo
      """.stripMargin


    log.info("Estrazione misure non orarie per i mesi in cui sono presenti volture")

    val dt_misure_volture= hiveCtx.sql(strmisure_con_volture)//.persist(StorageLevels.MEMORY_ONLY_SER)



    //ESTRAGGO TUTTE LE VOLTURE RICAVANDO LA FORNITURA REPLICANDO LE MISURE SOTTRAENDO UN GIORNO ALLA DATA DI VOLTURA
    // IN MODO DA OTTENERE L'INIZIO VOLTURA/FORNITURA E LA FINE VOLTURA/FORNITURA
    val strvno_rnv=
    s"""
        INSERT INTO misure.misure_non_orarie_base_volture
        SELECT cf_piva,n_id_fornitura ,pod ,giorno ,annomese,
        lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,inizio_fine,
        potf1,potf2,potf3,potm
        FROM
        (
         SELECT pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,'inizio_voltura' inizio_fine,
         potf1,potf2,potf3,potm
         FROM misure.tbl_misure_volture M_NO WHERE tipo_flusso ='VNO_RNV' DISTRIBUTE BY pod
         UNION ALL
         SELECT pod ,(giorno-1) giorno,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,'fine_voltura' inizio_fine,
         potf1,potf2,potf3,potm
         FROM misure.tbl_misure_volture M_NO WHERE tipo_flusso ='VNO_RNV' DISTRIBUTE BY pod
         UNION ALL
         SELECT pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,'pno_rno' inizio_fine,
         potf1,potf2,potf3,potm
         FROM misure.tbl_misure_volture M_NO WHERE tipo_flusso <> 'VNO_RNV' DISTRIBUTE BY pod
         ) M_NO
        INNER JOIN tbl_forniture F ON M_NO.pod = F.codice_pod
        WHERE CAST(concat(annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) >= F.inizio AND CAST(concat(annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) <= F.fine
      """.stripMargin

    log.info("Scrittura misure non orarie con relative forniture  per i mesi in cui sono presenti volture nella tabella misure.misure_non_orarie_base_volture")
    hiveCtx.sql(strvno_rnv)



    val calcdelta_mesi_volture=
      s"""
          INSERT INTO misure.misure_non_orarie_base_volture_c
          SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
          lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
          (lettura_monoraria-prev_lettura_monoraria)delta_monoraria,
          (eaf1-prev_eaf1)delta_eaf1,
          (eaf2-prev_eaf2)delta_eaf2,
          (eaf3-prev_eaf3)delta_eaf3,
          (eaf4-prev_eaf4)delta_eaf4,
          (eaf5-prev_eaf5)delta_eaf5,
          (eaf6-prev_eaf6)delta_eaf6, tipo_dato ,tipo_flusso,tipo_flusso2,inizio_fine,
          potf1,potf2,potf3,potm
          from(
          SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
          tipo_dato ,tipo_flusso,tipo_flusso2,
          lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6 ,
          potf1,potf2,potf3,potm,
          LAG(M.lettura_monoraria)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) order by M.giorno) prev_lettura_monoraria,
          LAG(M.eaf1)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) order by M.giorno) prev_eaf1,
          LAG(M.eaf2)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) order by M.giorno) prev_eaf2,
          LAG(M.eaf3)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) order by M.giorno) prev_eaf3,
          LAG(M.eaf4)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) order by M.giorno) prev_eaf4,
          LAG(M.eaf5)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) order by M.giorno) prev_eaf5,
          LAG(M.eaf6)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) order by M.giorno) prev_eaf6,inizio_fine,
          max(giorno)over (partition by CONCAT(M.cf_piva,M.pod,M.annomese) ) max_giorno
          FROM misure.misure_non_orarie_base_volture M
         ) as tbl where inizio_fine ='fine_voltura' or giorno = max_giorno
      """.stripMargin


    log.info("Scrittura misure non orarie con relative forniture e delta per i mesi in cui sono presenti volture nella tabella misure.misure_non_orarie_base_volture_c")
    hiveCtx.sql(calcdelta_mesi_volture)


    //ESTRAGGO TUTTE LE MISURE NON ORARIE RICAVANDO LA FORNITURA PER I PNO/RNO
    hiveCtx.sql("DROP TABLE IF EXISTS misure.tlb_no_orarie")

    val strno_orarie=
      s"""
        CREATE TABLE misure.tlb_no_orarie STORED AS PARQUET AS
        SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
         lettura_monoraria, eaf1 , eaf2  , eaf3  , eaf4  , eaf5 , eaf6,
         case when round(delta_monoraria,2) <0 then 0 else round(delta_monoraria,2) end delta_monoraria,
         case when round(delta_eaf1,2)  <0 then 0 else round(delta_eaf1,2) end delta_eaf1,
         case when round(delta_eaf2,2)  <0 then 0 else round(delta_eaf2,2) end delta_eaf2,
         case when round(delta_eaf3,2)  <0 then 0 else round(delta_eaf3,2) end delta_eaf3,
         case when round(delta_eaf4,2)  <0 then 0 else round(delta_eaf4,2) end delta_eaf4,
         case when round(delta_eaf5,2)  <0 then 0 else round(delta_eaf5,2) end delta_eaf5,
         case when round(delta_eaf6,2)  <0 then 0 else round(delta_eaf6,2) end delta_eaf6,
         tipo_dato ,tipo_flusso,tipo_flusso2,is_from_voltura,
         potf1,potf2,potf3,potm
        FROM
        (
        SELECT F.cf_piva,F.n_id_fornitura ,M_NO.pod ,M_NO.giorno ,M_NO.annomese,
        M_NO.lettura_monoraria ,M_NO.eaf1  , M_NO.eaf2  , M_NO.eaf3  , M_NO.eaf4  , M_NO.eaf5  , M_NO.eaf6,
        0.0 delta_monoraria,0.0 delta_eaf1,0.0 delta_eaf2,0.0 delta_eaf3,0.0 delta_eaf4,0.0 delta_eaf5,0.0 delta_eaf6,
        M_NO.tipo_dato ,M_NO.tipo_flusso,M_NO.tipo_flusso2,'0' is_from_voltura,
        M_NO.potf1,M_NO.potf2,M_NO.potf3,M_NO.potm
        FROM (SELECT * FROM misure.misure_non_orarie_base DISTRIBUTE BY pod) M_NO
        LEFT OUTER JOIN misure.misure_non_orarie_base_volture_c M_VOL ON CONCAT(M_NO.pod,M_NO.annomese,M_NO.giorno)=CONCAT(M_VOL.pod,M_VOL.annomese,M_VOL.giorno)
        INNER JOIN tbl_forniture F ON M_NO.pod = F.codice_pod
        WHERE M_VOL.pod IS NULL AND M_NO.tipo_flusso ='PNO_RNO' AND
         (CAST(concat(M_NO.annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) >= F.inizio AND CAST(concat(M_NO.annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) <= F.fine)
        UNION ALL
        SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,
         tipo_dato ,tipo_flusso,tipo_flusso2,
         case when inizio_fine ='pno_rno' then '1' when inizio_fine ='fine_voltura' and delta_monoraria is null and delta_eaf1 is null then '0' else '1' end is_from_voltura,
         potf1,potf2,potf3,potm
         FROM misure.misure_non_orarie_base_volture_c
         ) TT ORDER BY pod ,n_id_fornitura,annomese,giorno
      """.stripMargin

    log.info("Estrazione misure non orarie PNO/RNO insieme alle volture precalcolate")
    //val dt_no_orarie=hiveCtx.sql(strno_orarie)//.persist(StorageLevels.MEMORY_ONLY_SER)
    //dt_no_orarie.registerTempTable("tlb_no_orarie")
    hiveCtx.sql(strno_orarie)

    val tmp=s"""
         SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
         lettura_monoraria ,eaf1 , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         CAST(prev_data_arr[0] AS DOUBLE) prev_lettura_monoraria,
         CAST(prev_data_arr[1] AS DOUBLE) prev_eaf1,
         CAST(prev_data_arr[2] AS DOUBLE) prev_eaf2,
         CAST(prev_data_arr[3] AS DOUBLE) prev_eaf3,
         CAST(prev_data_arr[4] AS DOUBLE) prev_eaf4,
         CAST(prev_data_arr[5] AS DOUBLE) prev_eaf5,
         CAST(prev_data_arr[6] AS DOUBLE) prev_eaf6,
         delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,tipo_dato,tipo_flusso,tipo_flusso2,is_from_voltura,
         potf1,potf2,potf3,potm
         FROM
         (
          SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
          lettura_monoraria ,eaf1 , eaf2  , eaf3  ,eaf4  , eaf5 ,eaf6 ,
          split(prev_data,'\b')prev_data_arr,
          delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,tipo_dato,tipo_flusso,tipo_flusso2,is_from_voltura,
          potf1,potf2,potf3,potm
          FROM
          (
           SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
           lettura_monoraria ,eaf1 , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
           LAG(CONCAT(NVL(M.lettura_monoraria,'NULL'),'\b',NVL(M.eaf1,'NULL'),'\b' ,NVL(M.eaf2,'NULL'),'\b',NVL(M.eaf3,'NULL'),'\b',NVL(M.eaf4,'NULL'),'\b',NVL(M.eaf5,'NULL'),'\b',NVL(M.eaf6,'NULL')))
           over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) as prev_data,
           delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,tipo_dato,tipo_flusso,tipo_flusso2,is_from_voltura,
           potf1,potf2,potf3,potm
           FROM misure.tlb_no_orarie M
          ) AS TBL
         )AS XX
      """.stripMargin

    val dtx=hiveCtx.sql(tmp)//.persist(StorageLevels.MEMORY_ONLY_SER)
    dtx.cache()
    dtx.registerTempTable("tbl_tmp_mis_no_delta")


    hiveCtx.sql("set hive.exec.dynamic.partition=true")
    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveCtx.sql("set hive.mapred.mode = nonstrict")
    hiveCtx.sql("set hive.exec.parallel=true")

    val str_no_orarie_delta=
      s"""
         INSERT INTO misure.misure_non_orarie_c PARTITION(da_antswitch)
         SELECT cf_piva,n_id_fornitura,competenza_consumi,pod,tipo_misura,lettura_misura_monoraria ,
         lettura_misura_f1,lettura_misura_f2,lettura_misura_f3,
         lettura_misura_f4,lettura_misura_f5,lettura_misura_f6,
         delta_misure_f1,delta_misure_f2,delta_misure_f3,
         delta_misure_f4,delta_misure_f5,delta_misure_f6,delta_misura_monoraria,
         tipo_flusso2,data_lettura,potf1,potf2,potf3,potm,da_antswitch FROM (
         SELECT  cf_piva,n_id_fornitura,annomese competenza_consumi,pod,tipo_dato tipo_misura ,
         lettura_monoraria lettura_misura_monoraria ,
         eaf1 lettura_misura_f1  , eaf2 lettura_misura_f2 , eaf3  lettura_misura_f3,
         eaf4 lettura_misura_f4 , eaf5 lettura_misura_f5  , eaf6 lettura_misura_f6,
         CASE WHEN is_from_voltura='1' THEN delta_eaf1 ELSE
         (case when prev_eaf1 > eaf1 then 0 else round((eaf1-prev_eaf1),2) end) END delta_misure_f1,
         CASE WHEN is_from_voltura='1' THEN delta_eaf2 ELSE
         (case when prev_eaf2 > eaf2 then 0 else round((eaf2-prev_eaf2),2) end) END delta_misure_f2,
         CASE WHEN is_from_voltura='1' THEN delta_eaf3 ELSE
         (case when prev_eaf3 > eaf3 then 0 else round((eaf3-prev_eaf3),2) end) END delta_misure_f3,
         CASE WHEN is_from_voltura='1' THEN delta_eaf4 ELSE
         (case when prev_eaf4 > eaf4 then 0 else round((eaf4-prev_eaf4),2) end) END delta_misure_f4,
         CASE WHEN is_from_voltura='1' THEN delta_eaf5 ELSE
         (case when prev_eaf5 > eaf5 then 0 else round((eaf5-prev_eaf5),2) end) END delta_misure_f5,
         CASE WHEN is_from_voltura='1' THEN delta_eaf6 ELSE
         (case when prev_eaf6 > eaf6 then 0 else round((eaf6-prev_eaf6),2) end) END delta_misure_f6,
         CASE WHEN is_from_voltura='1' THEN delta_monoraria ELSE
         (case when prev_lettura_monoraria > lettura_monoraria then 0 else round((lettura_monoraria-prev_lettura_monoraria),2) end)  END delta_misura_monoraria,
         tipo_flusso,tipo_flusso2,concat(annomese,lpad(giorno,2,0))data_lettura,
         potf1,potf2,potf3,potm,'0' da_antswitch
         FROM tbl_tmp_mis_no_delta) AS TBL_NO_ORARIE_C WHERE tipo_flusso ='PNO_RNO'
      """.stripMargin


    log.info("Scrittura misure non orarie in tabella misure.misure_non_orarie_c")
    hiveCtx.sql(str_no_orarie_delta)

    // dt_no_orarie.unpersist()
    //hiveCtx.dropTempTable("tlb_no_orarie")
    hiveCtx.sql("DROP TABLE IF EXISTS misure.tlb_no_orarie")
    hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_misure_volture")
    hiveCtx.dropTempTable("tbl_tmp_mis_no_delta")
    dtx.unpersist(true)

    //val q_drop_partsx: String = s"ALTER TABLE misure.misure_non_orarie_base DROP IF EXISTS PARTITION(annomese<>200000)"
    //hiveCtx.sql(q_drop_partsx)
    //hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_non_orarie_base")

    log.info("Scrittura misure non orarie in tabella misure.misure_non_orarie_c completata")
  }

  def calMisure_Autoletture_Volture(d_max:String,d_min:String): Unit ={


    log.info(s"Avvio estrazione autoletture per il periodo : ${d_min} - ${d_max}")

    hiveCtx.sql(s"TRUNCATE TABLE misure.autoletture")
    //il flusso autoletture ancora non esiste

    hiveCtx.sql(s"TRUNCATE TABLE misure.volture")
    hiveCtx.sql(s"""INSERT INTO misure.volture
                select cf_piva,n_id_fornitura ,pod ,
                case tipo_flusso2_num when 5 then 'VNO' when 6 then 'VNO2G' when 7 then 'RNV' when 8 then 'RNV2G' end  tipo_flusso2,
                competenza_consumi,concat(competenza_consumi,lpad(giorno,2,0))data_lettura,
                lettura_misura_monoraria,lettura_misura_f1,lettura_misura_f2,lettura_misura_f3,lettura_misura_f4,lettura_misura_f5,lettura_misura_f6
                from
                (
                  select cf_piva,n_id_fornitura ,pod ,
                  max(tipo_flusso2_num)tipo_flusso2_num,
                  annomese competenza_consumi, MAX(giorno) giorno ,
                  MAX(lettura_monoraria) lettura_misura_monoraria,MAX(eaf1)  lettura_misura_f1, MAX(eaf2)  lettura_misura_f2,
                  MAX(eaf3)  lettura_misura_f3, MAX(eaf4)  lettura_misura_f4, MAX(eaf5)  lettura_misura_f5, MAX(eaf6) lettura_misura_f6
                  FROM (select*,CASE tipo_flusso2 when 'VNO' THEN 5 when 'VNO2G' then 6 when 'RNV' then 7 when 'RNV2G' then 8 END tipo_flusso2_num
                  from misure.misure_non_orarie_base_volture
                  WHERE  inizio_fine ='inizio_voltura' or inizio_fine = 'fine_voltura')misure_non_orarie_base_volture
                  GROUP BY cf_piva,n_id_fornitura ,pod ,annomese,giorno
                ) as volture
      """)

    log.info(s"Scrittura autolettura su tabella misure.volture per il periodo : ${d_min} - ${d_max} eseguita")





  }


  def TrovaValidazioneStatoPod(hvCtx:HiveContext, annomese:String): Unit = {

    val th1 = new Thread {

      // create table stato_pods_1 STORED AS PARQUET as
      override def run {
        val stato_pd1 =
          s"""
    create view stato_pods_1 as
    SELECT n_id_pod,STATO_POD
    FROM
    (
      select n_id_pod,'1' STATO_POD,
      CAST(NVL(CONCAT(year(D_AGGIORNAMENTO),LPAD(month(D_AGGIORNAMENTO),2,0),LPAD(day(D_AGGIORNAMENTO),2,0)),'20991231') AS INT) D_AGGIORNAMENTO ,
      MAX(NVL(CONCAT(year(D_AGGIORNAMENTO),LPAD(month(D_AGGIORNAMENTO),2,0),LPAD(day(D_AGGIORNAMENTO),2,0)),'20991231')) over ( partition by n_id_pod) MAX_D_AGGIORNAMENTO
      from rcu.rcu_pod_stato_p a
      where ${annomese} between CAST(NVL(CONCAT(year(D_ATTIVAZIONE),LPAD(month(D_ATTIVAZIONE),2,0)),'190001') AS INT)
      and CAST(NVL(CONCAT(year(D_DISATTIVAZIONE),LPAD(month(D_DISATTIVAZIONE),2,0)),'209912') AS INT)
    ) AS TBL
      WHERE D_AGGIORNAMENTO = MAX_D_AGGIORNAMENTO
    """


       // hvCtx.sql("DROP TABLE IF EXISTS stato_pods_1")
        hvCtx.sql("DROP VIEW IF EXISTS stato_pods_1")
        hvCtx.sql(stato_pd1)

        // create  table stato_pods STORED AS PARQUET
        val stato_pd =
          s"""
    create  view stato_pods AS
    SELECT n_id_pod,STATO_POD
    FROM
    (
      select r.n_id_pod,'1' STATO_POD,
      CAST(NVL(CONCAT(year(D_AGGIORNAMENTO),LPAD(month(D_AGGIORNAMENTO),2,0),LPAD(day(D_AGGIORNAMENTO),2,0)),'20991231') AS INT) D_AGGIORNAMENTO ,
      MAX(NVL(CONCAT(year(D_AGGIORNAMENTO),LPAD(month(D_AGGIORNAMENTO),2,0),LPAD(day(D_AGGIORNAMENTO),2,0)),'20991231')) over ( partition by r.n_id_pod) MAX_D_AGGIORNAMENTO
      from
      (SELECT * FROM rcus.rcus_podstato_p
       where ${annomese} between CAST(NVL(CONCAT(year(D_ATTIVAZIONE),LPAD(month(D_ATTIVAZIONE),2,0)),'190001') AS INT)
       and CAST(NVL(CONCAT(year(D_DISATTIVAZIONE),LPAD(month(D_DISATTIVAZIONE),2,0)),'209912') AS INT) and b_valido='Y' DISTRIBUTE BY n_id_pod
       ) r LEFT OUTER JOIN (SELECT * FROM stato_pods_1 DISTRIBUTE BY n_id_pod )pp on r.n_id_pod = pp.n_id_pod
       where  pp.n_id_pod is null
    ) as TBL2
      WHERE D_AGGIORNAMENTO = MAX_D_AGGIORNAMENTO
    UNION ALL
    SELECT n_id_pod , STATO_POD FROM stato_pods_1
   """

        //hvCtx.sql("DROP TABLE IF EXISTS stato_pods")
        hvCtx.sql("DROP VIEW IF EXISTS stato_pods")
        hvCtx.sql(stato_pd)
      }
    }

    th1.start()

    val th2 = new Thread {

      //CREATE TABLE MISURE STORED AS PARQUET  AS
      override def run {
        val misure =
          s"""
    CREATE VIEW MISURE AS
      select case when TRATTAMENTO = 'O' THEN '1' ELSE '0' END IS_T_TRATTAMENTO, N_ID_POD
    FROM
    (
      select CASE WHEN D_ANNO_MESE < ${annomese}01 THEN T_TRATTAMENTO_SUCC ELSE T_TRATTAMENTO END TRATTAMENTO, n_id_pod, RANG, POD14
    from (
      SELECT n_id_pod, RANK() OVER (PARTITION BY n_id_pod ORDER BY d_anno_mese desc, d_aggiornamento desc) rang, t_trattamento, t_trattamento_succ, D_ANNO_MESE, POD14
        from (
        SELECT Y.n_id_pod, t_trattamento, nvl(t_trattamento_succ,t_trattamento)t_trattamento_succ,
        CAST(CONCAT(year(x.D_ANNO_MESE),LPAD(month(x.D_ANNO_MESE),2,0),LPAD(day(x.D_ANNO_MESE),2,0)) AS INT) D_ANNO_MESE, X.d_aggiornamento,POD14
        FROM (SELECT * FROM RCUS.rcus_podmisure_p F WHERE CAST(CONCAT(year(F.D_ANNO_MESE),LPAD(month(F.D_ANNO_MESE),2,0),LPAD(day(F.D_ANNO_MESE),2,0)) AS INT)  <=  ${annomese}01 DISTRIBUTE BY F.N_ID_POD) X
        INNER JOIN (SELECT SUBSTR(T_CODICE_POD,1,14) POD14,N_ID_POD FROM RCU.RCU_POD_p DISTRIBUTE BY N_ID_POD )Y
        where X.N_ID_POD = Y.N_ID_POD
    )T1 )T2 where rang = 1
    ) AS TBL
      """

       // hvCtx.sql("DROP TABLE IF EXISTS MISURE")
        hvCtx.sql("DROP VIEW IF EXISTS MISURE")
        hvCtx.sql(misure)
      }
    }

    th2.start()

    val th3 = new Thread {

      // CREATE TABLE TRATTAMENTO STORED AS PARQUET AS
      override def run {
        val trattamento =
          s"""
      CREATE VIEW TRATTAMENTO AS
      SELECT CASE T_TRATTAMENTO WHEN 'O' THEN '1' ELSE '0' END IS_T_TRATTAMENTO,n_id_pod
      FROM
      (SELECT CAST(CONCAT(year(x.D_ANNO_MESE),LPAD(month(x.D_ANNO_MESE),2,0),LPAD(day(x.D_ANNO_MESE),2,0)) AS INT)D_ANNO_MESE, T_TRATTAMENTO,n_id_pod,
      MAX(CAST(CONCAT(year(x.D_ANNO_MESE),LPAD(month(x.D_ANNO_MESE),2,0),LPAD(day(x.D_ANNO_MESE),2,0)) AS INT)) over ( partition by n_id_pod) MAX_D_ANNO_MESE
        FROM RCU.RCU_POD_MISURE_P x
      where  ${annomese}01 >= CAST(CONCAT(year(x.D_ANNO_MESE),LPAD(month(x.D_ANNO_MESE),2,0),LPAD(day(x.D_ANNO_MESE),2,0)) AS INT)
      ) AS POD_MIS
      where D_ANNO_MESE = MAX_D_ANNO_MESE
    """


       // hvCtx.sql("DROP TABLE IF EXISTS TRATTAMENTO")
        hvCtx.sql("DROP VIEW IF EXISTS TRATTAMENTO")
        hvCtx.sql(trattamento)
      }
    }
    th3.start()


    val queryTratt =
      """
        CREATE TABLE validazione_pod STORED AS PARQUET AS
        SELECT  NVL(p.T_CODICE_POD,tt.T_CODICE_POD) T_CODICE_POD,COALESCE(tt.STATO_POD,tmp.STATO_POD,'0') STATO_POD ,
        COALESCE(tt.IS_T_TRATTAMENTO,t.IS_T_TRATTAMENTO, MISURE.IS_T_TRATTAMENTO,'0')IS_T_TRATTAMENTO
        from (SELECT substr(T_CODICE_POD,1,14)T_CODICE_POD,n_id_pod FROM RCU.RCU_POD_P DISTRIBUTE BY T_CODICE_POD) p
        FULL OUTER JOIN
        (select substr(POD,1,14) T_CODICE_POD, '1' STATO_POD,'1' IS_T_TRATTAMENTO FROM tmpod_cloud.forzare_trattamento_tot_am_p DISTRIBUTE BY T_CODICE_POD) tt
        ON tt.T_CODICE_POD = p.T_CODICE_POD
        LEFT OUTER JOIN (SELECT * FROM stato_pods DISTRIBUTE BY n_id_pod )tmp on tmp.n_id_pod=p.n_id_pod
        LEFT OUTER JOIN (SELECT * FROM TRATTAMENTO DISTRIBUTE BY n_id_pod) t ON  p.n_id_pod = t.n_id_pod
        LEFT OUTER JOIN (SELECT * FROM  MISURE DISTRIBUTE BY n_id_pod )MISURE ON p.N_ID_POD = MISURE.N_ID_POD
      """.stripMargin


    if (th1.isAlive)
      th1.join()
    if (th2.isAlive)
      th2.join()
    if (th3.isAlive)
      th3.join()


    hvCtx.sql("SET hive.auto.convert.join=false")
    hvCtx.sql("DROP TABLE IF EXISTS validazione_pod")

    hvCtx.sql(queryTratt)


    /*hvCtx.sql("DROP TABLE IF EXISTS stato_pods_1")
    hvCtx.sql("DROP TABLE IF EXISTS stato_pods")
    hvCtx.sql("DROP TABLE IF EXISTS MISURE")
    hvCtx.sql("DROP TABLE IF EXISTS TRATTAMENTO")
   */

    hvCtx.sql("DROP VIEW IF EXISTS stato_pods_1")
    hvCtx.sql("DROP VIEW IF EXISTS stato_pods")
    hvCtx.sql("DROP VIEW IF EXISTS MISURE")
    hvCtx.sql("DROP VIEW IF EXISTS TRATTAMENTO")


  }
}
