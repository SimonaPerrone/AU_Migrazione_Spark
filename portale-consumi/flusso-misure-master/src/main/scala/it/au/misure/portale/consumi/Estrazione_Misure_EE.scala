package it.au.misure.portale.consumi

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import it.au.misure.util.{CreateProperties, LoggingSupport}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object Estrazione_Misure_EE
  extends LoggingSupport {

  val format = new SimpleDateFormat("yyyy-MM-dd")
  val propertiesC =new CreateProperties(System.getProperty("user.dir"))
  val prop:Properties = propertiesC.prop
  val queryProp:Properties = propertiesC.query

  val tbl_misurequarti:String ="au."+prop.getProperty("spark.app.flussoquarti_table")
  val tbl_misurequartiDaOracle:String="au."+prop.getProperty("spark.app.flussoquarti_table.oracle")
  val read1G_no_orarie=prop.getProperty("spark.app.portale.usa_misure_noaggr1G").toBoolean


  val url:String = prop.getProperty("spark.app.url")
  val user:String = prop.getProperty("spark.app.user")
  val password:String = prop.getProperty("spark.app.password")
  val driver = prop.getProperty("spark.app.jdbc.driver")
  Class.forName(driver)

  val connOracle:Connection = DriverManager.getConnection(url, user, password)
  val queryTrattamento = queryProp.getProperty("spark.query.queryPs2")

  var hiveCtx:HiveContext=null
  val numMesi:Int=14


  /**
    * Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
    * @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
    */
  def main(args: Array[String]) {

    val commandLineOptions = new CommandLineOptions()
    val commonsCliUtils = new CommonsCliUtils()
    val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
    val argsObjMaster = new CommonsCliUtils().getArgsPort_Consumi(commandLine)


    val trovaPodOrari:Boolean=if(argsObjMaster.PdoRfo.contains("-TO") || argsObjMaster.PdoRfo=="TO")true else false

    val tipoEstrazione = argsObjMaster.PdoRfo.replace("-TO","")

    val descrTipoEstr=if(tipoEstrazione=="O") "ORARIE" else if(tipoEstrazione=="ON") "ORARIE DA NON ORARIE" else if(tipoEstrazione=="N")"NON ORARIE/AUTOLETTURE/VOLTURE" else if(tipoEstrazione=="A") "AUTOLETTURE/VOLTURE" else if(tipoEstrazione=="TO") "RICERCA POD-ORARI" else if(tipoEstrazione=="ST") " " else ""

    if(descrTipoEstr=="")
      {
        log.info("Bisogna indicare una opzione tra TO/O[-TO]/ON[-TO]/N[-TO]/A[-TO]/ST[-TO] (TROVA POD ORARI/ORARIE/ORARIE DA NON ORARIE/NON ORARIE-AUTOLETTURE-VOLTURE/AUTOLETTURE-VOLTURE/STORICO) inserendo -TO a seguire di O/ON/N/A/ST viene eseguita pure la ricerca dei pod orari")
        return
      }

    if(tipoEstrazione=="O" || tipoEstrazione=="ON" || tipoEstrazione=="A")
    log.info("RICORDARSI DI ESEGUIRE PRIMA LE MISURE NON ORARIE E POI LE ORARIE")

    val nameApp = argsObjMaster.appName + " " + descrTipoEstr
    log.info("***** Inizio processo " + nameApp + " *****")

    log.info("***** current user " + System.getProperty("user.name") + "****")
    log.info(propertiesC.printEnvVar)
    val cur_user = System.getProperty("user.name")


    log.info("***** Lettura misure 1G da tabella flusso_misure_noaggr : " + read1G_no_orarie.toString)


    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));

    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

    val d_max = anno + mese + giorno

    var mese_init = mese.toInt
    var anno_init = anno.toInt
    for (i <- 1 to (numMesi-1)) {
      mese_init = if (mese_init - 1 == 0) 12 else (mese_init - 1)
      anno_init = if (mese_init == 12) (anno_init - 1) else anno_init
    }


    val mese_init_str:String=("0" + (mese_init.toString)) takeRight 2


    val d_min = anno_init.toString + mese_init_str + "01"
    val d_min_mese = anno_init.toString + mese_init_str



    val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())


    val conf = new SparkConf()
      .setAppName(nameApp)

      .set("spark.shuffle.service.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster(argsObjMaster.master)

    val sc: SparkContext = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    sc.setLogLevel(argsObjMaster.logLevel)

    val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions")


    hiveCtx = new HiveContext(sc)
    hiveCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    hiveCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
    hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
    hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    hiveCtx.setConf("hive.exec.dynamic.partition", "true")
    hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
    hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
    hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")


    log.info(s"Estrazione pod con relative forniture per il periodo : ${d_min} - ${d_max}")

    val annomesiL: Boolean = commandLine.hasOption(commandLineOptions.annomese_sem.getOpt)
    val annomesiList: Array[String] = if (annomesiL) {
      val annomesi: String = commandLine.getOptionValue(commandLineOptions.annomese_sem.getOpt)
      log.info("Applicazione dell'estrazione misure ai seguenti mesi(se possibile) : " + annomesi)
      val list_annomesi = annomesi.split(",").filter(x => x.length == 6)
      list_annomesi
    } else Array()




    try {


      var mese_tmp = mese.toInt
      var anno_tmp = anno.toInt


      var d_min_mese_tmp = anno_init.toString + (("0" + mese_init.toString) takeRight 2)
      var d_max_mese_tmp = anno.toString + mese.toString

      if(tipoEstrazione!="TO" && tipoEstrazione!="ON")
      {
            log.info(s"Estrazione dati gdm per il periodo ${d_min_mese_tmp} - ${d_max_mese_tmp}")
            val dtgdm = hiveCtx.sql(
              s"""SELECT distinct codice_pod,CAST(SUBSTR(d_inst_misurator_att,1,6) AS INT)annomese FROM mongodbs.gdm
                     WHERE t_tipo_misuratore <> 'G' AND (nvl(d_inst_misurator_att,0)=0 or CAST(SUBSTR(d_inst_misurator_att,1,6) AS INT) <= ${d_max_mese_tmp}) """).persist(StorageLevels.MEMORY_ONLY_SER)
            dtgdm.registerTempTable("tbl_gdm")

      }


      if(trovaPodOrari){


        var mese_tmp2 = mese.toInt
        var anno_tmp2 = anno.toInt


        hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")


        for (i <- 1 to numMesi) {

          val annomese_cur = anno_tmp2.toString + (("0" + mese_tmp2.toString) takeRight 2)

          val rtv = verifyannomesipar(anno_tmp2, mese_tmp2, annomesiList, true)
          if (rtv > 0) {
            val x =hiveCtx.sql(s"SELECT count(codice_pod)num FROM misure.pods_orari where annomese =${annomese_cur}").collect()(0)
            log.info(s"Individuazione pod orari per annomese ${annomese_cur}")
            //(giorno.toInt <16) il giorno massimo ghigliottina aggregato periodico 16
            // la condizione presuppone se per il mese precedente rispetto al corrente è stata effettuata l'aggregazione periodica oraria

             //if (i == 1 || (i == 2 && (giorno.toInt < 16)) || (anno_tmp2 == 2018 && mese_tmp2 <= 7)) {
              if(x.getAs[Long](0)> 0){
              val tblmisure = if (anno_tmp2 == 2018 && mese_tmp2 <= 7) tbl_misurequartiDaOracle else tbl_misurequarti

              log.info(s"verifica trattamento da Oracle per annomese = ${annomese_cur}")

              val viewname = creazioneVistaStatoPod(annomese_cur)
              log.info(s"Creata vista Oracle ${viewname} - avvio lettura ")


              val dt_tmp_pods = hiveCtx.sql(
                s"""SELECT  podquarti,'${annomese_cur}' annomese_check
                                  FROM (select distinct podquarti from ${tblmisure} where annoquarti =${anno_tmp2} AND mesequarti = ${mese_tmp2} ) quarti
                                  LEFT OUTER JOIN (SELECT codice_pod t_pod FROM misure.pods_orari where annomese =${annomese_cur}) pods
                                  ON pods.t_pod = quarti.podquarti
                                  where  pods.t_pod is null """)



              val rdd_pods_Ora = dt_tmp_pods.map { erow =>
                val pod: String = erow.getString(0)
                val an_ms: String = erow.getString(1)
                //val stato :String = erow.getString(2)
                //val trattamento :String = erow.getString(3)

                val isOrario = validazioneStatoPod(pod, viewname)
                //val isOrario =trattamento.toUpperCase.equals("Y")
                if (isOrario)
                  Row.fromSeq(List(pod, "1", an_ms.toInt))
                else
                  Row.fromSeq(List(pod, "0", an_ms.toInt))
              }

              if (!rdd_pods_Ora.partitions.isEmpty) {
                log.info("Avvio scrittura in tabella misure.pods_orari")

                val schema_pods_ora = StructType(Array(
                  StructField("codice_pod", StringType, true),
                  StructField("is_orario", StringType, true),
                  StructField("annomese", IntegerType, true)))

                val dfQS1 = hiveCtx.createDataFrame(rdd_pods_Ora, schema_pods_ora)

                dfQS1
                  .write
                  .format("parquet")
                  .mode(SaveMode.Append)
                  .partitionBy("annomese")
                  .save("/acquirente_unico/misure/pods_orari")

                hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")
              }

            } else {

              val annomese_cur = anno_tmp2.toString + (("0" + mese_tmp2.toString) takeRight 2)
              val tmpx =
                s"""
              INSERT INTO misure.pods_orari PARTITION(annomese)
              select ORA.pod codice_pod,is_orario,${annomese_cur} annomese
              from
              (
                SELECT pod,max(is_orario)is_orario from(SELECT pod,case when trattamento = 'Y' then '1' else '0' end is_orario
                from au.aggregazioni_misure_orarie  where ANNO=${anno_tmp2} AND MESE =${mese_tmp2}) XX
                group by pod
               ) ORA

             """
                /*
                LEFT OUTER JOIN (SELECT codice_pod t_pod FROM misure.pods_orari where annomese =${annomese_cur}) pods on pods.t_pod = ORA.pod
                where pods.t_pod is null
                 */

              hiveCtx.sql(tmpx)
              hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")

              val tblmisure = if (anno_tmp2 == 2018 && mese_tmp2 <= 7) tbl_misurequartiDaOracle else tbl_misurequarti

              log.info(s"verifica trattamento da Oracle per annomese = ${annomese_cur}")

              val viewname = creazioneVistaStatoPod(annomese_cur)
              log.info(s"Creata vista Oracle ${viewname} - avvio lettura ")


                val dt_tmp_pods = hiveCtx.sql(
                  s"""SELECT  podquarti,'${annomese_cur}' annomese_check
                                  FROM (select distinct podquarti from ${tblmisure} where annoquarti =${anno_tmp2} AND mesequarti = ${mese_tmp2} ) quarti
                                  LEFT OUTER JOIN (SELECT codice_pod t_pod FROM misure.pods_orari where annomese =${annomese_cur}) pods
                                  ON pods.t_pod = quarti.podquarti
                                  where  pods.t_pod is null """)

              /*val dt_tmp_pods = hiveCtx.sql(
                s"""SELECT DISTINCT podquarti,'${annomese_cur}' annomese_check
                                  FROM ${tblmisure} quarti
                                                 LEFT OUTER JOIN (SELECT codice_pod t_pod FROM misure.pods_orari where annomese =${annomese_cur}) pods
                                                 ON pods.t_pod = quarti.podquarti
                                                 where annoquarti =${anno_tmp2} AND mesequarti = ${mese_tmp2} and pods.t_pod is null """)*/



              val rdd_pods_Ora = dt_tmp_pods.map { erow =>
                val pod: String = erow.getString(0)
                val an_ms: String = erow.getString(1)


                val isOrario = validazioneStatoPod(pod, viewname)
                if (isOrario)
                  Row.fromSeq(List(pod, "1", an_ms.toInt))
                else
                  Row.fromSeq(List(pod, "0", an_ms.toInt))
              }

              if (!rdd_pods_Ora.partitions.isEmpty) {
                log.info("Avvio scrittura in tabella misure.pods_orari")

                val schema_pods_ora = StructType(Array(
                  StructField("codice_pod", StringType, true),
                  StructField("is_orario", StringType, true),
                  StructField("annomese", IntegerType, true)))

                val dfQS1 = hiveCtx.createDataFrame(rdd_pods_Ora, schema_pods_ora)

                dfQS1
                  .write
                  .format("parquet")
                  .mode(SaveMode.Append)
                  .partitionBy("annomese")
                  .save("/acquirente_unico/misure/pods_orari")

                hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")
              }


            }

          }
            mese_tmp2 = if (mese_tmp2 - 1 == 0) 12 else (mese_tmp2 - 1)
            anno_tmp2 = if (mese_tmp2 == 12) (anno_tmp2 - 1) else anno_tmp2

        }
        connOracle.close()
        hiveCtx.sql(s"MSCK REPAIR TABLE misure.pods_orari")
      }


      //if (tipoEstrazione == "O" || tipoEstrazione == "N" || tipoEstrazione=="ST") {
       if (tipoEstrazione=="ST") {

        val prevanno=anno_init
        val prev_annomese_del = (if (mese_init == 12) (prevanno - 1) else prevanno).toString + ("0" + ((if (mese_init - 1 == 0) 12 else (mese_init - 1)).toString) takeRight 2)

        log.info(s"Cancellazioni partizioni misure non orarie , orarie ,storiche per annomese  = ${prev_annomese_del}")

        val q_drop_part:String=s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese=${prev_annomese_del})"
        hiveCtx.sql(q_drop_part)

        val q_drop_part2:String=s"ALTER TABLE misure.misure_non_orarie_base DROP IF EXISTS PARTITION(annomese=${prev_annomese_del})"
        hiveCtx.sql(q_drop_part2)

        val q_drop_part3:String=s"ALTER TABLE misure.misure_storic DROP IF EXISTS PARTITION(annomese=${prev_annomese_del})"
        hiveCtx.sql(q_drop_part3)

         val q_drop_part5: String = s"TRUNCATE TABLE misure.misure_storic_f"
         hiveCtx.sql(q_drop_part5)

        for (i <- 1 to numMesi) {

          val rtv = verifyannomesipar(anno_tmp, mese_tmp, annomesiList, true)
          if (rtv > 0) {
            //if (tipoEstrazione == "O")
             // calcMisure_Orarie(d_max_mese_tmp, d_min_mese_tmp, anno_tmp, mese_tmp)
            //else if (tipoEstrazione == "N")
             // calcMisure_NonOrarie(anno_tmp, mese_tmp)
            //else if (tipoEstrazione == "ST")
              calcMisure_storic(anno_tmp, mese_tmp)
          }



          d_max_mese_tmp = d_min_mese_tmp

          mese_tmp = if (mese_tmp - 1 == 0) 12 else (mese_tmp - 1)
          anno_tmp = if (mese_tmp == 12) (anno_tmp - 1) else anno_tmp

          d_min_mese_tmp = anno_tmp.toString + (("0" + mese_tmp.toString) takeRight 2)
        }
      }

      //if (tipoEstrazione == "O" || tipoEstrazione == "ON" || tipoEstrazione == "ST") {
      if (tipoEstrazione == "ON" || tipoEstrazione == "ST") {

        var prev_mese_tmp = mese.toInt
        var prev_anno_tmp = anno.toInt
        mese_tmp = mese.toInt
        anno_tmp = anno.toInt

        hiveCtx.sql("MSCK REPAIR TABLE misure.misure_non_orarie_base")
        hiveCtx.sql("MSCK REPAIR TABLE misure.pods_orari")



        for (i <- 1 to numMesi) {

          prev_mese_tmp = if (prev_mese_tmp - 1 == 0) 12 else (prev_mese_tmp - 1)
          prev_anno_tmp = if (prev_mese_tmp == 12) (prev_anno_tmp - 1) else prev_anno_tmp

          val rtv = verifyannomesipar(anno_tmp, mese_tmp, annomesiList, false)
          if (rtv > 0) {
              calcMisure_Orarie_DaNonOrarie(anno_tmp, mese_tmp, prev_anno_tmp, prev_mese_tmp)
            }



          d_max_mese_tmp = d_min_mese_tmp


          mese_tmp = if (mese_tmp - 1 == 0) 12 else (mese_tmp - 1)
          anno_tmp = if (mese_tmp == 12) (anno_tmp - 1) else anno_tmp

          d_min_mese_tmp = anno_tmp.toString + (("0" + mese_tmp.toString) takeRight 2)
        }
      }


      if(tipoEstrazione=="N" || tipoEstrazione=="ST" || tipoEstrazione=="O" || tipoEstrazione=="A")
      log.info(s"Estrazione forniture per il periodo ${d_min_mese_tmp} - ${d_max_mese_tmp}")
      val dt_forniture = hiveCtx.sql(
            """SELECT distinct t_cf cf_piva ,codice_fornitura n_id_fornitura,codice_pod,
                                        CAST(data_inizio_fornitura_num AS BIGINT)inizio,CAST(data_fine_fornitura_num AS BIGINT) fine from mongodbs.forniture_elettriche """).persist(StorageLevels.MEMORY_ONLY_SER)
      dt_forniture.registerTempTable("tbl_forniture")


      if(!trovaPodOrari) {
        if (tipoEstrazione == "N" || tipoEstrazione == "ST") {
          // calcMisure_storic_c(anno.toInt,mese.toInt)
          calMisure_NoOrarieGG_Mese_Delta(d_max, d_min)
        }
        if (tipoEstrazione == "O" || tipoEstrazione == "ST")
          calMisureOrarieGG_Mese_Delta(d_max, d_min)
        if (tipoEstrazione == "N" || tipoEstrazione == "A" || tipoEstrazione == "ST")
          calMisure_Autoletture_Volture(d_max, d_min)

      }

      if(tipoEstrazione!="TO" && tipoEstrazione!="ON")
      {
        hiveCtx.dropTempTable("tbl_gdm")
        hiveCtx.dropTempTable("tbl_forniture")
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sc.stop()
      log.info(s"***** Fine processo ${nameApp} *****")
    }




  }
  def creazioneVistaStatoPod(annomese:String) : String = {
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
  }

  def verifyannomesipar(anno:Int,mese:Int,annomesi:Array[String],showMsg:Boolean): Int ={

    val annomese_tmp =anno.toString +(("0" + mese) takeRight 2)
    if(annomesi.length > 0){
      if(showMsg) log.info("*** verifica anno mese " + annomese_tmp + " tra i mesi passati da riga comando ***");

      if(!annomesi.contains(annomese_tmp))
      {
        if(showMsg)log.info("*** anno e mese " + annomese_tmp + " non trovato e quindi skippato **** ");

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


     val prevOrarie=
       s"""
         SELECT DISTINCT podquarti codice_pod2
         FROM misure.misure_orarie_base ora
         WHERE ora.annomese < ${annomese} and da_no_ora='0'
       """.stripMargin
      val dtprev_orarie=hiveCtx.sql(prevOrarie)//.persist(StorageLevels.MEMORY_ONLY_SER)
      dtprev_orarie.registerTempTable("tbl_prevorarie")


      /*
       INSERT INTO misure.misure_orarie_base PARTITION(annomese,da_no_ora)
              SELECT NO_BASE.pod podquarti, NO_BASE.giorno giornoquarti, ${prev_anno} annoquarti,${prev_mese} mesequarti,
              CONCAT('${prev_annomese}',LPAD(NO_BASE.giorno,2,0)) annomesegiorno,0.0 consumo,
               NO_BASE.eaf1,NO_BASE.eaf2,NO_BASE.eaf3,NO_BASE.eaf4,NO_BASE.eaf5,NO_BASE.eaf6,
               0.0 perdita,0.0 potmax,
              '0' Is2G,NO_BASE.tipo_flusso,${prev_annomese} annomese,'1'  da_no_ora
       */
      val strInsert=
        s"""
             INSERT INTO misure.misure_orarie_base PARTITION(annomese,da_no_ora)
              SELECT NO_BASE.pod podquarti, NO_BASE.giorno giornoquarti, SUBSTR(annomese,1,4) annoquarti,SUBSTR(annomese,5,2) mesequarti,
              CONCAT(annomese,LPAD(NO_BASE.giorno,2,0)) annomesegiorno,0.0 consumo,
               NO_BASE.eaf1,NO_BASE.eaf2,NO_BASE.eaf3,NO_BASE.eaf4,NO_BASE.eaf5,NO_BASE.eaf6,
               0.0 perdita,0.0 potmax,
              '0' Is2G,NO_BASE.tipo_flusso, annomese,'1'  da_no_ora
              from
                (
                 select *, max(annomese_tp) over ( partition by pod)max_annomese_tp
                 from (select *,concat(annomese,(CASE tipo_flusso WHEN 'PNO_RNO' THEN 1 ELSE 2 END))annomese_tp from misure.misure_non_orarie_base where annomese < ${annomese})A
                 ) NO_BASE
                 INNER JOIN ( SELECT DISTINCT podquarti codice_pod
                              FROM misure.misure_orarie_base ora
                              LEFT OUTER JOIN tbl_prevorarie on podquarti = codice_pod2
                              WHERE ora.annomese =${annomese} and da_no_ora='0' and codice_pod2 is null
                              ) as pods
                 on pods.codice_pod = NO_BASE.pod
                 where NO_BASE.annomese_tp = NO_BASE.max_annomese_tp


            """.stripMargin

      hiveCtx.sql(strInsert)

      dtprev_orarie.unpersist()
      hiveCtx.dropTempTable("tbl_prevorarie")


    }

  }


  def calcMisure_storic(anno:Int,mese:Int): Unit ={

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)
    val meseanno_str= (("0" + mese.toString) takeRight 2) + "/" +  anno.toString

    log.info(s"Estrazione pod orari per il periodo : ${annomese}" )
    val tblmisure=if(anno==2018 && mese <=7)tbl_misurequartiDaOracle else tbl_misurequarti

    //misure orarie 1G + (2G con fasce) da considerare come orarie
    val strpod_orari=
      s"""
        SELECT DISTINCT codice_pod FROM (
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN ( SELECT codice_pod FROM tbl_gdm WHERE annomese <= ${annomese} ) gdm
        ON p_ora.codice_pod=gdm.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        UNION ALL
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN
        ( SELECT SUBSTR(podquarti,1,14)podquarti FROM au.flusso_misure_estensione_quarti
          WHERE CONCAT(annoquarti,LPAD(mesequarti,2,0)) = ${annomese}
         ) quarti_ext
        on podquarti=p_ora.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        ) AS TBL
      """.stripMargin


    val dt_pod_orari=hiveCtx.sql(strpod_orari).persist(StorageLevels.MEMORY_ONLY_SER)
    val tblpodorari="list_pod_orari"
    dt_pod_orari.registerTempTable(tblpodorari)



    val q_drop_part:String=s"ALTER TABLE misure.misure_storic DROP IF EXISTS PARTITION(annomese=${annomese})"
    hiveCtx.sql(q_drop_part)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_storic")

    //misure non orarie
    calcMisure_storic_no_ora(anno,mese,tblpodorari)

    val str_quarti=
      s"""
         select *,case (split(A.nomefile,"_")[3]) when 'PDO' THEN 1 when 'PDO2G' then 2 when 'RFO' then 3 when 'RFO2G' then 4 else 0 END tipo_flusso_num
         from ${tblmisure} A WHERE  A.annoquarti = ${anno} AND A.mesequarti =${mese} AND VALIDATO ='S' AND  tipodato_e = 1
      """.stripMargin
    val dtquarti=hiveCtx.sql(str_quarti).persist(StorageLevels.MEMORY_ONLY_SER)
    dtquarti.registerTempTable("tbl_quarti")

    /*
     CAST(concat(A.annoquarti,LPAD(A.mesequarti,2,0)) AS INT) annomese,
        CAST(concat(A.annoquarti,LPAD(A.mesequarti,2,0),LPAD(A.giornoquarti,2,0)) AS BIGINT) annomesegiorno,
        concat(LPAD(A.giornoquarti,2,0),'/',LPAD(A.mesequarti,2,0),'/', A.annoquarti)data_lettura_str,
     */
    log.info(s"Estrazione misure orarie per il periodo : ${annomese}" )
    val strtmp_table =
      s"""
        SELECT CONCAT(A.e1, ';' , A.e2 , ';' , A.e3 , ';' , A.e4 , ';' , A.e5 , ';' , A.e6 , ';' , A.e7 , ';' , A.e8 , ';' , A.e9 , ';' , A.e10 , ';' , A.e11 , ';' , A.e12 , ';' , A.e13 , ';' , A.e14 , ';' , A.e15 , ';' , A.e16 , ';' , A.e17 , ';' , A.e18 , ';' , A.e19 , ';' , A.e20 , ';' ,
        A.e21 , ';' , A.e22 , ';' , A.e23 , ';' , A.e24 , ';' , A.e25 , ';' , A.e26 , ';' , A.e27 , ';' , A.e28 , ';' , A.e29 , ';' , A.e30 , ';' , A.e31 , ';' , A.e32 , ';' , A.e33 , ';' , A.e34 , ';' , A.e35 , ';' , A.e36 , ';' , A.e37 , ';' , A.e38 , ';' ,
        A.e39 , ';' , A.e40 , ';' , A.e41 , ';' , A.e42 , ';' , A.e43 , ';' , A.e44 , ';' , A.e45 , ';' , A.e46 , ';' , A.e47 , ';' , A.e48 , ';' , A.e49 , ';' , A.e50 , ';' , A.e51 , ';' , A.e52 , ';' , A.e53 , ';' , A.e54 , ';' , A.e55 , ';' , A.e56 , ';' ,
        A.e57 , ';' , A.e58 , ';' , A.e59 , ';' , A.e60 , ';' , A.e61 , ';' , A.e62 , ';' , A.e63 , ';' , A.e64 , ';' , A.e65 , ';' , A.e66 , ';' , A.e67 , ';' , A.e68 , ';' , A.e69 , ';' , A.e70 , ';' , A.e71 , ';' , A.e72 , ';' , A.e73 , ';' , A.e74 , ';' ,
        A.e75 , ';' , A.e76 , ';' , A.e77 , ';' , A.e78 , ';' , A.e79 , ';' , A.e80 , ';' , A.e81 , ';' , A.e82 , ';' , A.e83 , ';' , A.e84 , ';' , A.e85 , ';' , A.e86 , ';' , A.e87 , ';' , A.e88 , ';' , A.e89 , ';' , A.e90 , ';' , A.e91 , ';' , A.e92 , ';' ,
        A.e93 , ';' , A.e94 , ';' , A.e95 , ';' , A.e96 )EA,
		    CONCAT(A.er1, ';' , A.er2 , ';' , A.er3 , ';' , A.er4 , ';' , A.er5 , ';' , A.er6 , ';' , A.er7 , ';' , A.er8 , ';' , A.er9 , ';' , A.er10 , ';' , A.er11 , ';' , A.er12 , ';' , A.er13 , ';' , A.er14 , ';' , A.er15 , ';' , A.er16 , ';' , A.er17 , ';' , A.er18 , ';' , A.er19 , ';' , A.er20 , ';' ,
        A.er21 , ';' , A.er22 , ';' , A.er23 , ';' , A.er24 , ';' , A.er25 , ';' , A.er26 , ';' , A.er27 , ';' , A.er28 , ';' , A.er29 , ';' , A.er30 , ';' , A.er31 , ';' , A.er32 , ';' , A.er33 , ';' , A.er34 , ';' , A.er35 , ';' , A.er36 , ';' , A.er37 , ';' , A.er38 , ';' ,
        A.er39 , ';' , A.er40 , ';' , A.er41 , ';' , A.er42 , ';' , A.er43 , ';' , A.er44 , ';' , A.er45 , ';' , A.er46 , ';' , A.er47 , ';' , A.er48 , ';' , A.er49 , ';' , A.er50 , ';' , A.er51 , ';' , A.er52 , ';' , A.er53 , ';' , A.er54 , ';' , A.er55 , ';' , A.er56 , ';' ,
        A.er57 , ';' , A.er58 , ';' , A.er59 , ';' , A.er60 , ';' , A.er61 , ';' , A.er62 , ';' , A.er63 , ';' , A.er64 , ';' , A.er65 , ';' , A.er66 , ';' , A.er67 , ';' , A.er68 , ';' , A.er69 , ';' , A.er70 , ';' , A.er71 , ';' , A.er72 , ';' , A.er73 , ';' , A.er74 , ';' ,
        A.er75 , ';' , A.er76 , ';' , A.er77 , ';' , A.er78 , ';' , A.er79 , ';' , A.er80 , ';' , A.er81 , ';' , A.er82 , ';' , A.er83 , ';' , A.er84 , ';' , A.er85 , ';' , A.er86 , ';' , A.er87 , ';' , A.er88 , ';' , A.er89 , ';' , A.er90 , ';' , A.er91 , ';' , A.er92 , ';' ,
        A.er93 , ';' , A.er94 , ';' , A.er95 , ';' , A.er96 )ER,
        (A.e1+A.e2 + A.e3 + A.e4 + A.e5 + A.e6 + A.e7 + A.e8 + A.e9 + A.e10 + A.e11 + A.e12 + A.e13 + A.e14 + A.e15 + A.e16 + A.e17 + A.e18 + A.e19 + A.e20 +
        A.e21 + A.e22 + A.e23 + A.e24 + A.e25 + A.e26 + A.e27 + A.e28 + A.e29 + A.e30 + A.e31 + A.e32 + A.e33 + A.e34 + A.e35 + A.e36 + A.e37 + A.e38 +
        A.e39 + A.e40 + A.e41 + A.e42 + A.e43 + A.e44 + A.e45 + A.e46 + A.e47 + A.e48 + A.e49 + A.e50 + A.e51 + A.e52 + A.e53 + A.e54 + A.e55 + A.e56 +
        A.e57 + A.e58 + A.e59 + A.e60 + A.e61 + A.e62 + A.e63 + A.e64 + A.e65 + A.e66 + A.e67 + A.e68 + A.e69 + A.e70 + A.e71 + A.e72 + A.e73 + A.e74 +
        A.e75 + A.e76 + A.e77 + A.e78 + A.e79 + A.e80 + A.e81 + A.e82 + A.e83 + A.e84 + A.e85 + A.e86 + A.e87 + A.e88 + A.e89 + A.e90 + A.e91 + A.e92 +
        A.e93 + A.e94 + A.e95 + A.e96 + A.e97 + A.e98 + A.e99 + A.e100)consumo,
        A.podquarti,A.perdita,substr(A.areaquarti,1,6)areaquarti,A.potmax,A.nomefile,A.dataelaborazione,A.cifrerea,
        ${annomese} annomese,CAST(concat('${annomese}',LPAD(A.giornoquarti,2,0)) AS BIGINT) annomesegiorno,
        CONCAT(A.time_stamp,unix_timestamp(A.dataelaborazione)) times_elab,tipo_flusso_num,
        annomesegiornodir data_ricezione,
        concat(LPAD(A.giornoquarti,2,0),'/${meseanno_str}')data_lettura_str,
        concat( SUBSTR(annomesegiornodir,7,2),'/',SUBSTR(annomesegiornodir,5,2),'/',SUBSTR(annomesegiornodir,1,4)) data_ricezione_str
        FROM tbl_quarti A
        INNER JOIN ${tblpodorari} as pods
        on pods.codice_pod = SUBSTR(A.podquarti,1,14)

      """.stripMargin


    var tbl_base ="tbl_misure_quarti_1"
    val dtmisure_o_quarti_1= hiveCtx.sql(strtmp_table)//.persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_o_quarti_1.registerTempTable(tbl_base)


    val str_misure_o_quarti=
      s"""
        SELECT consumo,cifrerea,
        podquarti,perdita,areaquarti,potmax,
        annomese,annomesegiorno,tipo_flusso_num,data_ricezione,EA,ER,
        data_lettura_str,data_ricezione_str,
        CONCAT(annomese,podquarti,cifrerea,nomefile,dataelaborazione) KEY_QUARTI FROM
        (SELECT MAX(consumo)consumo,nomefile,dataelaborazione,MAX(cifrerea)cifrerea,
        podquarti,max(perdita)perdita,areaquarti,MAX(potmax)potmax,
        annomese,annomesegiorno,tipo_flusso_num,max(EA)EA,max(ER)ER,data_ricezione,
        data_lettura_str,data_ricezione_str FROM (
        SELECT *,
        max(A.times_elab) over ( partition by A.annomesegiorno,A.podquarti,A.tipo_flusso_num)max_times_elab
        FROM ${tbl_base} A
        ) XX WHERE times_elab = max_times_elab
        group by nomefile,dataelaborazione,podquarti,areaquarti,
        annomese,annomesegiorno,tipo_flusso_num,data_ricezione,data_lettura_str,data_ricezione_str
        ) as tbl_quarti  DISTRIBUTE BY KEY_QUARTI
      """.stripMargin


    log.info(s"""Estrazione quarti da ${tblmisure} dove A.annoquarti = ${anno} AND A.mesequarti =${mese} AND VALIDATO ='S' AND  tipodato_e = 1 """)

    val dtmisure_o_quarti= hiveCtx.sql(str_misure_o_quarti).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_o_quarti.registerTempTable("tbl_misure_quarti")



    val str_misure_o_ext_quarti=
      s"""

        SELECT eam,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         podquarti,progr_podsez ,annomese,
         CONCAT(annomese,podquarti,progr_podsez,nomefile,dataelaborazione)KEY_QUARTI,motivazione
        FROM
        (SELECT B.eam,B.eaf1  , B.eaf2  , B.eaf3  , B.eaf4  , B.eaf5  , B.eaf6,
        B.podquarti,B.nomefile,B.dataelaborazione ,B.progr_podsez ,
        CAST(CONCAT(B.annoquarti,LPAD(B.mesequarti,2,0)) AS INT) annomese,motivazione
        FROM
         (
              SELECT eam,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
              podquarti,nomefile,dataelaborazione ,MAX(progr_podsez)progr_podsez , annoquarti , mesequarti,
              NVL(motivazione,'')motivazione
              FROM au.flusso_misure_estensione_quarti
              where annoquarti =${anno} AND mesequarti =${mese}
              GROUP BY eam,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
              podquarti,nomefile,dataelaborazione ,annoquarti,mesequarti,tipo_flusso,motivazione
         )B
         )tbl_extquarti DISTRIBUTE BY KEY_QUARTI
      """.stripMargin


    log.info(s"""Estrazione estensione_quarti B.annoquarti =${anno} AND B.mesequarti =${mese} """)


    val dtmisure_o_ext_quarti= hiveCtx.sql(str_misure_o_ext_quarti)//.persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_o_ext_quarti.registerTempTable("tbl_misure_ext_quarti")


    log.info(s""" Elaborazione misure orarie storiche per il periodo ${annomese}""")


    val str_misure_orarie_storic=
      s"""
         SELECT DISTINCT podquarti pod ,annomesegiorno data_lettura,CAST(data_ricezione AS BIGINT)data_ricezione,motivazione,
         eam,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6 ,EA, ER,
         consumo,perdita,potmax,Is2G,tipo_flusso_num,'PDO_RFO' cod_flusso,
         CASE tipo_flusso_num when 1 then 'PDO' when 2 then 'PDO2G' when 3  then 'RFO' when 4 then 'RFO2G' else ''  END tipo_flusso,
         case
         WHEN (tipo_flusso_num = 1 or tipo_flusso_num = 2) THEN  'Lettura Periodica'
         WHEN (tipo_flusso_num = 3 or tipo_flusso_num = 4) THEN  'Lettura di Rettifica'
         ELSE '' END descr_tipoflusso,
         case motivazione
         when '1' then 'misura che sostituisce una stima precedente'
         when '2' then 'misura che sostituisce una misura fornita precedentemente errata'
         when '3' then 'misura fornita precedentemente per errore'
         when '4' then 'ricostruzione per frode'
         when '5' then 'ricostruzione per malfunzionamento misuratore'
         else motivazione end descr_motivazione,
         data_lettura_str,data_ricezione_str,annomese,'1' is_mis_oraria
         from(
         SELECT QUARTI.consumo,SUBSTR(QUARTI.podquarti,1,14)podquarti,
         QUARTI.perdita,QUARTI.annomese,QUARTI.annomesegiorno,QUARTI.potmax,
         nvl(EXT_QUARTI.eaf1,0)eaf1  , nvl(EXT_QUARTI.eaf2,0)eaf2  , nvl(EXT_QUARTI.eaf3,0)eaf3  ,
         nvl(EXT_QUARTI.eaf4,0)eaf4 , nvl(EXT_QUARTI.eaf5,0)eaf5  , nvl(EXT_QUARTI.eaf6,0)eaf6,
         case when nvl(EXT_QUARTI.KEY_QUARTI,'')='' then '0' else '1' END Is2G,QUARTI.tipo_flusso_num,
         QUARTI.data_ricezione,nvl(EXT_QUARTI.motivazione,'')motivazione,nvl(eam,0)eam,QUARTI.EA,QUARTI.ER,
         QUARTI.data_lettura_str,QUARTI.data_ricezione_str
         FROM tbl_misure_quarti QUARTI
         LEFT OUTER JOIN tbl_misure_ext_quarti EXT_QUARTI
         ON QUARTI.areaquarti ='NEW_F_' AND QUARTI.KEY_QUARTI = EXT_QUARTI.KEY_QUARTI
         ) as TBL
      """.stripMargin

    log.info("Join quarti + estensione quarti")
    val dtmisure_o_tmp_all= hiveCtx.sql(str_misure_orarie_storic)



    log.info(s"Scrittura misure orarie su tabella  misure.misure_storic per anno =${anno} e mese=${mese}")


    dtmisure_o_tmp_all
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("annomese","is_mis_oraria")
      .save("/acquirente_unico/misure/misure_storic")


    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_storic")
    hiveCtx.refreshTable(s"misure.misure_storic")

    hiveCtx.dropTempTable("tbl_quarti")
    hiveCtx.dropTempTable(tbl_base)
    hiveCtx.dropTempTable("tbl_misure_quarti")
    hiveCtx.dropTempTable("tbl_misure_ext_quarti")

    dtquarti.unpersist()
    //dtmisure_o_quarti_1.unpersist()
    dtmisure_o_quarti.unpersist()
   // dtmisure_o_ext_quarti.unpersist()

    hiveCtx.dropTempTable(tblpodorari)
    dt_pod_orari.unpersist()


    //scrittura su tabella misure.misure_orarie_base

    log.info("Avvio estrazione e scrittura misure su tabella misure.misure_orarie_base")

    val q_drop_parts:String=s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese=${annomese})"
    hiveCtx.sql(q_drop_parts)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")

    //a parita di pod e annomesegiorno misura viene prediletta la misura di rettifica con data di ricezione piu recente
    val str_misure_nora_base=
      s"""
        INSERT INTO  misure.misure_orarie_base PARTITION(annomese,da_no_ora)
        SELECT podquarti ,giornoquarti,annoquarti,mesequarti,annomesegiorno,
        consumo,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,perdita,potmax,Is2G,tipo_flusso,
        annomese,da_no_ora
        from
        (
        SELECT pod podquarti,CAST(SUBSTR(data_lettura,7,2) AS INT)giornoquarti,CAST(SUBSTR(data_lettura,1,4) AS INT)annoquarti,
        CAST(SUBSTR(data_lettura,5,2) AS INT)mesequarti,data_lettura annomesegiorno,consumo,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,perdita,potmax,Is2G,
        tipo_flusso,max(concat(data_ricezione,tipo_flusso_num)) over(partition by pod,data_lettura)max_dtric_tipo_flusso_num,
        concat(data_ricezione,tipo_flusso_num)dtric_tipo_flusso_num ,annomese,'0' da_no_ora
        from misure.misure_storic where annomese = ${annomese} and is_mis_oraria = '1' and motivazione <> '3'
        ) as tbl where dtric_tipo_flusso_num = max_dtric_tipo_flusso_num
      """.stripMargin

    hiveCtx.sql(str_misure_nora_base)


    log.info(s"Avvio estrazione e scrittura misure storiche su tabella misure.misure_storic_f")


          val storic_f =
            s"""
            select  F.t_cf cf_piva , pod ,data_lettura_str data_lettura ,data_ricezione_str data_ricezione ,
            descr_motivazione motivazione ,0.0 lettura_monoraria,
            nvl(eaf1,0.0)  lettura_f1,nvl(eaf2,0.0) lettura_f2 , nvl(eaf3,0.0) lettura_f3 ,
            nvl(eaf4,0.0) lettura_f4 , nvl(eaf5,0.0) lettura_f5 ,nvl(eaf6,0.0) lettura_f6,
            EA , ER  ,descr_tipoflusso tipo_flusso ,
            annomese annomese_riferimento , is_mis_oraria,data_lettura data_lettura_num
            from (SELECT * FROM misure.misure_storic WHERE annomese = ${annomese}) TBL_STOR1
            INNER JOIN mongodbs.forniture_elettriche F ON TBL_STOR1.pod = F.codice_pod
            WHERE TBL_STOR1.data_lettura >= CAST(F.data_inizio_fornitura_num AS BIGINT) AND
            TBL_STOR1.data_lettura <= CAST(F.data_fine_fornitura_num AS BIGINT)
      """.stripMargin

          hiveCtx.sql(storic_f)
            .write
            .format("parquet")
            .mode(SaveMode.Append)
            .save(s"/acquirente_unico/misure/misure_storic_f")

          hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_storic_f")
          log.info(s"Scrittura misure storiche su tabella misure.misure_storic_f per annomese : ${annomese} completata")

  }
  def calcMisure_storic_c(anno_i:Int,mese_i:Int): Unit ={

    //
    var anno_tmp=anno_i
    var mese_tmp=mese_i

    /*
     val str_misure_storic_c=
        s"""
        select F.cf_piva,pod ,data_lettura ,data_ricezione ,motivazione ,
        eam lettura_monoraria,eaf1  lettura_f1, eaf2 lettura_f2 , eaf3 lettura_f3 , eaf4 lettura_f4 , eaf5 lettura_f5 , eaf6 lettura_f6, EA , ER  ,tipo_flusso ,
        annomese annomese_riferimento ,is_mis_oraria
        from (select pod ,data_lettura ,data_ricezione ,motivazione ,
          eam ,eaf1 , eaf2  , eaf3  , eaf4  , eaf5  , eaf6 , EA , ER  ,tipo_flusso ,
          annomese  ,is_mis_oraria from misure.misure_storic) TBL_STOR
          INNER JOIN tbl_forniture F ON TBL_STOR.pod = F.codice_pod
        WHERE  data_lettura >= F.inizio AND data_lettura <= F.fine
      """.stripMargin
     */
    hiveCtx.sql(s"TRUNCATE TABLE misure.misure_storic_f")
    for (i <- 1 to numMesi) {

      val annomese = anno_tmp.toString + (("0" + mese_tmp.toString) takeRight 2)

      log.info("Ottimizzazione tabella storico per servizio rest annomese : " + annomese)
      val str_misure_storic_c=
        s"""
        INSERT INTO misure.misure_storic_f
        select pod ,data_lettura_str data_lettura ,data_ricezione_str data_ricezione ,descr_motivazione motivazione ,
        eam lettura_monoraria,eaf1  lettura_f1, eaf2 lettura_f2 , eaf3 lettura_f3 , eaf4 lettura_f4 , eaf5 lettura_f5 , eaf6 lettura_f6, EA , ER  ,descr_tipoflusso tipo_flusso ,
        annomese annomese_riferimento ,is_mis_oraria,data_lettura data_lettura_num
        from misure.misure_storic TBL_STOR where annomese = ${annomese}
      """.stripMargin

       hiveCtx.sql(str_misure_storic_c)


      mese_tmp = if (mese_tmp - 1 == 0) 12 else (mese_tmp - 1)
      anno_tmp = if (mese_tmp == 12) (anno_tmp - 1) else anno_tmp
    }


    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_storic_f")

    log.info("Scrittura storico in misure_storic_f completato con successo!")
  }

  def calcMisure_storic_no_ora(anno:Int,mese:Int,tblpodorari:String): Unit ={

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    log.info(s"Estrazione misure non orarie storiche per il mese ${mese} e anno ${anno}")

    /*
    SELECT nvl(CONCAT(e1, ';' , e2 , ';' , e3 , ';' , e4 , ';' , e5 , ';' , e6 , ';' , e7 , ';' , e8 , ';' , e9 , ';' , e10 , ';' , e11 , ';' , e12 , ';' , e13 , ';' , e14 , ';' , e15 , ';' , e16 , ';' , e17 , ';' , e18 , ';' , e19 , ';' , e20 , ';' ,
          e21 , ';' , e22 , ';' , e23 , ';' , e24 , ';' , e25 , ';' , e26 , ';' , e27 , ';' , e28 , ';' , e29 , ';' , e30 , ';' , e31 , ';' , e32 , ';' , e33 , ';' , e34 , ';' , e35 , ';' , e36 , ';' , e37 , ';' , e38 , ';' ,
          e39 , ';' , e40 , ';' , e41 , ';' , e42 , ';' , e43 , ';' , e44 , ';' , e45 , ';' , e46 , ';' , e47 , ';' , e48 , ';' , e49 , ';' , e50 , ';' , e51 , ';' , e52 , ';' , e53 , ';' , e54 , ';' , e55 , ';' , e56 , ';' ,
          e57 , ';' , e58 , ';' , e59 , ';' , e60 , ';' , e61 , ';' , e62 , ';' , e63 , ';' , e64 , ';' , e65 , ';' , e66 , ';' , e67 , ';' , e68 , ';' , e69 , ';' , e70 , ';' , e71 , ';' , e72 , ';' , e73 , ';' , e74 , ';' ,
          e75 , ';' , e76 , ';' , e77 , ';' , e78 , ';' , e79 , ';' , e80 , ';' , e81 , ';' , e82 , ';' , e83 , ';' , e84 , ';' , e85 , ';' , e86 , ';' , e87 , ';' , e88 , ';' , e89 , ';' , e90 , ';' , e91 , ';' , e92 , ';' ,
          e93 , ';' , e94 , ';' , e95 , ';' , e96 , ';' , e97 , ';' , e98 , ';' , e99 , ';' , e100),'') EA,
		      nvl(CONCAT(er1, ';' , er2 , ';' , er3 , ';' , er4 , ';' , er5 , ';' , er6 , ';' , er7 , ';' , er8 , ';' , er9 , ';' , er10 , ';' , er11 , ';' , er12 , ';' , er13 , ';' , er14 , ';' , er15 , ';' , er16 , ';' , er17 , ';' , er18 , ';' , er19 , ';' , er20 , ';' ,
          er21 , ';' , er22 , ';' , er23 , ';' , er24 , ';' , er25 , ';' , er26 , ';' , er27 , ';' , er28 , ';' , er29 , ';' , er30 , ';' , er31 , ';' , er32 , ';' , er33 , ';' , er34 , ';' , er35 , ';' , er36 , ';' , er37 , ';' , er38 , ';' ,
          er39 , ';' , er40 , ';' , er41 , ';' , er42 , ';' , er43 , ';' , er44 , ';' , er45 , ';' , er46 , ';' , er47 , ';' , er48 , ';' , er49 , ';' , er50 , ';' , er51 , ';' , er52 , ';' , er53 , ';' , er54 , ';' , er55 , ';' , er56 , ';' ,
          er57 , ';' , er58 , ';' , er59 , ';' , er60 , ';' , er61 , ';' , er62 , ';' , er63 , ';' , er64 , ';' , er65 , ';' , er66 , ';' , er67 , ';' , er68 , ';' , er69 , ';' , er70 , ';' , er71 , ';' , er72 , ';' , er73 , ';' , er74 , ';' ,
          er75 , ';' , er76 , ';' , er77 , ';' , er78 , ';' , er79 , ';' , er80 , ';' , er81 , ';' , er82 , ';' , er83 , ';' , er84 , ';' , er85 , ';' , er86 , ';' , er87 , ';' , er88 , ';' , er89 , ';' , er90 , ';' , er91 , ';' , er92 , ';' ,
          er93 , ';' , er94 , ';' , er95 , ';' , er96 , ';' , er97 , ';' , er98 , ';' , er99 , ';' , er100),'') ER,
     */

    val where1G=if(read1G_no_orarie) "((tipo_flusso in('PNO','PNO2G','VNO','VNO2G') AND tipodato_e=1) OR tipo_flusso IN ('RNO','RNO2G','RNV','RNV2G'))" else "((tipo_flusso in('PNO2G','VNO2G') AND tipodato_e=1) OR tipo_flusso IN ('RNO2G','RNV2G'))"


    val noaggr=
      s"""
        SELECT pod, eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,data_rilevazione,EaM,annomesegiornodir,motivazione,
                 giorno,SUBSTR(tipo_flusso,1,3)tipo_flusso,data_voltura,data_misura,
                 case tipo_flusso when 'PNO' THEN 1 when 'PNO2G' then 2 when 'RNO' then 3 when 'RNO2G' then 4
                 when 'VNO' THEN 5 when 'VNO2G' then 6 when 'RNV' then 7 when 'RNV2G' then 8
                 else 0 END tipo_flusso_num ,anno,mese,time_stamp,dataelaborazione
                  from au.flusso_misure_noaggr where  (anno = ${anno} AND mese =${mese}) AND (VALIDATO ='S' AND ${where1G} )

      """.stripMargin
    val dt_noaggr=hiveCtx.sql(noaggr).persist(StorageLevels.MEMORY_ONLY_SER)
    dt_noaggr.registerTempTable("tbl_noaggr")

    //CAST(concat(anno,LPAD(mese,2,0)) AS INT) annomese
    val tmp =
      s"""
          SELECT EA,ER,annomese,tipo_flusso,pod, eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,
          data_rilevazione,EaM,annomesegiornodir,
          motivazione,giorno,tipo_flusso_num,times_elab
          FROM
          (
           SELECT '' EA,'' ER,
           pod, eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,
           data_rilevazione,EaM,annomesegiornodir,motivazione,
           CAST( (CASE WHEN nvl(giorno,0)<>0 THEN giorno WHEN tipo_flusso ='VNO' OR tipo_flusso ='RNV' THEN substr(data_voltura,1,2) ELSE substr(data_misura,1,2) END) AS INT) giorno,
           tipo_flusso,tipo_flusso_num ,
           CONCAT(time_stamp,unix_timestamp(dataelaborazione)) times_elab ,
           ${annomese} annomese
           from tbl_noaggr noaggr
           LEFT OUTER JOIN ${tblpodorari} as pods on codice_pod = SUBSTR(pod,1,14)
           where  pods.codice_pod is null
           ) AS TBKX
      """.stripMargin

    //val dtx=hiveCtx.sql(tmp).persist(StorageLevels.MEMORY_ONLY_SER)
    //dtx.registerTempTable("tmp_tblx")

    val dtmisure_no_orarie_cloudera_base= hiveCtx.sql(tmp).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_no_orarie_cloudera_base.registerTempTable("tbl_misure_no_orarie_cloudera_base_storic")

   /* val strbase_misureno=
      s"""
          SELECT dataelaborazione,anno,mese,tipo_flusso,pod, eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,
          CAST(concat(anno,LPAD(mese,2,0)) AS INT) annomese,
          CAST( (CASE WHEN nvl(giorno,0)<>0 THEN giorno WHEN tipo_flusso ='VNO' OR tipo_flusso ='RNV' THEN substr(data_voltura,1,2) ELSE substr(data_misura,1,2) END) AS INT) giorno,
          time_stamp,data_voltura,data_misura,data_rilevazione,EaM,annomesegiornodir,
          case tipo_flusso_chk when 'PNO' THEN 1 when 'PNO2G' then 2 when 'RNO' then 3 when 'RNO2G' then 4
          when 'VNO' THEN 5 when 'VNO2G' then 6 when 'RNV' then 7 when 'RNV2G' then 8
          else 0 END tipo_flusso_num,motivazione,EA,ER
          from tmp_tblx
          WHERE (VALIDATO ='S' AND ${where1G} )

      """.stripMargin

    log.info(s"""Estrazione misure non orarie da au.flusso_misure_noaggr anno = ${anno} AND mese =${mese} AND VALIDATO ='S'
                AND ${where1G} """)

    val dtmisure_no_orarie_cloudera_base= hiveCtx.sql(strbase_misureno).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_no_orarie_cloudera_base.registerTempTable("tbl_misure_no_orarie_cloudera_base_storic")*/


    //CAST(CONCAT(SUBSTR(data_lettura_str,7,4),SUBSTR(data_lettura_str,4,2),SUBSTR(data_lettura_str,1,2)) AS BIGINT)data_lettura,


    val str_misure_norarie_cloudera=
      s"""
        SELECT SUBSTR(pod,1,14)pod,annomese,lettura_monoraria,
		    eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,tipo_flusso,
		    data_lettura,data_ricezione,tipo_flusso_num,data_rilevazione,motivazione,EA,ER
        FROM (
        SELECT lettura_monoraria ,A.pod,annomese,
        nvl(eaf1,0.0)eaf1,nvl(eaf2,0.0)eaf2,nvl(eaf3,0.0)eaf3,
		    nvl(eaf4,0.0)eaf4,nvl(eaf5,0.0)eaf5,nvl(eaf6,0.0)eaf6,
		    tipo_flusso,annomesegiorno data_lettura, times_elab, data_ricezione,
        max(A.times_elab) over ( partition by A.annomesegiorno,A.pod,A.tipo_flusso_num) max_times_elab,
        CAST(CONCAT(SUBSTR(data_rilevazione_str,7,4),SUBSTR(data_rilevazione_str,4,2),SUBSTR(data_rilevazione_str,1,2)) AS BIGINT)data_rilevazione,
        tipo_flusso_num,motivazione,EA,ER
        FROM
        (
         SELECT nvl(NO_BASE.EaM,0) as lettura_monoraria ,
         NO_BASE.pod,annomese,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,CAST(NO_BASE.annomesegiornodir AS BIGINT) as data_ricezione,
		     CASE WHEN tipo_flusso ='VNO' OR tipo_flusso ='RNV' THEN 'VNO_RNV' ELSE 'PNO_RNO' END tipo_flusso,
         CAST(concat(annomese,giorno) AS BIGINT) annomesegiorno,NO_BASE.times_elab ,
         data_rilevazione data_rilevazione_str,tipo_flusso_num,nvl(motivazione,'')motivazione,EA,ER
         FROM tbl_misure_no_orarie_cloudera_base_storic NO_BASE
         ) AS A
        ) XX WHERE times_elab =  max_times_elab

      """.stripMargin





    val dtmisure_no_orarie_cloudera= hiveCtx.sql(str_misure_norarie_cloudera).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_no_orarie_cloudera.registerTempTable("tbl_misure_no_orarie_cloudera_2")


    val strmisure_norarie_novno_all=s"""
                              SELECT pod,data_lettura,CAST(data_ricezione AS BIGINT)data_ricezione,motivazione,
                              max(lettura_monoraria)eam,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,max(eaf6)eaf6,EA,ER,
                              0.0 consumo,0.0 perdita,0.0 potmax,'0' Is2G,tipo_flusso_num,tipo_flusso cod_flusso,
                              case max(tipo_flusso_num) when 1 THEN 'PNO'  when 2 then 'PNO2G'  when 3 then 'RNO'  when 4 then 'RNO2G'
                              when 5 then 'VNO' when 6 then 'VNO2G' when 7 then 'RNV' when 8 then 'RNV2G' else '' end tipo_flusso,
                              annomese,'0' is_mis_oraria
                              FROM
                              (
                              select *,max(dt_filter) over(partition by KEY_PART) max_dt_filter
                              from(
                              SELECT pod,annomese,lettura_monoraria,
                              eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,tipo_flusso,
                              data_lettura,data_ricezione,
                              concat(data_lettura,data_ricezione)dt_filter,tipo_flusso_num,
                              nvl(motivazione,'') motivazione,EA,ER,CONCAT(data_lettura,pod,tipo_flusso_num)KEY_PART
                              from tbl_misure_no_orarie_cloudera_2 where tipo_flusso <> 'VNO_RNV'
                              UNION ALL
                              SELECT prt_tmo_mn_f.codice_pod pod,annomese, nvl(eam,0.0) lettura_monoraria,
                              nvl(eaf1,0.0)eaf1, nvl(eaf2,0.0)eaf2, nvl(eaf3,0.0)eaf3, 0.0 eaf4, 0.0 eaf5, 0.0 eaf6,'PNO_RNO' tipo_flusso,
                              data_lettura,data_ricezione,
                              concat(data_lettura,data_ricezione)dt_filter,tipo_flusso_num,
                              nvl(motivazione,'')motivazione,'' EA, '' ER,CONCAT(data_lettura,prt_tmo_mn_f.codice_pod,tipo_flusso_num)KEY_PART
                              from (select *,case cod_flusso when 'PNO' THEN 1 when 'RNO' then 3 else 0 end tipo_flusso_num from misure.prt_tmo_mn_f where annomese =${annomese}) prt_tmo_mn_f
                              LEFT OUTER JOIN  ${tblpodorari} as pods
                              on pods.codice_pod = prt_tmo_mn_f.codice_pod
                              where  pods.codice_pod is null
                              ) as X
                              ) AS TBL
                              WHERE dt_filter = max_dt_filter
                              GROUP BY pod,data_lettura,data_ricezione,motivazione,tipo_flusso_num,tipo_flusso,annomese,EA,ER
                              """

    //hiveCtx.sql(s"CREATE TABLE misure.PNO_VNO STORED AS PARQUET AS ${strmisure_norarie_novno_all}")

    log.info(s"""Estrazione misure non orarie oracle/cloudera """)

    val dtmisure_no_orarie_novno_all= hiveCtx.sql(strmisure_norarie_novno_all)//.persist(StorageLevels.MEMORY_ONLY_SER)

    //la data_lettura corrisponde dalla data_voltura


    val strmisure_norarie_vno_all=s"""
                              SELECT pod,data_lettura,CAST(data_ricezione AS BIGINT)data_ricezione,motivazione,
                              max(lettura_monoraria)eam,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,max(eaf6)eaf6,EA,ER,
                              0.0 consumo,0.0 perdita,0.0 potmax,'0' Is2G,tipo_flusso_num,tipo_flusso cod_flusso,
                              case max(tipo_flusso_num) when 1 THEN 'PNO'  when 2 then 'PNO2G'  when 3 then 'RNO'  when 4 then 'RNO2G'
                              when 5 then 'VNO' when 6 then 'VNO2G' when 7 then 'RNV' when 8 then 'RNV2G' else '' end tipo_flusso,
                              annomese,'0' is_mis_oraria
                              FROM
                              (
                              select *,max(dt_filter) over(partition by KEY_PART) max_dt_filter
                              from(
                              SELECT pod,annomese,lettura_monoraria,
                              eaf1,eaf2,eaf3,eaf4,eaf5,eaf6 ,tipo_flusso,
                              data_lettura,nvl(data_rilevazione,data_lettura) data_ricezione,
                              concat(data_lettura,nvl(data_rilevazione,data_lettura))dt_filter,tipo_flusso_num,
                              nvl(motivazione,'')motivazione,EA,ER,CONCAT(data_lettura,pod,tipo_flusso_num)KEY_PART
                              from tbl_misure_no_orarie_cloudera_2 where tipo_flusso = 'VNO_RNV'
                              UNION ALL
                              SELECT prt_tmo_mv_f.codice_pod pod,annomese, nvl(eam,0.0) lettura_monoraria,
                              nvl(eaf1,0.0)eaf1, nvl(eaf2,0.0)eaf2, nvl(eaf3,0.0)eaf3,0.0 eaf4,0.0 eaf5,0.0 eaf6,'VNO_RNV' tipo_flusso,
                              data_voltura data_lettura,nvl(d_rilevazione,data_voltura) data_ricezione,
                              concat(data_voltura,nvl(d_rilevazione,data_voltura))dt_filter,tipo_flusso_num,
                              '' motivazione,'' EA, '' ER, CONCAT(data_voltura,prt_tmo_mv_f.codice_pod,tipo_flusso_num)KEY_PART
                              from (select *,case cod_flusso when 'VNO' THEN 5 when 'RNV' then 7 else 0 end tipo_flusso_num from misure.prt_tmo_mv_f where annomese =${annomese}) prt_tmo_mv_f
                              LEFT OUTER JOIN ${tblpodorari} as pods
                              on pods.codice_pod = prt_tmo_mv_f.codice_pod
                              where pods.codice_pod is null
                              ) as X
                              ) AS TBL
                              WHERE dt_filter = max_dt_filter
                              GROUP BY pod,data_lettura,data_ricezione,motivazione,tipo_flusso_num,tipo_flusso,annomese,EA,ER
                              """

    log.info(s"""Estrazione volture oracle/cloudera """)

    val dtmisure_no_orarie_vno_all= hiveCtx.sql(strmisure_norarie_vno_all)

    val dtmisure_no_orarie_all = dtmisure_no_orarie_novno_all.unionAll(dtmisure_no_orarie_vno_all)

    dtmisure_no_orarie_all.registerTempTable("tbl_all_nora")



    log.info(s"Scrittura misure non orarie su tabella  misure.misure_storic per anno =${anno} e mese=${mese}")

    val q_nora_forn=
      """
        SELECT pod,data_lettura,data_ricezione,motivazione,
        eam,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,EA,ER,
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
        annomese,is_mis_oraria
        FROM tbl_all_nora  TBL_STOR

      """.stripMargin

    hiveCtx.sql(q_nora_forn)
      .write
      .format("parquet")
      .partitionBy("annomese","is_mis_oraria")
      .mode(SaveMode.Append)
      .save("/acquirente_unico/misure/misure_storic")

    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_storic")
    hiveCtx.refreshTable(s"misure.misure_storic")


    //hiveCtx.dropTempTable("tmp_tblx")
    //dtx.unpersist()

    hiveCtx.dropTempTable("tbl_noaggr")
    dt_noaggr.unpersist()

    hiveCtx.dropTempTable("tbl_misure_no_orarie_cloudera_base_storic")
    dtmisure_no_orarie_cloudera_base.unpersist()

    hiveCtx.dropTempTable("tbl_misure_no_orarie_cloudera_2")
    dtmisure_no_orarie_cloudera.unpersist()

    hiveCtx.dropTempTable("tbl_all_nora")

    //scrittura su tabella misure.misure_non_orarie_base

    log.info("Avvio estrazione e scrittura misure su tabella misure.misure_non_orarie_base")

    val q_drop_parts:String=s"ALTER TABLE misure.misure_non_orarie_base DROP IF EXISTS PARTITION(annomese=${annomese})"
    hiveCtx.sql(q_drop_parts)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_non_orarie_base")

    //la query seguente predilige prima l'ultima misura pervenuta nel mese per pod con distinzione tra pno e vno
    // e a parita di annomesegiorno ,pod le rettifiche
    val str_misure_nora_base=
      s"""
        INSERT INTO  misure.misure_non_orarie_base PARTITION(annomese)
        SELECT pod,giorno,lettura_monoraria,
        eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6,'E' tipo_dato,
        cod_flusso tipo_flusso,tipo_flusso tipo_flusso2,annomese
        from
        (
        SELECT pod,CAST(SUBSTR(data_lettura,7,2) AS INT)giorno,eam lettura_monoraria,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,cod_flusso ,
        tipo_flusso ,max(CONCAT(data_lettura,data_ricezione,tipo_flusso_num)) over(partition by annomese,pod,cod_flusso)max_dt_lett_ric ,tipo_flusso_num,annomese,
        data_lettura ,CONCAT(data_lettura,data_ricezione,tipo_flusso_num) dt_lett_ric
        from misure.misure_storic where  annomese = ${annomese} and is_mis_oraria='0' and motivazione <> '3'
        ) as tbl where dt_lett_ric = max_dt_lett_ric
      """.stripMargin

    /*
            SELECT pod,giorno,lettura_monoraria,
        eaf1, eaf2, eaf3 , eaf4, eaf5 ,eaf6,tipo_dato,tipo_flusso,tipo_flusso2,annomese FROM (
        SELECT pod,giorno,lettura_monoraria,
        eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6,'E' tipo_dato,
        cod_flusso tipo_flusso,tipo_flusso tipo_flusso2,annomese,tipo_flusso_num,
        max(tipo_flusso_num) over(partition by data_lettura,pod,cod_flusso)max_tipo_flusso_num
        from
        (
        SELECT pod,CAST(SUBSTR(data_lettura,7,2) AS INT)giorno,eam lettura_monoraria,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,cod_flusso ,
        tipo_flusso ,max(CONCAT(data_lettura,data_ricezione)) over(partition by annomese,pod,cod_flusso)max_dt_lett_ric ,tipo_flusso_num,annomese,
        data_lettura ,CONCAT(data_lettura,data_ricezione) dt_lett_ric from misure.misure_storic where  annomese = ${annomese} and is_mis_oraria='0' and motivazione <> '3'
        ) as tbl where dt_lett_ric = max_dt_lett_ric
        )  TT2 WHERE tipo_flusso_num = max_tipo_flusso_num
     */

    //la query seguente predilige prima il tipo di flusso di rettifica con distinzione tra pno e vno e poi prende la misura massima
    //quindi protrebbe succedere che un PNO arrivato dopo un RNO non venga considerato
    /*
    INSERT INTO  misure.misure_non_orarie_base PARTITION(annomese)
        SELECT pod,max(giorno)giorno,max(lettura_monoraria)lettura_monoraria,
        max(eaf1)eaf1, max(eaf2)eaf2, max(eaf3)eaf3 , max(eaf4)eaf4, max(eaf5)eaf5 , max(eaf6)eaf6,'E' tipo_dato,
        tipo_flusso,tipo_flusso2,annomese
        from
        (
        SELECT pod,CAST(SUBSTR(data_lettura,7,2) AS INT)giorno,eam lettura_monoraria,eaf1, eaf2, eaf3 , eaf4, eaf5 , eaf6 ,cod_flusso tipo_flusso ,
        tipo_flusso tipo_flusso2 ,max(tipo_flusso_num) over(partition by pod,annomese,cod_flusso)max_tipo_flusso_num ,tipo_flusso_num,annomese
        from misure.misure_storic where  annomese = ${annomese} and is_mis_oraria='0' and motivazione <> '3'
        ) as tbl where tipo_flusso_num = max_tipo_flusso_num
        group by pod,tipo_flusso,tipo_flusso2,annomese
     */
    hiveCtx.sql(str_misure_nora_base)


  }

  def calcMisure_Orarie(d_max_mese:String ,d_min_mese:String,anno:Int,mese:Int): Unit ={

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)
    log.info(s"Estrazione misure orarie per il periodo : ${annomese}" )

/*
 LA CHIAVE DI FILTRO A.time_stamp,A.dataelaborazione,NVL(A.cifrerea,0)
 CONTIENE PURE CIFREREA PER I FLUSSI 2G NEL CASO DI MISURE PRESENTI ALL'INTERNO DELLO STESSO FILE
 PREDENDO L'ULTIMA MISURA PRESENTE NEL FILE INGERITO
 ESEMPIO POD
 select cifrerea,areaquarti,potmax,podquarti,pivautentequarti,nomefile,annomesegiornodir,dataelaborazione ,time_stamp ,
 trattamento_o,tipodato_e,tipodato_s,tensione
 from au.FLUSSO_MISURE_QUARTI_BCK_OK20190306 where annoquarti=2018 and mesequarti =11 and giornoquarti=19
 and podquarti ='IT009E00000746'
 */
    val tblmisure=if(anno==2018 && mese <=7)tbl_misurequartiDaOracle else tbl_misurequarti

    //misure orarie 1G + (2G con fasce) da considerare come orarie
    /*
    ( SELECT codice_pod FROM mongodbs.gdm
          WHERE t_tipo_misuratore <> 'G' AND (nvl(d_inst_misurator_att,0)=0 or CAST(SUBSTR(d_inst_misurator_att,1,6) AS INT) <= ${annomese} )
        )gdm
     */


    val strpod_orari=
      s"""
        SELECT DISTINCT codice_pod FROM (
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN ( SELECT codice_pod FROM tbl_gdm WHERE annomese <= ${annomese} ) gdm
        ON p_ora.codice_pod=gdm.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        UNION ALL
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN
        ( SELECT SUBSTR(podquarti,1,14) podquarti FROM au.flusso_misure_estensione_quarti
          WHERE CONCAT(annoquarti,LPAD(mesequarti,2,0)) = ${annomese}
         ) quarti_ext
        on podquarti=p_ora.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        ) AS TBL

      """.stripMargin
    val dt_pod_orari=hiveCtx.sql(strpod_orari).persist(StorageLevels.MEMORY_ONLY_SER)
    val tblpodorari="list_pod_orari"
    dt_pod_orari.registerTempTable(tblpodorari)

    //CAST(concat(A.annoquarti,LPAD(A.mesequarti,2,0)) AS INT) annomese,
    val strtmp_table =
      s"""
        SELECT (A.e1+A.e2 + A.e3 + A.e4 + A.e5 + A.e6 + A.e7 + A.e8 + A.e9 + A.e10 + A.e11 + A.e12 + A.e13 + A.e14 + A.e15 + A.e16 + A.e17 + A.e18 + A.e19 + A.e20 +
        A.e21 + A.e22 + A.e23 + A.e24 + A.e25 + A.e26 + A.e27 + A.e28 + A.e29 + A.e30 + A.e31 + A.e32 + A.e33 + A.e34 + A.e35 + A.e36 + A.e37 + A.e38 +
        A.e39 + A.e40 + A.e41 + A.e42 + A.e43 + A.e44 + A.e45 + A.e46 + A.e47 + A.e48 + A.e49 + A.e50 + A.e51 + A.e52 + A.e53 + A.e54 + A.e55 + A.e56 +
        A.e57 + A.e58 + A.e59 + A.e60 + A.e61 + A.e62 + A.e63 + A.e64 + A.e65 + A.e66 + A.e67 + A.e68 + A.e69 + A.e70 + A.e71 + A.e72 + A.e73 + A.e74 +
        A.e75 + A.e76 + A.e77 + A.e78 + A.e79 + A.e80 + A.e81 + A.e82 + A.e83 + A.e84 + A.e85 + A.e86 + A.e87 + A.e88 + A.e89 + A.e90 + A.e91 + A.e92 +
        A.e93 + A.e94 + A.e95 + A.e96 + A.e97 + A.e98 + A.e99 + A.e100)consumo , A.nomefile,A.dataelaborazione,A.cifrerea,
        A.annoquarti,A.mesequarti,A.giornoquarti,A.podquarti,A.perdita,A.areaquarti,A.potmax,
        ${annomese} annomese,
        CAST(concat('${annomese}',LPAD(A.giornoquarti,2,0)) AS BIGINT) annomesegiorno,
        CONCAT(A.time_stamp,unix_timestamp(A.dataelaborazione)) times_elab,
        case (split(A.nomefile,"_")[3]) when 'PDO' THEN 1 when 'PDO2G' then 2 when 'RFO' then 3 when 'RFO2G' then 4 else 0 END tipo_flusso_num
        FROM ${tblmisure} A
        INNER JOIN ${tblpodorari} as pods
        on pods.codice_pod = A.podquarti
        WHERE  A.annoquarti = ${anno} AND A.mesequarti =${mese} AND VALIDATO ='S' AND  tipodato_e = 1
      """.stripMargin


      var tbl_base ="tbl_misure_quarti_1"
      val dtmisure_o_quarti_1= hiveCtx.sql(strtmp_table).persist(StorageLevels.MEMORY_ONLY_SER)
      dtmisure_o_quarti_1.registerTempTable(tbl_base)

      //var tbl_base =s"( ${strtmp_table} )"

      /*
       LA CONDIZIONE
       AND substr(A.areaquarti,1,6)='NEW_F_'
       MI FILTRA SOLO LE MISURE 2G ORARIE AVENTI LE FASCIE
       */

    //RACCOLTA NOT IN ('S','V','T')



    val str_misure_o_quarti=
      s"""
        SELECT consumo,nomefile,dataelaborazione,cifrerea,
        annoquarti,mesequarti,giornoquarti,podquarti,perdita,areaquarti,potmax,
        annomese,annomesegiorno,tipo_flusso_num,
        CONCAT(annomese,podquarti,cifrerea,nomefile,dataelaborazione) KEY_QUARTI FROM
        (SELECT MAX(consumo)consumo,nomefile,dataelaborazione,MAX(cifrerea)cifrerea,
        annoquarti,mesequarti,giornoquarti,podquarti,max(perdita)perdita,areaquarti,MAX(potmax)potmax,
        annomese,annomesegiorno,tipo_flusso_num FROM (
        SELECT *,
        max(A.times_elab) over ( partition by A.annomesegiorno,A.podquarti)max_times_elab
        FROM ${tbl_base} A
        ) XX WHERE times_elab = max_times_elab
        group by nomefile,dataelaborazione,
        annoquarti,mesequarti,giornoquarti,podquarti,areaquarti,
        annomese,annomesegiorno,tipo_flusso_num
        ) as tbl_quarti  DISTRIBUTE BY KEY_QUARTI
      """.stripMargin




    val q_drop_part:String=s"ALTER TABLE misure.misure_orarie_base DROP IF EXISTS PARTITION(annomese=${annomese})"
    hiveCtx.sql(q_drop_part)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_orarie_base")

    log.info(s"""Estrazione quarti da ${tblmisure} dove A.annoquarti = ${anno} AND A.mesequarti =${mese} AND VALIDATO ='S' AND  tipodato_e = 1 """)

    val dtmisure_o_quarti= hiveCtx.sql(str_misure_o_quarti).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_o_quarti.registerTempTable("tbl_misure_quarti")

    dtmisure_o_quarti_1.unpersist()
    hiveCtx.dropTempTable(tbl_base)

    val str_misure_o_ext_quarti=
      s"""

        SELECT eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         podquarti,nomefile,dataelaborazione ,progr_podsez ,annomese,
         CONCAT(annomese,podquarti,progr_podsez,nomefile,dataelaborazione)KEY_QUARTI,motivazione
        FROM
        (SELECT B.eaf1  , B.eaf2  , B.eaf3  , B.eaf4  , B.eaf5  , B.eaf6,
        B.podquarti,B.nomefile,B.dataelaborazione ,B.progr_podsez ,
        CAST(CONCAT(B.annoquarti,LPAD(B.mesequarti,2,0)) AS INT) annomese,motivazione
        FROM
         (
              SELECT eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
              podquarti,nomefile,dataelaborazione ,MAX(progr_podsez)progr_podsez , annoquarti , mesequarti,
              NVL(motivazione,'')motivazione
              FROM au.flusso_misure_estensione_quarti
              where annoquarti =${anno} AND mesequarti =${mese}
              GROUP BY eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
              podquarti,nomefile,dataelaborazione ,annoquarti,mesequarti,tipo_flusso,motivazione
         )B
         )tbl_extquarti DISTRIBUTE BY KEY_QUARTI


      """.stripMargin


    log.info(s"""Estrazione estensione_quarti B.annoquarti =${anno} AND B.mesequarti =${mese} """)


    val dtmisure_o_ext_quarti= hiveCtx.sql(str_misure_o_ext_quarti).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_o_ext_quarti.registerTempTable("tbl_misure_ext_quarti")


//'PDO_RFO' tipo_flusso
    val str_misure_orarie=
      s"""
         SELECT DISTINCT podquarti,giornoquarti,annoquarti ,mesequarti,annomesegiorno,
         consumo,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6 ,perdita,potmax,Is2G,
         CASE tipo_flusso_num when 1 then 'PDO' when 2 then'PDO2G' when 3  then 'RFO' when 4 then 'RFO2G' else ''  END tipo_flusso,
         annomese,'0' da_no_ora
         from(
         SELECT QUARTI.consumo,SUBSTR(QUARTI.podquarti,1,14)podquarti,QUARTI.giornoquarti,QUARTI.annoquarti ,QUARTI.mesequarti,
         QUARTI.perdita,QUARTI.annomese,QUARTI.annomesegiorno,QUARTI.potmax,
         nvl(EXT_QUARTI.eaf1,0)eaf1  , nvl(EXT_QUARTI.eaf2,0)eaf2  , nvl(EXT_QUARTI.eaf3,0)eaf3  ,
         nvl(EXT_QUARTI.eaf4,0)eaf4 , nvl(EXT_QUARTI.eaf5,0)eaf5  , nvl(EXT_QUARTI.eaf6,0)eaf6,
         case when nvl(EXT_QUARTI.KEY_QUARTI,'')='' then '0' else '1' END Is2G,QUARTI.tipo_flusso_num,
         max(QUARTI.tipo_flusso_num) over ( partition by QUARTI.annomesegiorno,QUARTI.podquarti)max_tipo_flusso_num
         FROM tbl_misure_quarti QUARTI
         LEFT OUTER JOIN tbl_misure_ext_quarti EXT_QUARTI
         ON substr(QUARTI.areaquarti,1,6)='NEW_F_' AND QUARTI.KEY_QUARTI = EXT_QUARTI.KEY_QUARTI
         WHERE  nvl(EXT_QUARTI.motivazione,'') NOT IN('3')
         ) as TBL where tipo_flusso_num = max_tipo_flusso_num
      """.stripMargin

    //LEFT OUTER JOIN tbl_misure_ext_quarti
    log.info("Join quarti + estensione quarti")
    val dtmisure_o_tmp= hiveCtx.sql(str_misure_orarie)

    /*
    DROP TABLE misure.misure_orarie_base;
    create table misure.misure_orarie_base(podquarti STRING,giornoquarti INT,annoquarti INT ,mesequarti INT ,annomese INT ,annomesegiorno INT,
         consumo DOUBLE,eaf1 DOUBLE , eaf2 DOUBLE , eaf3 DOUBLE , eaf4 DOUBLE , eaf5 DOUBLE , eaf6 DOUBLE,perdita DOUBLE,potmax DOUBLE,
		 tipodato_e INT,tipodato_s INT,tipodato_a INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION '/acquirente_unico/misure/misure_orarie_base';
    */

    log.info(s"Scrittura estrazioni su tabella  misure.misure_orarie_base per anno =${anno} e mese=${mese}")



    dtmisure_o_tmp
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("annomese","da_no_ora")
      .save("/acquirente_unico/misure/misure_orarie_base")


    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_orarie_base")
    hiveCtx.refreshTable(s"misure.misure_orarie_base")

    hiveCtx.dropTempTable(tblpodorari)
    hiveCtx.dropTempTable("tbl_misure_quarti")
    hiveCtx.dropTempTable("tbl_misure_ext_quarti")
    dt_pod_orari.unpersist()
    dtmisure_o_quarti.unpersist()
    dtmisure_o_ext_quarti.unpersist()



    //hiveCtx.sql("TRUNCATE TABLE misure.misurequarti_base")

  }


  def calcMisure_NonOrarie(anno:Int,mese:Int): Unit ={

    log.info(s"Estrazione misure non orarie per il mese ${mese} e anno ${anno}")

    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    //misure orarie 1G + (2G con fasce) da escludere dalle non orarie
    val strpod_orari=
      s"""
        SELECT DISTINCT codice_pod FROM (
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN ( SELECT codice_pod FROM tbl_gdm WHERE annomese <= ${annomese} ) gdm
        ON p_ora.codice_pod=gdm.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        UNION ALL
        SELECT p_ora.codice_pod
        from misure.pods_orari p_ora
        INNER JOIN
        ( SELECT podquarti FROM au.flusso_misure_estensione_quarti
          WHERE CONCAT(annoquarti,LPAD(mesequarti,2,0)) = ${annomese}
         ) quarti_ext
        on podquarti=p_ora.codice_pod
        WHERE annomese =${annomese}  and is_orario='1'
        ) AS TBL

      """.stripMargin
    val dt_pod_orari=hiveCtx.sql(strpod_orari).persist(StorageLevels.MEMORY_ONLY_SER)
    val tblpodorari="list_pod_orari"
    dt_pod_orari.registerTempTable(tblpodorari)


    //data_lettura BIGINT,data_ricezione BIGINT,cod_flusso STRING,motivazione STRING

    //estraggo pure i vno/rnv
    val where1G=if(read1G_no_orarie) "((tipo_flusso in('PNO','PNO2G','VNO','VNO2G') AND tipodato_e=1) OR tipo_flusso IN ('RNO','RNO2G','RNV','RNV2G'))" else "((tipo_flusso in('PNO2G','VNO2G') AND tipodato_e=1) OR tipo_flusso IN ('RNO2G','RNV2G'))"

    val strbase_misureno=
      s"""
         SELECT dataelaborazione,anno,mese,SUBSTR(tipo_flusso,1,3)tipo_flusso,pod, eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,
          CAST(concat(anno,LPAD(mese,2,0)) AS INT) annomese,
          CAST( (CASE WHEN nvl(giorno,0)<>0 THEN giorno WHEN SUBSTR(tipo_flusso,1,3) ='VNO' OR SUBSTR(tipo_flusso,1,3) ='RNV' THEN substr(data_voltura,1,2) ELSE substr(data_misura,1,2) END) AS INT) giorno,
          time_stamp,data_voltura,data_misura,data_rilevazione,
          EaM,annomesegiornodir, 'E' tipo_dato,
          case tipo_flusso when 'PNO' THEN 1 when 'PNO2G' then 2 when 'RNO' then 3 when 'RNO2G' then 4
          when 'VNO' THEN 5 when 'VNO2G' then 6 when 'RNV' then 7 when 'RNV2G' then 8
          else 0 END tipo_flusso_num
          from au.flusso_misure_noaggr
          where  anno = ${anno} AND mese =${mese} and NVL(motivazione,'') NOT IN('3') AND (VALIDATO ='S' AND ${where1G} )
      """.stripMargin

    log.info(s"""Estrazione misure non orarie da au.flusso_misure_noaggr A.anno = ${anno} AND A.mese =${mese} AND VALIDATO ='S'
                AND ${where1G} """)

    val dtmisure_no_orarie_cloudera_base= hiveCtx.sql(strbase_misureno).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_no_orarie_cloudera_base.registerTempTable("tbl_misure_no_orarie_cloudera_base")

    val str_misure_norarie_cloudera=
      s"""
        SELECT pod, giorno,annomese,annomesegiorno,lettura_monoraria,
		    eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,tipo_dato,tipo_flusso,
		    data_lettura,data_ricezione,tipo_flusso_num,data_rilevazione
        FROM (
        SELECT lettura_monoraria ,A.dataelaborazione,A.anno,A.mese,giorno,A.pod,
		    tipo_dato ,annomese,
        nvl(eaf1,0)eaf1,nvl(eaf2,0)eaf2,nvl(eaf3,0)eaf3,
		    nvl(eaf4,0)eaf4,nvl(eaf5,0)eaf5,nvl(eaf6,0)eaf6,
		    tipo_flusso,annomesegiorno data_lettura, times_elab, data_ricezione,
        max(A.times_elab) over ( partition by A.annomesegiorno,A.pod,A.tipo_flusso) max_times_elab,
        max(A.annomesegiorno) over ( partition by A.annomese,A.pod,A.tipo_flusso) max_annomesegiorno,
        CAST(CONCAT(SUBSTR(data_lettura_str,7,4),SUBSTR(data_lettura_str,4,2),SUBSTR(data_lettura_str,1,2)) AS BIGINT)data_lettura,
        CAST(CONCAT(SUBSTR(data_rilevazione_str,7,4),SUBSTR(data_rilevazione_str,4,2),SUBSTR(data_rilevazione_str,1,2)) AS BIGINT)data_rilevazione,
        tipo_flusso_num
        FROM
        (
         SELECT nvl(NO_BASE.EaM,0) as lettura_monoraria ,
         NO_BASE.dataelaborazione,NO_BASE.anno,NO_BASE.mese,giorno,
         NO_BASE.pod,tipo_dato ,annomese,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,CAST(NO_BASE.annomesegiornodir AS BIGINT) as data_ricezione,
		     CASE WHEN tipo_flusso ='VNO' OR tipo_flusso ='RNV' THEN 'VNO_RNV' ELSE 'PNO_RNO' END tipo_flusso,
         CAST(concat(annomese,giorno) AS BIGINT) annomesegiorno,
         CONCAT(NO_BASE.time_stamp,unix_timestamp(NO_BASE.dataelaborazione)) times_elab ,
         CASE WHEN tipo_flusso ='VNO' OR tipo_flusso ='RNV' THEN nvl(data_voltura,data_misura) ELSE data_misura END data_lettura_str,
         data_rilevazione data_rilevazione_str,tipo_flusso_num
         FROM tbl_misure_no_orarie_cloudera_base NO_BASE
         LEFT OUTER JOIN ${tblpodorari} as pods
         on pods.codice_pod = NO_BASE.pod
         WHERE pods.codice_pod is null
         ) AS A
        ) XX WHERE CONCAT(times_elab,'-',annomesegiorno) =  CONCAT(max_times_elab,'-',max_annomesegiorno)

      """.stripMargin





    val dtmisure_no_orarie_cloudera= hiveCtx.sql(str_misure_norarie_cloudera).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmisure_no_orarie_cloudera.registerTempTable("tbl_misure_no_orarie_cloudera_2")



    val q_drop_part:String=s"ALTER TABLE misure.misure_non_orarie_base DROP IF EXISTS PARTITION(annomese=${annomese})"
    hiveCtx.sql(q_drop_part)
    hiveCtx.sql(s"MSCK REPAIR TABLE misure.misure_non_orarie_base")

    /*
    SELECT pod,max(giorno)giorno,max(lettura_monoraria)lettura_monoraria,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,max(eaf6)eaf6,
                              tipo_dato,tipo_flusso,max(data_lettura)data_lettura,data_ricezione,cod_flusso,motivazione,annomese
                              FROM
                              (
                              )
    GROUP BY pod,tipo_dato,tipo_flusso,data_ricezione,cod_flusso,motivazione,annomese
     */
    val strmisure_norarie_novno_all=s"""
                              SELECT pod,max(giorno)giorno,max(lettura_monoraria)lettura_monoraria,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,max(eaf6)eaf6,
                              tipo_dato,tipo_flusso,
                              case max(tipo_flusso_num) when 1 THEN 'PNO'  when 2 then 'PNO2G'  when 3 then 'RNO'  when 4 then 'RNO2G'
                              when 5 then 'VNO' when 6 then 'VNO2G' when 7 then 'RNV' when 8 then 'RNV2G' else '' end tipo_flusso2,
                              annomese
                              FROM
                              (
                              select *,max(dt_filter) over(partition by pod,annomese) max_dt_filter
                              from(
                              SELECT pod, giorno,annomese,lettura_monoraria,
                              eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,tipo_dato,tipo_flusso,
                              data_lettura,data_ricezione,
                              concat(data_lettura,data_ricezione,'\b',tipo_flusso_num)dt_filter,tipo_flusso_num
                              from tbl_misure_no_orarie_cloudera_2 where tipo_flusso <> 'VNO_RNV'
                              UNION ALL
                              SELECT prt_tmo_mn_f.codice_pod pod,giornomisura giorno,annomese, nvl(eam,0.0) lettura_monoraria,
                              eaf1,eaf2,eaf3,0.0 eaf4,0.0 eaf5,0.0 eaf6,tipodato tipo_dato,'PNO_RNO' tipo_flusso,
                              data_lettura,data_ricezione,
                              concat(data_lettura,data_ricezione,'\b',tipo_flusso_num)dt_filter,tipo_flusso_num
                              from (select *,case cod_flusso when 'PNO' THEN 1 when 'RNO' then 3 else 0 end tipo_flusso_num from misure.prt_tmo_mn_f where annomese =${annomese}) prt_tmo_mn_f
                              LEFT OUTER JOIN  ${tblpodorari} as pods
                              on pods.codice_pod = prt_tmo_mn_f.codice_pod
                              where  pods.codice_pod is null
                              ) as X
                              ) AS TBL
                              WHERE dt_filter = max_dt_filter
                              GROUP BY pod,tipo_dato,tipo_flusso,annomese
                              """

    log.info(s"""Estrazione misure non orarie oracle/cloudera """)

    val dtmisure_no_orarie_novno_all= hiveCtx.sql(strmisure_norarie_novno_all)//.persist(StorageLevels.MEMORY_ONLY_SER)

    /*
     SELECT pod,max(giorno)giorno,max(lettura_monoraria)lettura_monoraria,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,
                              max(eaf6)eaf6, tipo_dato,tipo_flusso,data_lettura,data_ricezione,cod_flusso,motivazione,annomese
                              FROM
                              (
                              )
     GROUP BY pod,tipo_dato,tipo_flusso,data_lettura,data_ricezione,cod_flusso,motivazione,annomese
     */
//la data_lettura corrisponde dalla data_voltura


    val strmisure_norarie_vno_all=s"""
                              SELECT pod,max(giorno)giorno,max(lettura_monoraria)lettura_monoraria,
                              max(eaf1)eaf1,max(eaf2)eaf2,max(eaf3)eaf3,max(eaf4)eaf4,max(eaf5)eaf5,max(eaf6)eaf6,
                              tipo_dato,tipo_flusso,
                              case max(tipo_flusso_num) when 1 THEN 'PNO'  when 2 then 'PNO2G'  when 3 then 'RNO'  when 4 then 'RNO2G'
                              when 5 then 'VNO' when 6 then 'VNO2G' when 7 then 'RNV' when 8 then 'RNV2G' else '' end tipo_flusso2,
                              annomese
                              FROM
                              (
                              select *,max(dt_filter) over(partition by pod,annomese) max_dt_filter
                              from(
                              SELECT pod, giorno,annomese,lettura_monoraria,
                              eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,tipo_dato ,tipo_flusso,
                              data_lettura,nvl(data_rilevazione,data_lettura) data_ricezione,
                              concat(data_lettura,nvl(data_rilevazione,data_lettura),'\b',tipo_flusso_num)dt_filter,tipo_flusso_num
                              from tbl_misure_no_orarie_cloudera_2 where tipo_flusso = 'VNO_RNV'
                              UNION ALL
                              SELECT prt_tmo_mv_f.codice_pod pod,giorno_voltura giorno,annomese, nvl(eam,0.0) lettura_monoraria,
                              eaf1,eaf2,eaf3,0.0 eaf4,0.0 eaf5,0.0 eaf6,tipodato tipo_dato,'VNO_RNV' tipo_flusso,
                              data_voltura data_lettura,nvl(d_rilevazione,data_voltura) data_ricezione,
                              concat(data_voltura,nvl(d_rilevazione,data_voltura),'\b',tipo_flusso_num)dt_filter,tipo_flusso_num
                              from (select *,case cod_flusso when 'VNO' THEN 5 when 'RNV' then 7 else 0 end tipo_flusso_num from misure.prt_tmo_mv_f where annomese =${annomese}) prt_tmo_mv_f
                              LEFT OUTER JOIN ${tblpodorari} as pods
                              on pods.codice_pod = prt_tmo_mv_f.codice_pod
                              where pods.codice_pod is null
                              ) as X
                              ) AS TBL
                              WHERE dt_filter = max_dt_filter
                              GROUP BY pod,tipo_dato,tipo_flusso,annomese
                              """

    log.info(s"""Estrazione volture oracle/cloudera """)

    val dtmisure_no_orarie_vno_all= hiveCtx.sql(strmisure_norarie_vno_all)

    log.info("Scrittura PNO/RNO - VNO/RNV su tabella  misure.misure_non_orarie_base")

    val dtmisure_no_orarie_all = dtmisure_no_orarie_novno_all.unionAll(dtmisure_no_orarie_vno_all)

    dtmisure_no_orarie_all
      .write
      .format("parquet")
      .partitionBy("annomese")
      .mode(SaveMode.Append)
      .save("/acquirente_unico/misure/misure_non_orarie_base")

    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_non_orarie_base")
    hiveCtx.refreshTable(s"misure.misure_non_orarie_base")

    hiveCtx.dropTempTable(tblpodorari)
    dt_pod_orari.unpersist()

    hiveCtx.dropTempTable("tbl_misure_no_orarie_cloudera_base")
    dtmisure_no_orarie_cloudera_base.unpersist()

    hiveCtx.dropTempTable("tbl_misure_no_orarie_cloudera_2")
    dtmisure_no_orarie_cloudera.unpersist()


    //dtmisure_o_ext_quarti.unpersist()


  }


  def calMisureOrarieGG_Mese_Delta(d_max:String,d_min:String): Unit ={



    // PULIZIA TABELLA misure_orarie_c
    hiveCtx.sql("TRUNCATE TABLE misure.misure_orarie_c")
    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_orarie_c");
    hiveCtx.refreshTable(s"misure.misure_orarie_c")


    // PULIZIA TABELLA misure_mensili_c
    hiveCtx.sql("TRUNCATE TABLE misure.misure_mensili_c")
    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_mensili_c")
    hiveCtx.refreshTable(s"misure.misure_mensili_c")

    log.info(s"Avvio calcolo delta consumi giornalieri , fasce + incrocio con forniture per il periodo ${d_min} - ${d_max}")


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
              LAG(CONCAT(M.consumo,'\b',M.eaf1,'\b',M.eaf2,'\b',M.eaf3,'\b',M.eaf4,'\b',M.eaf5,'\b',M.eaf6))
              over (partition by CONCAT(M.cf_piva,M.podquarti) order by M.annomesegiorno) prev_dati,
              tipo_flusso,da_no_ora
              FROM (
               SELECT cf_piva,n_id_fornitura,MO.podquarti,MO.giornoquarti,MO.annomese,MO.annomesegiorno,
                MO.consumo,MO.eaf1  , MO.eaf2  , MO.eaf3  , MO.eaf4  , MO.eaf5  , MO.eaf6 ,MO.perdita,MO.potmax,
                MO.Is2G is2g,MO.tipo_flusso,MO.da_no_ora
                FROM misure.misure_orarie_base MO
                INNER JOIN tbl_forniture F ON MO.podquarti = F.codice_pod
                WHERE (MO.annomesegiorno >= CAST(F.inizio AS BIGINT) AND MO.annomesegiorno <= CAST(F.fine AS BIGINT))
               )M
              ) TBL_SPLITTED
             )TBL_F
         """.stripMargin

    val dtx =hiveCtx.sql(tmp).persist(StorageLevels.MEMORY_ONLY_SER)
    dtx.registerTempTable("tbl_orarie_tmp")


    val str_misure_orarie_delta=
      s"""
          INSERT INTO misure.misure_orarie_c
          SELECT cf_piva,n_id_fornitura, pod, giorno,tipo_misura,potenza_max_erogata,lettura_giornaliero_f1,
          lettura_giornaliero_f2,lettura_giornaliero_f3,lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,
          delta_misure_f1,delta_misure_f2,delta_misure_f3,delta_misure_f4,delta_misure_f5,delta_misure_f6,consumo_giornaliero_gg,
          is2g,competenza_consumi,tipo_flusso,data_lettura
          FROM
          (
           SELECT cf_piva,n_id_fornitura,podquarti pod,giornoquarti giorno,'E' tipo_misura,
           round(potmax,2) potenza_max_erogata,round(eaf1,2) lettura_giornaliero_f1 ,round(eaf2,2) lettura_giornaliero_f2,
           round(eaf3,2) lettura_giornaliero_f3,round(eaf4,2) lettura_giornaliero_f4 ,round(eaf5,2) lettura_giornaliero_f5,
           round(eaf6,2) lettura_giornaliero_f6,
		       case when cast(nvl(prev_eaf1,0) as int) = 0 or prev_eaf1 > eaf1 then 0 else round((eaf1-prev_eaf1),2) end delta_misure_f1,
		       case when cast(nvl(prev_eaf2,0) as int) = 0 or prev_eaf2 > eaf2 then 0 else round((eaf2-prev_eaf2),2) end delta_misure_f2,
		       case when cast(nvl(prev_eaf3,0) as int) = 0 or prev_eaf3 > eaf3 then 0 else round((eaf3-prev_eaf3),2) end delta_misure_f3,
		       case when cast(nvl(prev_eaf4,0) as int) = 0 or prev_eaf4 > eaf4 then 0 else round((eaf4-prev_eaf4),2) end delta_misure_f4,
		       case when cast(nvl(prev_eaf5,0) as int) = 0 or prev_eaf5 > eaf5 then 0 else round((eaf5-prev_eaf5),2) end delta_misure_f5,
		       case when cast(nvl(prev_eaf6,0) as int) = 0 or prev_eaf6 > eaf6 then 0 else round((eaf6-prev_eaf6),2) end delta_misure_f6,
           case when round(consumo,2) < 0 or cast(nvl(prev_eaf1,0) as int) <> 0 then 0 else round(consumo,2) end consumo_giornaliero_gg ,is2g,
           annomese competenza_consumi,tipo_flusso,annomesegiorno data_lettura,da_no_ora  FROM tbl_orarie_tmp AS TBL
           ) AS TTX WHERE da_no_ora ='0'
      """.stripMargin


    val dtmisure_o_delta= hiveCtx.sql(str_misure_orarie_delta)
    log.info("Scrittura misure orarie  effettuata su tabella  misure.misure_orarie_c ")
    dtx.unpersist()
    hiveCtx.dropTempTable("tbl_orarie_tmp")


    /*dtmisure_o_delta
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .save("/acquirente_unico/misure/misure_orarie_c")*/

    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_orarie_c")
    hiveCtx.refreshTable(s"misure.misure_orarie_c")


    log.info(s"Estrazione misure orarie mensili  per il periodo ${d_min} - ${d_max}" )


     /*
     DA VERIFICARE LA MISURA MONORARIA CON DELTA PER LE MISURE ORARIE

      sum(consumo_giornaliero_gg) over(partition by n_id_fornitura,competenza_consumi)lettura_misura_monoraria,

     */




    val strorarie_mensili1=
      s"""
         select sum(CASE WHEN Is2G='0' THEN consumo_giornaliero_gg ELSE 0 END) delta_misura_monoraria,
         sum(consumo_giornaliero_gg)delta_misura_monoraria_2,
         sum(delta_misure_f1)delta_misure_f1,sum(delta_misure_f2)delta_misure_f2,sum(delta_misure_f3)delta_misure_f3,
         sum(delta_misure_f4)delta_misure_f4,sum(delta_misure_f5)delta_misure_f5,sum(delta_misure_f6)delta_misure_f6,
         concat(n_id_fornitura,pod,competenza_consumi)KK_JOIN
         from misure.misure_orarie_c where n_id_fornitura <> ''
         group by n_id_fornitura,pod,competenza_consumi
         DISTRIBUTE BY KK_JOIN
      """.stripMargin

    val dtmm1=hiveCtx.sql(strorarie_mensili1).persist(StorageLevels.MEMORY_ONLY_SER)
    dtmm1.registerTempTable("tbl_mm1")

    val strorarie_mensili =
      s"""
         INSERT INTO misure.misure_mensili_c
         select distinct cf_piva,n_id_fornitura,tipo_misura,lettura_misura_monoraria,
         CASE WHEN nvl(lettura_misura_f1,0.0)=0.0 AND nvl(lettura_misura_f2,0.0)=0.0 AND nvl(lettura_misura_f3,0.0)=0.0 AND nvl(lettura_misura_f4,0.0)=0.0 AND nvl(lettura_misura_f5,0.0)=0.0 AND nvl(lettura_misura_f6,0)=0.0  THEN
         round(delta_misura_monoraria_2,2) ELSE round(delta_misura_monoraria,2) END delta_misura_monoraria,
         lettura_misura_f1,lettura_misura_f2,lettura_misura_f3,lettura_misura_f4,lettura_misura_f5,lettura_misura_f6,
         round(delta_misure_f1,2)delta_misure_f1,round(delta_misure_f2,2)delta_misure_f2,round(delta_misure_f3,2)delta_misure_f3,
         round(delta_misure_f4,2)delta_misure_f4,round(delta_misure_f5,2)delta_misure_f5,round(delta_misure_f6,2)delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,data_lettura
         from
         (
         SELECT cf_piva,n_id_fornitura,tipo_misura,lettura_misura_monoraria,delta_misura_monoraria,delta_misura_monoraria_2,
         lettura_giornaliero_f1,lettura_giornaliero_f2,lettura_giornaliero_f3,lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,
         CAST((split(letture_misure_f,'\b')[0]) AS DOUBLE)lettura_misura_f1,
         CAST((split(letture_misure_f,'\b')[1]) AS DOUBLE)lettura_misura_f2,
         CAST((split(letture_misure_f,'\b')[2]) AS DOUBLE)lettura_misura_f3,
         CAST((split(letture_misure_f,'\b')[3]) AS DOUBLE)lettura_misura_f4,
         CAST((split(letture_misure_f,'\b')[4]) AS DOUBLE)lettura_misura_f5,
         CAST((split(letture_misure_f,'\b')[5]) AS DOUBLE)lettura_misura_f6,
         delta_misure_f1,delta_misure_f2,delta_misure_f3,delta_misure_f4,delta_misure_f5,delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,
         CAST((split(letture_misure_f,'\b')[6]) AS BIGINT)data_lettura
         FROM
         (
          select cf_piva,n_id_fornitura,competenza_consumi,tipo_misura,0 lettura_misura_monoraria,
          tbl_mm1.delta_misura_monoraria, tbl_mm1.delta_misura_monoraria_2,
          lettura_giornaliero_f1,lettura_giornaliero_f2,lettura_giornaliero_f3,lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,letture_misure_f,
          tbl_mm1.delta_misure_f1,tbl_mm1.delta_misure_f2,tbl_mm1.delta_misure_f3,tbl_mm1.delta_misure_f4,tbl_mm1.delta_misure_f5,tbl_mm1.delta_misure_f6,
          pod,tipo_flusso
          from
           (select *,concat(n_id_fornitura,pod,competenza_consumi)KK_JOIN,
            last_value(CONCAT(lettura_giornaliero_f1,'\b',lettura_giornaliero_f2,'\b',lettura_giornaliero_f3,'\b',lettura_giornaliero_f4,'\b',lettura_giornaliero_f5,'\b',lettura_giornaliero_f6,'\b',data_lettura))
            over(partition by concat(n_id_fornitura,pod,competenza_consumi) order by CAST(CONCAT(competenza_consumi,LPAD(giorno,2,0)) AS BIGINT) rows between unbounded preceding and unbounded following) letture_misure_f
            from misure.misure_orarie_c where n_id_fornitura <> '' DISTRIBUTE BY KK_JOIN) m_orari_c
           INNER JOIN tbl_mm1 ON m_orari_c.KK_JOIN = tbl_mm1.KK_JOIN
          )AS TMP_TBL
        )AS TBL
      """.stripMargin

    /*val strorarie_mensili =
      s"""
         INSERT INTO misure.misure_mensili_c
         select distinct cf_piva,n_id_fornitura,tipo_misura,lettura_misura_monoraria,
         CASE WHEN nvl(lettura_misura_f1,0.0)=0.0 AND nvl(lettura_misura_f2,0.0)=0.0 AND nvl(lettura_misura_f3,0.0)=0.0 AND nvl(lettura_misura_f4,0.0)=0.0 AND nvl(lettura_misura_f5,0.0)=0.0 AND nvl(lettura_misura_f6,0)=0.0  THEN
         round(delta_misura_monoraria_2,2) ELSE round(delta_misura_monoraria,2) END delta_misura_monoraria,
         lettura_misura_f1,lettura_misura_f2,lettura_misura_f3,lettura_misura_f4,lettura_misura_f5,lettura_misura_f6,
         round(delta_misure_f1,2)delta_misure_f1,round(delta_misure_f2,2)delta_misure_f2,round(delta_misure_f3,2)delta_misure_f3,
         round(delta_misure_f4,2)delta_misure_f4,round(delta_misure_f5,2)delta_misure_f5,round(delta_misure_f6,2)delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,data_lettura
         from
         (
         SELECT cf_piva,n_id_fornitura,tipo_misura,lettura_misura_monoraria,delta_misura_monoraria,delta_misura_monoraria_2,
         lettura_giornaliero_f1,lettura_giornaliero_f2,lettura_giornaliero_f3,lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,
         CAST((split(letture_misure_f,'\b')[0]) AS DOUBLE)lettura_misura_f1,
         CAST((split(letture_misure_f,'\b')[1]) AS DOUBLE)lettura_misura_f2,
         CAST((split(letture_misure_f,'\b')[2]) AS DOUBLE)lettura_misura_f3,
         CAST((split(letture_misure_f,'\b')[3]) AS DOUBLE)lettura_misura_f4,
         CAST((split(letture_misure_f,'\b')[4]) AS DOUBLE)lettura_misura_f5,
         CAST((split(letture_misure_f,'\b')[5]) AS DOUBLE)lettura_misura_f6,
         delta_misure_f1,delta_misure_f2,delta_misure_f3,delta_misure_f4,delta_misure_f5,delta_misure_f6,
         pod,competenza_consumi,tipo_flusso,
         CAST((split(letture_misure_f,'\b')[6]) AS BIGINT)data_lettura
         FROM
         (
          select cf_piva,n_id_fornitura,competenza_consumi,tipo_misura,0 lettura_misura_monoraria,
          sum(CASE WHEN Is2G='0' THEN consumo_giornaliero_gg ELSE 0 END) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misura_monoraria,
          sum(consumo_giornaliero_gg) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misura_monoraria_2,
          lettura_giornaliero_f1,lettura_giornaliero_f2,lettura_giornaliero_f3,lettura_giornaliero_f4,lettura_giornaliero_f5,lettura_giornaliero_f6,
          last_value(CONCAT(lettura_giornaliero_f1,'\b',lettura_giornaliero_f2,'\b',lettura_giornaliero_f3,'\b',lettura_giornaliero_f4,'\b',lettura_giornaliero_f5,'\b',lettura_giornaliero_f6,'\b',data_lettura))
          over(partition by concat(n_id_fornitura,pod,competenza_consumi) order by CAST(CONCAT(competenza_consumi,LPAD(giorno,2,0)) AS BIGINT) rows between unbounded preceding and unbounded following) letture_misure_f,
          sum(delta_misure_f1) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misure_f1,
          sum(delta_misure_f2) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misure_f2,
          sum(delta_misure_f3) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misure_f3,
          sum(delta_misure_f4) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misure_f4,
          sum(delta_misure_f5) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misure_f5,
          sum(delta_misure_f6) over(partition by concat(n_id_fornitura,pod,competenza_consumi))delta_misure_f6,
          pod,tipo_flusso
          from  misure.misure_orarie_c where n_id_fornitura <> ''
         )AS TMP_TBL
        )AS TBL
      """.stripMargin*/


    log.info(s"Calcolo Delta misure orarie mensili con scrittura su tabella misure.misure_mensili_c")

    val dtmisure_o_mensili= hiveCtx.sql(strorarie_mensili)//.persist(StorageLevels.MEMORY_ONLY_SER)

    dtmm1.unpersist()
    hiveCtx.dropTempTable("tbl_mm1")

    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_mensili_c")
    hiveCtx.refreshTable(s"misure.misure_mensili_c")




  }

  def calMisure_NoOrarieGG_Mese_Delta(d_max:String,d_min:String): Unit =
  {

    // PULIZIA TABELLA misure_mensili_c
    hiveCtx.sql("TRUNCATE TABLE misure.misure_non_orarie_c")
    hiveCtx.sql("TRUNCATE TABLE misure.misure_non_orarie_base_volture")
    hiveCtx.sql("TRUNCATE TABLE misure.misure_non_orarie_base_volture_c")


    hiveCtx.sql("MSCK REPAIR TABLE misure.misure_non_orarie_c")
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
         NO_ORA.tipo_dato ,NO_ORA.tipo_flusso,NO_ORA.tipo_flusso2
         FROM  misure.misure_non_orarie_base NO_ORA
         INNER JOIN (
          select DISTINCT CONCAT(pod,annomese)pod_periodo
         from misure.misure_non_orarie_base where tipo_flusso='VNO_RNV'
         ) AS POD_VOLTURE ON CONCAT(NO_ORA.pod,NO_ORA.annomese) = POD_VOLTURE.pod_periodo
      """.stripMargin

    /*
    INNER JOIN (
          select CONCAT(pod,annomese)pod_periodo
         from misure.misure_non_orarie_base group by pod,annomese
         having count(*) > 1
         )
     */
    log.info("Estrazione misure non orarie per i mesi in cui sono presenti volture")

    val dt_misure_volture= hiveCtx.sql(strmisure_con_volture)//.persist(StorageLevels.MEMORY_ONLY_SER)
   // dt_misure_volture.registerTempTable("tbl_misure_volture")


    //ESTRAGGO TUTTE LE VOLTURE RICAVANDO LA FORNITURA REPLICANDO LE MISURE SOTTRAENDO UN GIORNO ALLA DATA DI VOLTURA
    // IN MODO DA OTTENERE L'INIZIO VOLTURA/FORNITURA E LA FINE VOLTURA/FORNITURA
    val strvno_rnv=
      s"""
        INSERT INTO misure.misure_non_orarie_base_volture
        SELECT cf_piva,n_id_fornitura ,pod ,giorno ,annomese,
        lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,inizio_fine
        FROM
        (
         SELECT pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,'inizio_voltura' inizio_fine
         FROM misure.tbl_misure_volture M_NO WHERE tipo_flusso ='VNO_RNV'
         UNION ALL
         SELECT pod ,(giorno-1) giorno,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,'fine_voltura' inizio_fine
         FROM misure.tbl_misure_volture M_NO WHERE tipo_flusso ='VNO_RNV'
         UNION ALL
         SELECT pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,tipo_dato ,tipo_flusso,tipo_flusso2,'pno_rno' inizio_fine
         FROM misure.tbl_misure_volture M_NO WHERE tipo_flusso <> 'VNO_RNV'
         ) M_NO
        INNER JOIN tbl_forniture F ON M_NO.pod = F.codice_pod
        WHERE CAST(concat(annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) >= F.inizio AND CAST(concat(annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) <= F.fine
      """.stripMargin

    log.info("Scrittura misure non orarie con relative forniture  per i mesi in cui sono presenti volture nella tabella misure.misure_non_orarie_base_volture")
    hiveCtx.sql(strvno_rnv)

    //hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_misure_volture")
    //dt_misure_volture.unpersist()
    //hiveCtx.dropTempTable("dt_misure_volture")

    /*val calcdelta_mesi_volture=
      s"""
          INSERT INTO misure.misure_non_orarie_base_volture_c
         SELECT n_id_fornitura, fornitura_attiva,pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         (succ_lettura_monoraria-lettura_monoraria)delta_monoraria,
         (succ_eaf1-eaf1)delta_eaf1,
         (succ_eaf2-eaf2)delta_eaf2,
         (succ_eaf3-eaf3)delta_eaf3,
         (succ_eaf4-eaf4)delta_eaf4,
         (succ_eaf5-eaf5)delta_eaf5,
         (succ_eaf6-eaf6)delta_eaf6,tipo_dato ,tipo_flusso,'inizio_voltura' inizio_fine
         from(
         SELECT n_id_fornitura, fornitura_attiva,pod ,giorno ,annomese,
         tipo_dato ,tipo_flusso,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6 ,
         LEAD(M.lettura_monoraria)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) succ_lettura_monoraria,
         LEAD(M.eaf1)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) succ_eaf1,
         LEAD(M.eaf2)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) succ_eaf2,
         LEAD(M.eaf3)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) succ_eaf3,
         LEAD(M.eaf4)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) succ_eaf4,
         LEAD(M.eaf5)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) succ_eaf5,
         LEAD(M.eaf6)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) succ_eaf6,inizio_fine
         FROM misure.misure_non_orarie_base_volture M
         ) as tbl where inizio_fine ='inizio_voltura'
         UNION ALL
         SELECT n_id_fornitura, fornitura_attiva,pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         (lettura_monoraria-prev_lettura_monoraria)delta_monoraria,
         (eaf1-prev_eaf1)delta_eaf1,
         (eaf2-prev_eaf2)delta_eaf2,
         (eaf3-prev_eaf3)delta_eaf3,
         (eaf4-prev_eaf4)delta_eaf4,
         (eaf5-prev_eaf5)delta_eaf5,
         (eaf6-prev_eaf6)delta_eaf6, tipo_dato ,tipo_flusso,'fine_voltura' inizio_fine
         from(
         SELECT n_id_fornitura, fornitura_attiva,pod ,giorno ,annomese,
         tipo_dato ,tipo_flusso,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6 ,
         LAG(M.lettura_monoraria)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) prev_lettura_monoraria,
         LAG(M.eaf1)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) prev_eaf1,
         LAG(M.eaf2)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) prev_eaf2,
         LAG(M.eaf3)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) prev_eaf3,
         LAG(M.eaf4)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) prev_eaf4,
         LAG(M.eaf5)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) prev_eaf5,
         LAG(M.eaf6)over (partition by CONCAT(M.n_id_fornitura,M.pod,M.annomese) order by M.giorno) prev_eaf6,inizio_fine
         FROM misure.misure_non_orarie_base_volture M
         ) as tbl where inizio_fine ='fine_voltura'
      """.stripMargin
*/

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
          (eaf6-prev_eaf6)delta_eaf6, tipo_dato ,tipo_flusso,tipo_flusso2,inizio_fine
          from(
          SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
          tipo_dato ,tipo_flusso,tipo_flusso2,
          lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6 ,
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
         nvl(lettura_monoraria,0.0)lettura_monoraria ,nvl(eaf1,0.0)eaf1 , nvl(eaf2,0.0)eaf2  , nvl(eaf3,0.0)eaf3  , nvl(eaf4,0.0)eaf4  , nvl(eaf5,0.0)eaf5 , nvl(eaf6,0.0)eaf6,
         case when round(delta_monoraria,2) <0 then 0 else round(delta_monoraria,2) end delta_monoraria,
         case when round(delta_eaf1,2)  <0 then 0 else round(delta_eaf1,2) end delta_eaf1,
         case when round(delta_eaf2,2)  <0 then 0 else round(delta_eaf2,2) end delta_eaf2,
         case when round(delta_eaf3,2)  <0 then 0 else round(delta_eaf3,2) end delta_eaf3,
         case when round(delta_eaf4,2)  <0 then 0 else round(delta_eaf4,2) end delta_eaf4,
         case when round(delta_eaf5,2)  <0 then 0 else round(delta_eaf5,2) end delta_eaf5,
         case when round(delta_eaf6,2)  <0 then 0 else round(delta_eaf6,2) end delta_eaf6,
         tipo_dato ,tipo_flusso,tipo_flusso2,is_from_voltura
        FROM
        (
        SELECT F.cf_piva,F.n_id_fornitura ,M_NO.pod ,M_NO.giorno ,M_NO.annomese,
        M_NO.lettura_monoraria ,M_NO.eaf1  , M_NO.eaf2  , M_NO.eaf3  , M_NO.eaf4  , M_NO.eaf5  , M_NO.eaf6,
        0.0 delta_monoraria,0.0 delta_eaf1,0.0 delta_eaf2,0.0 delta_eaf3,0.0 delta_eaf4,0.0 delta_eaf5,0.0 delta_eaf6,
        M_NO.tipo_dato ,M_NO.tipo_flusso,M_NO.tipo_flusso2,'0' is_from_voltura
        FROM misure.misure_non_orarie_base M_NO
        LEFT OUTER JOIN misure.misure_non_orarie_base_volture_c M_VOL
        ON CONCAT(M_NO.pod,M_NO.annomese,M_NO.giorno)=CONCAT(M_VOL.pod,M_VOL.annomese,M_VOL.giorno)
        INNER JOIN tbl_forniture F ON M_NO.pod = F.codice_pod
        WHERE M_VOL.pod IS NULL AND M_NO.tipo_flusso ='PNO_RNO' AND
         (CAST(concat(M_NO.annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) >= F.inizio AND CAST(concat(M_NO.annomese,LPAD(M_NO.giorno,2,0)) AS BIGINT) <= F.fine)
        UNION ALL
        SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,
         tipo_dato ,tipo_flusso,tipo_flusso2,
         case when inizio_fine ='pno_rno' then '1' when inizio_fine ='fine_voltura' and delta_monoraria is null and delta_eaf1 is null then '0' else '1' end is_from_voltura
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
         delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,tipo_dato,tipo_flusso2,is_from_voltura
         FROM
         (
          SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
          lettura_monoraria ,eaf1 , eaf2  , eaf3  ,eaf4  , eaf5 ,eaf6 ,
          split(prev_data,'\b')prev_data_arr,
          delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,tipo_dato,tipo_flusso2,is_from_voltura
          FROM
          (
           SELECT cf_piva,n_id_fornitura,pod ,giorno ,annomese,
           lettura_monoraria ,eaf1 , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
           LAG(CONCAT(M.lettura_monoraria,'\b',M.eaf1,'\b' ,M.eaf2,'\b',M.eaf3,'\b',M.eaf4,'\b',M.eaf5,'\b',M.eaf6))
           over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) as prev_data,
           delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,tipo_dato,tipo_flusso2,is_from_voltura
           FROM misure.tlb_no_orarie M
          ) AS TBL
         )AS XX
      """.stripMargin

    val dtx=hiveCtx.sql(tmp).persist(StorageLevels.MEMORY_ONLY_SER)
    dtx.registerTempTable("tbl_tmp_mis_no_delta")

    /*val str_no_orarie_delta=
      s"""
         INSERT INTO misure.misure_non_orarie_c
         SELECT  cf_piva,n_id_fornitura,annomese competenza_consumi,pod,tipo_dato tipo_misura ,
         lettura_monoraria lettura_misura_monoraria ,
         eaf1 lettura_misura_f1  , eaf2 lettura_misura_f2 , eaf3  lettura_misura_f3,
         eaf4 lettura_misura_f4 , eaf5 lettura_misura_f5  , eaf6 lettura_misura_f6,
         CASE WHEN is_from_voltura='1' THEN delta_eaf1 ELSE
         (case when cast(nvl(prev_eaf1,0) as int) = 0 or prev_eaf1 > eaf1 then 0 else round((eaf1-prev_eaf1),2) end) END delta_misure_f1,
         CASE WHEN is_from_voltura='1' THEN delta_eaf2 ELSE
         (case when cast(nvl(prev_eaf2,0) as int) = 0 or prev_eaf2 > eaf2 then 0 else round((eaf2-prev_eaf2),2) end) END delta_misure_f2,
         CASE WHEN is_from_voltura='1' THEN delta_eaf3 ELSE
         (case when cast(nvl(prev_eaf3,0) as int) = 0 or prev_eaf3 > eaf3 then 0 else round((eaf3-prev_eaf3),2) end) END delta_misure_f3,
         CASE WHEN is_from_voltura='1' THEN delta_eaf4 ELSE
         (case when cast(nvl(prev_eaf4,0) as int) = 0 or prev_eaf4 > eaf4 then 0 else round((eaf4-prev_eaf4),2) end) END delta_misure_f4,
         CASE WHEN is_from_voltura='1' THEN delta_eaf5 ELSE
         (case when cast(nvl(prev_eaf5,0) as int) = 0 or prev_eaf5 > eaf5 then 0 else round((eaf5-prev_eaf5),2) end) END delta_misure_f5,
         CASE WHEN is_from_voltura='1' THEN delta_eaf6 ELSE
         (case when cast(nvl(prev_eaf6,0) as int) = 0 or prev_eaf6 > eaf6 then 0 else round((eaf6-prev_eaf6),2) end) END delta_misure_f6,
         CASE WHEN is_from_voltura='1' THEN delta_monoraria ELSE
         (case when cast(nvl(prev_lettura_monoraria,0) as int) = 0 or prev_lettura_monoraria > lettura_monoraria then 0 else round((lettura_monoraria-prev_lettura_monoraria),2) end)  END delta_misura_monoraria
         FROM
         (
         SELECT  cf_piva,n_id_fornitura,pod ,giorno ,annomese,
         lettura_monoraria ,eaf1  , eaf2  , eaf3  , eaf4  , eaf5  , eaf6,
         LAG(M.lettura_monoraria)over (partition by CONCAT(M.n_id_fornitura,M.pod) order by M.annomese) prev_lettura_monoraria,
         LAG(M.eaf1)over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) prev_eaf1,
         LAG(M.eaf2)over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) prev_eaf2,
         LAG(M.eaf3)over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) prev_eaf3,
         LAG(M.eaf4)over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) prev_eaf4,
         LAG(M.eaf5)over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) prev_eaf5,
         LAG(M.eaf6)over (partition by CONCAT(M.cf_piva,M.pod) order by M.annomese) prev_eaf6,
         delta_monoraria,delta_eaf1,delta_eaf2,delta_eaf3,delta_eaf4,delta_eaf5,delta_eaf6,tipo_dato,is_from_voltura
         FROM misure.tlb_no_orarie M
         ) AS TBL_NO_ORARIE_C
      """.stripMargin*/

    val str_no_orarie_delta=
      s"""
         INSERT INTO misure.misure_non_orarie_c
         SELECT  cf_piva,n_id_fornitura,annomese competenza_consumi,pod,tipo_dato tipo_misura ,
         lettura_monoraria lettura_misura_monoraria ,
         eaf1 lettura_misura_f1  , eaf2 lettura_misura_f2 , eaf3  lettura_misura_f3,
         eaf4 lettura_misura_f4 , eaf5 lettura_misura_f5  , eaf6 lettura_misura_f6,
         CASE WHEN is_from_voltura='1' THEN delta_eaf1 ELSE
         (case when cast(nvl(prev_eaf1,0) as int) = 0 or prev_eaf1 > eaf1 then 0 else round((eaf1-prev_eaf1),2) end) END delta_misure_f1,
         CASE WHEN is_from_voltura='1' THEN delta_eaf2 ELSE
         (case when cast(nvl(prev_eaf2,0) as int) = 0 or prev_eaf2 > eaf2 then 0 else round((eaf2-prev_eaf2),2) end) END delta_misure_f2,
         CASE WHEN is_from_voltura='1' THEN delta_eaf3 ELSE
         (case when cast(nvl(prev_eaf3,0) as int) = 0 or prev_eaf3 > eaf3 then 0 else round((eaf3-prev_eaf3),2) end) END delta_misure_f3,
         CASE WHEN is_from_voltura='1' THEN delta_eaf4 ELSE
         (case when cast(nvl(prev_eaf4,0) as int) = 0 or prev_eaf4 > eaf4 then 0 else round((eaf4-prev_eaf4),2) end) END delta_misure_f4,
         CASE WHEN is_from_voltura='1' THEN delta_eaf5 ELSE
         (case when cast(nvl(prev_eaf5,0) as int) = 0 or prev_eaf5 > eaf5 then 0 else round((eaf5-prev_eaf5),2) end) END delta_misure_f5,
         CASE WHEN is_from_voltura='1' THEN delta_eaf6 ELSE
         (case when cast(nvl(prev_eaf6,0) as int) = 0 or prev_eaf6 > eaf6 then 0 else round((eaf6-prev_eaf6),2) end) END delta_misure_f6,
         CASE WHEN is_from_voltura='1' THEN delta_monoraria ELSE
         (case when cast(nvl(prev_lettura_monoraria,0) as int) = 0 or prev_lettura_monoraria > lettura_monoraria then 0 else round((lettura_monoraria-prev_lettura_monoraria),2) end)  END delta_misura_monoraria,
         tipo_flusso2,concat(annomese,lpad(giorno,2,0))data_lettura
         FROM tbl_tmp_mis_no_delta AS TBL_NO_ORARIE_C
      """.stripMargin


    log.info("Scrittura misure non orarie in tabella misure.misure_non_orarie_c")
    hiveCtx.sql(str_no_orarie_delta)

   // dt_no_orarie.unpersist()
    //hiveCtx.dropTempTable("tlb_no_orarie")
    hiveCtx.sql("DROP TABLE IF EXISTS misure.tlb_no_orarie")
    hiveCtx.sql("DROP TABLE IF EXISTS misure.tbl_misure_volture")
    hiveCtx.dropTempTable("tbl_tmp_mis_no_delta")
    dtx.unpersist()

  }

  def calMisure_Autoletture_Volture(d_max:String,d_min:String): Unit ={


    log.info(s"Avvio estrazione autoletture per il periodo : ${d_min} - ${d_max}")

    hiveCtx.sql("TRUNCATE TABLE misure.autoletture")
    //il flusso autoletture ancora non esiste

    hiveCtx.sql("TRUNCATE TABLE misure.volture")
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
                  WHERE  inizio_fine ='inizio_voltura')misure_non_orarie_base_volture
                  GROUP BY cf_piva,n_id_fornitura ,pod ,annomese,giorno
                ) as volture
      """)

    log.info(s"Scrittura autolettura per il periodo : ${d_min} - ${d_max} eseguita")





  }
}
