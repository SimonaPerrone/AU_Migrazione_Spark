package it.au.misure.util

import java.util.{Calendar, Properties, TimeZone}

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._


object Rigenerate_flusso_misure_noaggr
  extends LoggingSupport {


  val propertiesC =new CreateProperties(System.getProperty("user.dir"))
  val prop:Properties = propertiesC.prop
  val queryProp:Properties = propertiesC.query

  val _dbDest:String = prop.getProperty("spark.app.dbdest")

  val tbl_misurenoaggr:String =s"${_dbDest}.flusso_misure_noaggr"
  val tbl_misurenoaggr_dest=s"${_dbDest}.flusso_misure_noaggr_new"

  var hiveCtx:HiveContext=null

  var curr_elab=""

  //hdfs://hadoop2g.siiau.local:8020 vecchio cluster
  //hdfs://hdfs.fsisilon.siiau.local:8020 nuovo cluster
  val isilon_new_path="hdfs://hdfs.fsisilon.siiau.local:8020" //hdfs://hadoop2g.siiau.local:8020
  val isilon_path="hdfs://hadoop2g.siiau.local:8020"

  def main(args: Array[String]) {

    val commandLineOptions = new CommandLineOptions()
    val commonsCliUtils = new CommonsCliUtils()
    val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
    val argsObjMaster = new CommonsCliUtils().getArgs(commandLine)

    val dataelaborazione: java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis())


    val nameApp = "Rigenerazione flusso_misure_noaggr"

    log.info("***** Inizio processo " + nameApp + " *****")

    log.info("***** current user " + System.getProperty("user.name") + "****")
    log.info(propertiesC.printEnvVar)
    val cur_user = System.getProperty("user.name")

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val current_isilon:String= "hdfs://" + hdfs.getUri().getHost() + ":8020"

    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))

    val anno: String = "2023"//Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

    curr_elab = anno + mese + giorno
    log.info(s"*** current elab : ${curr_elab}")

    log.info(s"*** db in uso : ${_dbDest}" )
    log.info(s"*** tabella noaggr sorgente : ${tbl_misurenoaggr}" )

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
    hiveCtx.setConf("spark.sql.parquet.compression.codec", "snappy")

    hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
    hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    hiveCtx.setConf("hive.exec.dynamic.partition", "true")
    hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
    hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
    hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")
    hiveCtx.setConf("spark.sql.hive.convertMetastoreParquet", "false")



    val dati_schema =hiveCtx.sql(s"show create table ${tbl_misurenoaggr_dest}").collect()
    val l =dati_schema.length-1
    var location_noaggr_dest=""

    breakable {
      for (i <- l to 0 by -1) {
        val str = dati_schema(i).getString(0)
        if (str.contains(current_isilon)) {
          location_noaggr_dest = str.replace("'", "").replace(current_isilon, "").trim
          break
        }
      }
    }
    log.info(s"*** Location tabella ${tbl_misurenoaggr_dest} : ${location_noaggr_dest}")


    val location_noaggr_src=if(current_isilon==isilon_new_path) s"/user/hive/warehouse/au.db/misure_ee_au/flusso_misure_noaggr" else "/user/silvia/au/misure_ee_au/noaggr"

    log.info(s"*** Location tabella noaggr src : ${location_noaggr_src}")


    val annomesiL: Boolean = commandLine.hasOption(commandLineOptions.annomese_sem.getOpt)
    val annomesiList: Array[String] = if (annomesiL) {
      val annomesi: String = commandLine.getOptionValue(commandLineOptions.annomese_sem.getOpt)
      log.info("Applicazione dell'elaborazione ai seguenti mesi(se possibile) : " + annomesi)
      val list_annomesi = annomesi.split(",").filter(x => x.length == 6)
      list_annomesi
    } else Array()


    var mesi =""
    for (an <- 2004 to anno.toInt ) {
     for(ms <- 1 to 12) {

       val res=verifyannomesipar(an, ms, annomesiList, true)
       if(res > 0) {
         if (ms % 1 == 0) {
           mesi += s"${ms}"
           log.info(s"*** Avvio lettura e scrittura anno ${an} e mesi ${mesi} da ${tbl_misurenoaggr} a ${tbl_misurenoaggr_dest}")


           val pt=new Path(s"${location_noaggr_src}/anno=${an}/mese=${mesi}/")
           if(hdfs.exists(pt)) {
             val sizeDir = hdfs.getContentSummary(pt).getSpaceConsumed()
             var numCoalesceFiles = (sizeDir / (blocksize * 1.0)).toInt
             if(numCoalesceFiles==0)
               numCoalesceFiles=1
             else
               numCoalesceFiles = numCoalesceFiles + 3

             log.info(s"*** Num coalesce files tabella noaggr : ${numCoalesceFiles}")

             val query =
               s"""
           SELECT distinct pivadistributore ,codcontrdisp ,area ,isnew_flusso ,coduc ,pod ,pivautente ,data_misura ,data_inizio ,data_voltura ,motivazione ,trattamento ,tensione ,
           perdita ,potcontrimpl ,potimp ,potdisp ,cifreatt ,cifrerea ,cifrepot ,cod_tariffa ,serv_tutela ,prestazioni ,ka ,kr ,kp ,matr_att ,matr_rea ,matr_pot ,
           data_inst_misatt ,data_inst_misrea ,data_inst_mispot ,gruppomis ,forfait ,raccolta ,tipodato_e ,tipodato_s ,tipodato_a ,validato ,potmax ,tipo_rettifica ,
           data_rilevazione ,data_prest ,codprat_att ,codprat_sii ,motivazione_stima ,data_inizio_periodo ,nomefile ,annomesegiornodir ,dataelaborazione ,time_stamp ,giorno ,
           e1 ,e2 ,e3 ,e4 ,e5 ,e6 ,e7 ,e8 ,e9 ,e10 ,
           e11 ,e12 ,e13 ,e14 ,e15 ,e16 ,e17 ,e18 ,e19 ,e20 ,e21 ,e22 ,e23 ,e24 ,e25 ,e26 ,e27 ,e28 ,e29 ,e30 ,e31 ,e32 ,e33 ,e34 ,e35 ,e36 ,e37 ,e38 ,e39 ,e40 ,e41 ,e42 ,
           e43 ,e44 ,e45 ,e46 ,e47 ,e48 ,e49 ,e50 ,e51 ,e52 ,e53 ,e54 ,e55 ,e56 ,e57 ,e58 ,e59 ,e60 ,e61 ,e62 ,e63 ,e64 ,e65 ,e66 ,e67 ,e68 ,e69 ,e70 ,e71 ,e72 ,e73 ,e74 ,
           e75 ,e76 ,e77 ,e78 ,e79 ,e80 ,e81 ,e82 ,e83 ,e84 ,e85 ,e86 ,e87 ,e88 ,e89 ,e90 ,e91 ,e92 ,e93 ,e94 ,e95 ,e96 ,e97 ,e98 ,e99 ,e100 ,er1 ,er2 ,er3 ,er4 ,er5 ,er6 ,
           er7 ,er8 ,er9 ,er10 ,er11 ,er12 ,er13 ,er14 ,er15 ,er16 ,er17 ,er18 ,er19 ,er20 ,er21 ,er22 ,er23 ,er24 ,er25 ,er26 ,er27 ,er28 ,er29 ,er30 ,er31 ,er32 ,er33 ,
           er34 ,er35 ,er36 ,er37 ,er38 ,er39 ,er40 ,er41 ,er42 ,er43 ,er44 ,er45 ,er46 ,er47 ,er48 ,er49 ,er50 ,er51 ,er52 ,er53 ,er54 ,er55 ,er56 ,er57 ,er58 ,er59 ,
           er60 ,er61 ,er62 ,er63 ,er64 ,er65 ,er66 ,er67 ,er68 ,er69 ,er70 ,er71 ,er72 ,er73 ,er74 ,er75 ,er76 ,er77 ,er78 ,er79 ,er80 ,er81 ,er82 ,er83 ,er84 ,er85 ,
           er86 ,er87 ,er88 ,er89 ,er90 ,er91 ,er92 ,er93 ,er94 ,er95 ,er96 ,er97 ,er98 ,er99 ,er100 ,
           eaf1 ,eaf2 ,eaf3 ,eaf4 ,eaf5 ,eaf6 ,erf1 ,erf2 ,erf3 ,erf4 ,erf5 ,erf6 ,potf1 ,potf2 ,potf3 ,potf4 ,potf5 ,potf6 ,eam ,erm ,potm ,anno,mese,tipo_flusso
           FROM ${tbl_misurenoaggr} where anno=${an} and mese IN(${mesi})
         """.stripMargin

             val dtTemp =  hiveCtx.sql(query).repartition(numCoalesceFiles)
             /*val dt =hiveCtx.sql(query)
           val dtx=dt.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count()
           dtx.registerTempTable("tmp")
           val x =hiveCtx.sql("select sum(count) from tmp").collect()
           val numRows=x(0).getAs[Long](0)
           */

             dtTemp
               .write.option("parquet.block.size", blocksize.toString)
               .format("parquet")
               .mode(SaveMode.Append)
               .partitionBy("anno", "mese","tipo_flusso")
               .save(location_noaggr_dest)

             hiveCtx.sql(s"MSCK REPAIR TABLE ${tbl_misurenoaggr_dest}")



             log.info(s"*** Lettura e scrittura anno ${an} e mesi ${mesi} da ${tbl_misurenoaggr} a ${tbl_misurenoaggr_dest} completato")

             Thread.sleep(5000)
           }
           mesi = ""
         }
         else mesi +=s"${ms},"
       }
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

}
