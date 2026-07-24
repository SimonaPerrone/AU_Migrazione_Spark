package it.au.misure.aggregazioni

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.Properties

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.util.CreateProperties
import org.apache.spark.sql.{Row, SaveMode}
import it.au.misure.util.LoggingSupport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.mutable.Map
import java.io.{BufferedReader, File, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.io.Source

/**
	* Estrazione dettaglio pod da aggregazione orarie
	* Estrae tutti i pod coinvolti dal precedente processo di aggregazione oraria periodica
	* e salva l'estrazione su tabella hive prt_tmo_aggr_periodica_pod_cons e csv nel percorso impostato nel file di proprietà job.properties
	*/
object AggregazioneMisureOrarieDettaglio extends LoggingSupport {

	/*
   * inizializzazione lettura file di properties
   */
	val propertiesC =new CreateProperties(System.getProperty("user.dir"))
	val prop:Properties = propertiesC.prop
	val queryProp:Properties = propertiesC.query

	val _dbDest:String = prop.getProperty("spark.app.dbdest")
	val _basePath:String =prop.getProperty("spark.app.basepath")

	///mnt/isilonshare1/AGGR_ORARIO_DETT_POD
	val pathCSV:String = prop.getProperty("spark.app.dettpod.aggregato.path")

	var pathCSV_Dest=""
	var pathZIP_Dest_Distr=""
	var pathZIP_Dest_UDD=""


	/**
		* Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
		* @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
		*/
	def main(args: Array[String]) {

		val commandLineOptions = new CommandLineOptions()
		val commonsCliUtils = new CommonsCliUtils()
		val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObj = commonsCliUtils.getArgsDettaglioPodAggregati(commandLine)


		val conf = new SparkConf()
			.setAppName(argsObj.appName)
			.set("spark.shuffle.service.enabled", "false")
			.set("spark.dynamicAllocation.enabled", "false")
			.set("spark.io.compression.codec", "snappy")
			.set("spark.rdd.compress", "true")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

			.setMaster(argsObj.master)

		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
		sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

		val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions")

		val hiveCtx = new HiveContext(sc)

		hiveCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
		hiveCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
		hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
		hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
		hiveCtx.setConf("hive.exec.dynamic.partition", "true")
		hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

		hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
		hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
		hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")

		log.info("***** Inizio processo " + argsObj.appName + " *****")

		var dropTable :Boolean=false
		try {
			val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

			val annoAggr: String = argsObj.anno
			val meseAggr: String = argsObj.mese
			val genCSVParam= if (commandLine.hasOption(commandLineOptions.writeCsvAggrOrario.getOpt))
			 	                 {if(commandLine.getOptionValue(commandLineOptions.writeCsvAggrOrario.getOpt)==null)"" else commandLine.getOptionValue(commandLineOptions.writeCsvAggrOrario.getOpt)}
			                 else ""

			val genCSV = !genCSVParam.equals("N")

			val dett_orarie = sc.getConf.get("spark.aggregazioni.misure.orarie.dettagliopod")


			val annomese = annoAggr + meseAggr
			val v_ora=commandLine.getOptionValue(commandLineOptions.aggregatiDettaglio.getOpt)
			val versione_orarie: Long = if(v_ora ==null || v_ora=="") 0
			                            else v_ora.toLong

			val uidElab=sdf.format(new java.util.Date()).toLong

			val uidElabSel = if(genCSV && !genCSVParam.equals("Y"))
				               {log.info(s"***** Generazione csv/zip da estrazione già effettuata utilizzando il timestamp di elaborazione : ${genCSVParam}")
				               genCSVParam.toLong}
			                 else uidElab





			log.info("***** current user " + System.getProperty("user.name") + "****")
			log.info(propertiesC.printEnvVar)

			log.info("***** anno: " + annoAggr)
			log.info("***** mese: " + meseAggr)
			log.info("***** annomese: " + annomese)
			log.info("***** versione orarie: " + (if(versione_orarie==0) "tutte" else versione_orarie.toString))
			log.info("***** dettaglio orarie hive path: " + dett_orarie)
			log.info("***** timestamp elaborazione: " + uidElab)



			//val db = if (dett_orarie.trim().contains("/user/silvia/au/misure_ee_au/prt_tmo_aggr_periodica_pod_cons")) "au" else "au_test"

			log.info("***** database hive : " + _dbDest)
			log.info("***** sc.master: " + sc.master)


			val queryRestrizione: String = if (commandLine.hasOption(commandLineOptions.distrAgg.getOpt)) {
				val dul = commandLine.getOptionValue(commandLineOptions.distrAgg.getOpt).split(',').toList
				s" and pivadistributore in (${dul.map(x => "'" + x + "'").mkString(",")})"

			} else if (commandLine.hasOption(commandLineOptions.uteAgg.getOpt)) {
				val dul = commandLine.getOptionValue(commandLineOptions.uteAgg.getOpt).split(',').toList
				s" and pivautente in (${dul.map(x => "'" + x + "'").mkString(",")})"

			} else if (commandLine.hasOption(commandLineOptions.noDistrAgg.getOpt)) {
				val dul = commandLine.getOptionValue(commandLineOptions.noDistrAgg.getOpt).split(',').toList
				s" and pivadistributore not in (${dul.map(x => "'" + x + "'").mkString(",")})"

			} else if (commandLine.hasOption(commandLineOptions.noUteAgg.getOpt)) {
				val dul = commandLine.getOptionValue(commandLineOptions.noUteAgg.getOpt).split(',').toList
				s" and pivautente not in (${dul.map(x => "'" + x + "'").mkString(",")})"

			} else {
				""
			}

			if (queryRestrizione != "")
				log.info("*** Restrizione applicata all'estrazione : " + queryRestrizione)



			log.info("*** Root file system path scrittura csv : " + pathCSV)
			log.info("*** Generazione csv :" + genCSV.toString)


			//val filenamecsv=s"dettaglio_${uidElab}.csv"
			val pathCsvFileDir = if (genCSV) {
          setDirectories(pathCSV,annoAggr,meseAggr,uidElab.toString)
			} else ""


			val whereVersioneOr=if(versione_orarie==0) "" else s" and versione = ${versione_orarie}"

			val whereCond = s" flag_validazione ='Y' and anno=${annoAggr.toInt} and mese=${meseAggr.toInt} ${whereVersioneOr} ${queryRestrizione}"




		/*	var tblaggr=
  s"""
		select `x`.`pivautente`,`x`.`pod`,`x`.`giorno`,`x`.`area`,`x`.`validato`,`x`.`nomefile`,`x`.`codcontrdisp`,`x`.`coduc`,`x`.`tipodato_e`,`x`.`tipodato_s`,`x`.`tensione`,`x`.`trattamento_o`,`x`.`potcontrimpl`,`x`.`potdisp`,`x`.`cifreatt`,`x`.`cifrerea`,`x`.`raccolta`,`x`.`potmax`,`x`.`perdita`,`x`.`annomesegiornodir`,`x`.`h1`,`x`.`h2`,`x`.`h3`,`x`.`h4`,`x`.`h5`,`x`.`h6`,`x`.`h7`,`x`.`h8`,`x`.`h9`,`x`.`h10`,`x`.`h11`,`x`.`h12`,`x`.`h13`,`x`.`h14`,`x`.`h15`,`x`.`h16`,`x`.`h17`,`x`.`h18`,`x`.`h19`,`x`.`h20`,`x`.`h21`,`x`.`h22`,`x`.`h23`,`x`.`h24`,`x`.`h25`,`x`.`time_stamp`,`x`.`dataelaborazione`,`x`.`flaguddpod`,`x`.`stato`,`x`.`trattamento`,`x`.`flagarea`,`x`.`n_id_udd`,`x`.`t_piva`,
    case when `x`.`n_id_distr`='150601000000007384' and `x`.`anno` = 2018 and `x`.`mese` = 8 then '180219000000025579'
    when `x`.`n_id_distr`='180823000000031081' and `x`.`anno` = 2018 and `x`.`mese` = 8 then '1452'
    else `x`.`n_id_distr` end `n_id_distr`,
    `x`.`n_id_distr_rif`,`x`.`flag_validazione`,`x`.`anno`,`x`.`mese`,`x`.`pivadistributore`,`x`.`versione`
     FROM (SELECT * FROM `AU`.`aggregazioni_misure_orarie` WHERE ${whereCond}) `X`, `au`.`cronologia_aggr` `Y` WHERE `x`.`versione` = `y`.`versione`
     AND `x`.`anno`=`y`.`anno` AND `x`.`n_id_distr` = `y`.`n_id_distr` AND `x`.`n_id_udd` = `y`.`n_id_udd`
    AND `x`.`area`=`y`.`area` AND `x`.`pivadistributore` = `y`.`pivadistributore` AND `x`.`pivautente` = `y`.`pivautente`


  """.stripMargin*/




			if(uidElabSel==uidElab) {
        if (hiveCtx.tableNames.contains("aggregazioni_misure_orarie_ver_tmp"))
          hiveCtx.sql("DROP TABLE aggregazioni_misure_orarie_ver_tmp")

        if (hiveCtx.tableNames.contains("aggregazioni_misure_orarie_tmp"))
          hiveCtx.sql("DROP TABLE aggregazioni_misure_orarie_tmp")

        if (hiveCtx.tableNames.contains("cronologia_aggr_tmp"))
          hiveCtx.sql("DROP TABLE cronologia_aggr_tmp")


        val th1 = new Thread {

          // create table stato_pods_1 STORED AS PARQUET as
          override def run {
            hiveCtx.sql(
              s"""
            CREATE TABLE aggregazioni_misure_orarie_tmp STORED AS PARQUET AS
            SELECT * ,SUBSTR(pod,7,2) COD_POD, CONCAT(ANNO,MESE,VERSIONE,N_ID_DISTR,N_ID_UDD,AREA,PIVADISTRIBUTORE,PIVAUTENTE)KK_KEY
            FROM AU.aggregazioni_misure_orarie WHERE ${whereCond} ${whereVersioneOr}
            DISTRIBUTE BY COD_POD,KK_KEY
          """.stripMargin)
          }
        }
        th1.start()
        val th2 = new Thread {

          override def run {
            hiveCtx.sql(
              s"""
            CREATE TABLE cronologia_aggr_tmp STORED AS PARQUET AS
            SELECT * , CONCAT(ANNO,MESE,VERSIONE,N_ID_DISTR,N_ID_UDD,AREA,PIVADISTRIBUTORE,PIVAUTENTE)KK_KEY
            FROM AU.cronologia_aggr WHERE anno=${annoAggr} AND MESE =${meseAggr} ${whereVersioneOr}
            DISTRIBUTE BY KK_KEY
          """.stripMargin)
          }
        }
        th2.start()

        if(th1.isAlive)
        th1.join()
        if(th2.isAlive)
        th2.join()


        var query_schema=""
        val schema :Array[Row] =hiveCtx.sql(s"SHOW CREATE TABLE ${_dbDest}.aggregazioni_misure_orarie_ver_p").collect()
        for (el <- schema){
          query_schema +=el(0).toString.trim() + "\r\n"
        }
				query_schema=query_schema.replace("`","").replace("CREATE VIEW au.aggregazioni_misure_orarie_ver_p AS ","").replace("AU.aggregazioni_misure_orarie",s"aggregazioni_misure_orarie_tmp aggregazioni_misure_orarie")
				query_schema=query_schema.replace("au.cronologia_aggr",s"cronologia_aggr_tmp")


        hiveCtx.sql(s"""CREATE TABLE aggregazioni_misure_orarie_ver_tmp STORED AS PARQUET AS
           ${query_schema} """)

        hiveCtx.sql("DROP TABLE aggregazioni_misure_orarie_tmp")
        hiveCtx.sql("DROP TABLE cronologia_aggr_tmp")

				var query =
					s"""
          SELECT az.t_piva  as pivadistributore, az1.t_piva as pivautente, udd.t_codice_terna  as codcontrdisp ,aggr_orario.* FROM
           (
          SELECT n_id_distr,n_id_udd,area t_area_rif , codice_pod,n_id_distr_rif,
          nomefile n_id_file,d_ricezione,tipo_pratica,ROUND(SUM(consumo),3) consumo,
          round(tensione,0)tensione,flag_validazione,perdita,${annoAggr} anno,${meseAggr} mese ,${uidElab} uid_elab , vers_aggrorario
          FROM (
         SELECT n_id_distr, n_id_udd, NVL(area,'NO_AREA')area, concat(ANNO,LPAD(mese,2,0)) annomese, SUBSTR(POD,1,14) codice_pod, N_ID_DISTR_rif, case
         when nomefile like '%2G%' AND nomefile LIKE '%PDO%' then 'PDO2G'
         when nomefile like '%2G%' AND nomefile LIKE '%RFO%' then 'RFO2G'
         ELSE nomefile END AS nomefile,
         max(time_stamp) over ( partition by anno,mese,pivadistributore,pod) AS d_ricezione,
        'P' tipo_pratica,(H1+H2+H3+H4+H5+H6+H7+H8+H9+H10+H11+H12+H13+H14+H15+H16+H17+H18+H19+H20+H21+H22+H23+H24+H25)/(1+nvl(PERDITA,0)) consumo,
        tensione, flag_validazione, perdita ,versione vers_aggrorario
         FROM aggregazioni_misure_orarie_ver_tmp ) X GROUP BY
        n_id_distr,n_id_udd,area,annomese, codice_pod,n_id_distr_rif,nomefile,d_ricezione,tipo_pratica,
        tensione, flag_validazione, perdita,vers_aggrorario ORDER BY vers_aggrorario,n_id_distr,n_id_udd,codice_pod
        ) AS aggr_orario , rcu.rcu_udd_p udd, rcu.rcu_azienda_p az, rcu.rcu_azienda_p az1
        where aggr_orario.n_id_distr=az.n_id_azienda and az1.n_id_azienda = aggr_orario.n_id_udd and aggr_orario.n_id_udd = udd.n_id_udd
  """.stripMargin

				//val tt = hiveCtx.sql(query)

				log.info(s"*** query spark.query.estrazione.dettagliopod.orarie \n${query}")


				val dtt = hiveCtx.sql(query)


				log.info(s"*** Estrazione dettaglio pod da aggregato orario per l'anno = ${annoAggr} , mese = ${meseAggr} ${whereVersioneOr.replace("and", "e")}")


				log.info("*** Scrittura in " + dett_orarie)
				dtt
					.write
					.format("parquet")
					.mode(SaveMode.Append)
					.partitionBy("anno", "mese", "uid_elab", "vers_aggrorario")
					.save(dett_orarie)

        hiveCtx.sql("DROP TABLE aggregazioni_misure_orarie_ver_tmp")


				log.info("*** Estrazione avvenuta con successo!")
				/*
     * aggiorno le partizioni
     */
				hiveCtx.sql("MSCK REPAIR TABLE " + _dbDest + ".prt_tmo_aggr_periodica_pod_cons")


				log.info("*** aggiornamento partizioni OK")
			}

			if (genCSV) {


				log.info("*** Avvio organizzazione dati per esportazione in csv in :" + pathCsvFileDir + pathCSV_Dest)


				/*
				select x.*, az.t_piva  as PivaDistributore, az1.t_piva as PivaUDD, udd.t_codice_terna  as COD_DP from prt_tmo_aggr_periodica_pod_cons x,
				rcu.rcu_udd udd, rcu.rcu_azienda az, rcu.rcu_azienda az1 where x.n_id_distr=az.n_id_azienda
				and az1.n_id_azienda = x.n_id_udd and x.n_id_udd = udd.n_id_udd

				 */

				val whereVersioneCsv=if(versione_orarie==0) "" else s" and vers_aggrorario = ${versione_orarie}"

				val locationcsv=s"${_basePath}/prt_tmo_aggr_periodica_sem_pod_cons_csv"

				if (hiveCtx.tableNames.contains("prt_tmo_aggr_periodica_sem_pod_cons_csv"))
						hiveCtx.sql("DROP TABLE IF EXISTS prt_tmo_aggr_periodica_sem_pod_cons_csv")


				val querytbl=
       s"""
		   CREATE TABLE prt_tmo_aggr_periodica_sem_pod_cons_csv
       (
         codice_pod STRING , consumo STRING  , tensione STRING ,
         n_id_file STRING  , perdita STRING, tipo_pratica STRING  , sessione STRING,
         annomese STRING, sem_corrente STRING
        )
        PARTITIONED BY (k_nome STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\u003B"  STORED AS TEXTFILE
        LOCATION '${locationcsv}'
        """.stripMargin

				hiveCtx.sql(querytbl)

				dropTable=true

				hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
				hiveCtx.sql("set hive.exec.dynamic.partition=true")
				hiveCtx.sql("set hive.exec.max.dynamic.partitions=100000")

				val queryAll =s"""
         INSERT INTO prt_tmo_aggr_periodica_sem_pod_cons_csv PARTITION(k_nome)
         SELECT 'CODICE_POD' codice_pod  ,'ENERGIA' consumo  ,'TENSIONE' tensione ,
         'NOME_FILE' n_id_file , 'COEFF_PERDITA' perdita ,'TIPO_PRATICA' tipo_pratica  , 'SESSIONE' sessione,
         'ANNOMESE' annomese ,'SEM_CORRENTE' sem_corrente ,
         CONCAT('dettaglio','_${uidElab}') k_nome
         UNION ALL
         SELECT codice_pod  , regexp_replace(CAST(format_number(consumo,3) AS STRING),',','') consumo  ,CAST(CAST(tensione AS INT) as STRING)tensione ,
         n_id_file , CAST(perdita as STRING) perdita , tipo_pratica , 'PERIODICA' sessione ,
         CONCAT(anno,LPAD(mese,2,0))annomese ,'NS' sem_corrente ,
         CONCAT('dettaglio','_${uidElab}') k_nome
         from  ${_dbDest}.prt_tmo_aggr_periodica_pod_cons
         where  anno = ${annoAggr} and  mese = ${meseAggr} and  uid_elab = ${uidElabSel} ${whereVersioneCsv}  order by codice_pod,n_id_file
         """

				hiveCtx.sql(queryAll)

				val query =s"""
         INSERT INTO prt_tmo_aggr_periodica_sem_pod_cons_csv PARTITION(k_nome)
         SELECT 'CODICE_POD' codice_pod  ,'ENERGIA' consumo  ,'TENSIONE' tensione ,
         'NOME_FILE' n_id_file , 'COEFF_PERDITA' perdita ,'TIPO_PRATICA' tipo_pratica  , 'SESSIONE' sessione,
         'ANNOMESE' annomese ,'SEM_CORRENTE' sem_corrente ,
         CONCAT(pivadistributore,'_',pivautente,'_',codcontrdisp,'_',t_area_rif,'_',anno,LPAD(mese,2,0),'_${uidElab}') k_nome
         from  ${_dbDest}.prt_tmo_aggr_periodica_pod_cons
         where  anno = ${annoAggr} and  mese = ${meseAggr} and  uid_elab = ${uidElabSel} ${whereVersioneCsv}
         group by CONCAT(pivadistributore,'_',pivautente,'_',codcontrdisp,'_',t_area_rif,'_',anno,LPAD(mese,2,0),'_${uidElab}')
         UNION ALL
         SELECT codice_pod  , regexp_replace(CAST(format_number(consumo,3) AS STRING),',','') consumo  ,CAST(CAST(tensione AS INT) as STRING)tensione ,
         n_id_file , CAST(perdita as STRING) perdita , tipo_pratica , 'PERIODICA' sessione ,
         CONCAT(anno,LPAD(mese,2,0))annomese ,'NS' sem_corrente ,
         CONCAT(pivadistributore,'_',pivautente,'_',codcontrdisp,'_',t_area_rif,'_',anno,LPAD(mese,2,0),'_${uidElab}') k_nome
         from  ${_dbDest}.prt_tmo_aggr_periodica_pod_cons
         where  anno = ${annoAggr} and  mese = ${meseAggr} and  uid_elab = ${uidElabSel} ${whereVersioneCsv}  order by codice_pod,n_id_file
         """



				hiveCtx.sql(query)

				log.info("*** Organizzazione dati per esportazione in csv effettuato")

				writeCsvAndZip(hiveCtx,locationcsv,pathCsvFileDir,annomese,false)

			}



		}
		catch {
			case e: Exception => {
				log.error(e.getMessage, e)
			}
		} finally {

			if(dropTable) hiveCtx.sql("DROP TABLE IF EXISTS prt_tmo_aggr_periodica_sem_pod_cons_csv")
			sc.stop()

		}
		log.info("***** Fine processo " + argsObj.appName + " *****")
	}

	def setDirectories(pathRootExport:String ,annoElab:String,meseElab:String,uidEl:String): String =
	{
		pathCSV_Dest="CSV" + File.separator + uidEl
		pathZIP_Dest_Distr="ZIP" + File.separator + uidEl + File.separator +"DISTR"
		pathZIP_Dest_UDD="ZIP" + File.separator + uidEl + File.separator +"UDD"

		val annomese_aggr = annoElab.toString + (("0" + meseElab.toString) takeRight 2)
		val tmpDirC = new File(s"${pathRootExport}")

		if (pathRootExport != "" && tmpDirC.exists() && tmpDirC.isDirectory()) {
			val tmpDir =new File(s"${pathRootExport}${File.separator}${annomese_aggr}")

			val tmpDirF = new File(tmpDir.getAbsolutePath+ File.separator +  pathCSV_Dest)
			//val tmpDir = new File(tmpDirF.getAbsolutePath)
			tmpDirF.mkdirs()

			val tmpDirZipD = new File(tmpDir.getAbsolutePath+ File.separator + pathZIP_Dest_Distr)
			tmpDirZipD.mkdirs()

			val tmpDirZipU = new File(tmpDir.getAbsolutePath+ File.separator + pathZIP_Dest_UDD)
			tmpDirZipU.mkdirs()

			tmpDir.getAbsolutePath + File.separator
		}
		else {
			log.info(s"*** Attenzione il path ${pathRootExport} non esiste. Estrazione interrotta.")
			throw new Exception(s"Verificare il percorso ${pathRootExport}")
		}


	}

	def writeCsvAndZip(hiveCtx:HiveContext,locationcsv:String,pathCsvFileDir:String,annomese:String ,isRettifica:Boolean,suffix_sem:String=""): Unit ={

		val cc=hiveCtx.sql(s"SELECT COUNT(DISTINCT k_nome) FROM prt_tmo_aggr_periodica_sem_pod_cons_csv ").collect()(0).getLong(0)

		log.info("*** Numero di csv da generare " + cc .toString)
		val htables=mergeAndMove(locationcsv,pathCsvFileDir,cc,isRettifica)

		log.info("*** Scrittura csv avvenuta con successo!")
    val namezip = if(isRettifica)s"_DETTAGLIOPOD_PRATICA_R_${suffix_sem}_" else "_DETTAGLIOPOD_PRATICA_P_"

		val f_distr=htables._1
		val f_udd=htables._2

		val sD=f_distr.size
		var i=0
		log.info("*** Avvio creazioni archivi per i distributori in :" + pathCsvFileDir + pathZIP_Dest_Distr +" . Tot : " + sD.toString)
		for (key <- f_distr.keys)
		{
			if(i==0)
				log.info("*** 0%")


			if(key!="") {
				i=i+1
				val files = f_distr(key).split(";").toList
				val archivio = pathCsvFileDir + pathZIP_Dest_Distr + File.separator + key + s"${namezip}${annomese}.zip"
				compress(archivio, files)
			}

			if(i%5==0 || i== sD)
				log.info("*** " + (((i*1.0)/(sD*1.0))*100.0).toInt.toString +"%")
		}

		i=0
		val sUD=f_udd.size
		log.info("*** Avvio creazioni archivi per gli UDD in :" + pathCsvFileDir + pathZIP_Dest_UDD +" . Tot : " + sUD.toString)
		for (key <- f_udd.keys)
		{
			if(i==0)
				log.info("*** 0%")

			if(key!="") {

				i=i+1

				val files = f_udd(key).split(";").toList
				val archivio = pathCsvFileDir + pathZIP_Dest_UDD  + File.separator+ key + s"${namezip}${annomese}_a.zip"
				compress(archivio, files)
			}

			if(i%10==0 || i== sUD)
				log.info("*** " + (((i*1.0)/(sUD*1.0))*100.0).toInt.toString +"%")
		}
	}

	def mergeAndMove(locationcsv:String,pathLocal:String,numpartitions :Long,isRettifica:Boolean): (Map[String,String],Map[String,String]) =  {
		val hadoopConfig = new Configuration()
		val hdfs = FileSystem.get(hadoopConfig)


		val dirs_partions=hdfs.listLocatedStatus(new Path(locationcsv))

		var dictDistr:Map[String,String] = Map("" -> "")
		var dictUdd:Map[String,String] = Map("" -> "")


    var i=0
		while(dirs_partions.hasNext)
		{
			if(i==0)
				log.info("*** 0%")

			i=i+1

		  val srcPath=	dirs_partions.next().getPath
		  var namefilecsv = srcPath.getName.replace("k_nome=","")+"_1.csv"
			val cols=namefilecsv.split("_")
			val piva_distr=cols(0)
			val piva_udd=cols(1)
			if(piva_distr!="dettaglio" && cols.length>=6) {
				val tmp_t = cols(5)
				//il timestamp di elab nel caso di dettaglio pod sem viene riscritto da AAAAMMGGHHMMS a GGMMAAAAHHMMS
				val timestamp_elab = tmp_t.slice(6, 8) + tmp_t.slice(4, 6) + tmp_t.slice(0, 4) + tmp_t.slice(8, tmp_t.length)
				namefilecsv=namefilecsv.replaceAll(tmp_t,timestamp_elab)
			}



			val dstPath=s"/tmp/${namefilecsv}"

			FileUtil.copyMerge(hdfs, srcPath, hdfs, new Path(dstPath), false, hadoopConfig, null)
			val localCSV = "file://"+pathLocal + pathCSV_Dest + File.separator + namefilecsv
			hdfs.moveToLocalFile(new Path(dstPath),new Path(localCSV))

			val locCsv=localCSV.replace("file://","")
			val crc =new File(locCsv.replace(namefilecsv,"."+namefilecsv+".crc"))
			if(crc.exists())crc.delete()

			if(piva_distr!="dettaglio") {
				if (!dictDistr.contains(piva_distr))
					dictDistr += (piva_distr -> locCsv)
				else
					dictDistr(piva_distr) = dictDistr(piva_distr) + ";" + locCsv

				if (!dictUdd.contains(piva_udd))
					dictUdd += (piva_udd -> locCsv)
				else
					dictUdd(piva_udd) = dictUdd(piva_udd) + ";" + locCsv

			}
			if(i%100==0 || i== numpartitions)
			log.info("*** " + (((i*1.0)/(numpartitions*1.0)*100.0).toInt.toString +"%"))

			// + " - generazione csv " + namefilecsv
		}

		 (dictDistr,dictUdd)


	}

	def compress(zipFilepath: String, files: List[String]) {
		def readByte(bufferedReader: BufferedReader): Stream[Int] = {
			bufferedReader.read() #:: readByte(bufferedReader)
		}

		val zip = new ZipOutputStream(new FileOutputStream(zipFilepath))

		try {
			for (filestr <- files) {
				//inserisco all'archivio zip un nuovo file in streaming
				val file = new File(filestr)
				zip.putNextEntry(new ZipEntry(file.getName))

				val in = Source.fromFile(file.getCanonicalPath).bufferedReader()
				try {
					readByte(in).takeWhile(_ > -1).toList.foreach(zip.write(_))
				}
				catch {
					case e: Exception => {
						log.error(e.getMessage, e)
					}
				}
				finally {
					in.close()
				}

				zip.closeEntry()
			}
		}
		catch {
			case e: Exception => {
				log.error(e.getMessage, e)
			}
		}
		finally {
			zip.close()
		}
	}







}
