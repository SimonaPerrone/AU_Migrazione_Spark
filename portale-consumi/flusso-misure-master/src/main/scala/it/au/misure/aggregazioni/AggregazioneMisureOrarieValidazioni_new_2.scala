package it.au.misure.aggregazioni

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
//import org.apache.spark.sql.functions.{max, min}
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.util.CreateProperties
import it.au.misure.util.Schemas._
import org.apache.spark.sql.{Row, SaveMode}
//import it.au.misure.cli.CommonsCliUtils.Args
import it.au.misure.util.LoggingSupport
import org.apache.spark.api.java.StorageLevels

/**
	* ==Flusso Misure Aggregazione Misure Orarie==
	* Acquisisce le misure divise in quarti d'ora precedentemente elaborate dal processo
	* di ingestione denominato 'Flusso Misure Inserimento Misure Quarti' e le aggrega per ora. Al termine dell'aggregazione, il risultato viene salvato
	* nella tabella hdfs denominata ''aggregazioni_misure_orarie''.
	*/
object AggregazioneMisureOrarieValidazioni_new_2 extends LoggingSupport {

	/*
   * inizializzazione lettura file di properties
   */
	val propertiesC =new CreateProperties(System.getProperty("user.dir"))
	val prop:Properties = propertiesC.prop
	val queryProp:Properties = propertiesC.query

	val _dbDest:String = prop.getProperty("spark.app.dbdest")
	val _basePath:String =prop.getProperty("spark.app.basepath")

	/*
   * connessione db
   */
	val url:String = prop.getProperty("spark.app.url")
	val user:String = prop.getProperty("spark.app.user")
	val password:String = prop.getProperty("spark.app.password")
	val driver = prop.getProperty("spark.app.jdbc.driver")
	Class.forName(driver)

	/*
   * Flag controlli
   */
	val flaguddpodApp:Boolean = prop.getProperty("spark.app.controllo.flaguddpod").toBoolean
	val trattamentoApp:Boolean = prop.getProperty("spark.app.controllo.trattamento").toBoolean
	val statoApp:Boolean = prop.getProperty("spark.app.controllo.stato").toBoolean
	val validatoApp:Boolean = prop.getProperty("spark.app.controllo.validato").toBoolean
	val flagAreaApp:Boolean = prop.getProperty("spark.app.controllo.flagarea").toBoolean
	val distrAziendaApp:Boolean = prop.getProperty("spark.app.controllo.distr_azienda").toBoolean
	val estraiAreaApp:Boolean = prop.getProperty("spark.app.controllo.estrai_area").toBoolean


	/**
		* Connessione utilizzata per la validazione area
		*/
	val conn1:Connection = DriverManager.getConnection(url, user, password)
	/**
		* Connessione utilizzata per la validazione stato pod ed esistenza RCU_misure per Mercato libero.
		*/
	val conn2:Connection = DriverManager.getConnection(url, user, password)
	/**
		* Connessione utilizzata per al validazione orarie pod-udd.
		*/
	val conn3:Connection = DriverManager.getConnection(url, user, password)

	/**
		* Connessione utilizzata per la vista azienda distributore.
		*/
	val conn4:Connection = DriverManager.getConnection(url, user, password)

	/**
		* Connessione utilizzata ottenere l'area dal pod.
		*/
	val conn5:Connection = DriverManager.getConnection(url, user, password)

	/**
		* Query utilizzata per validare ogni singolo giorno del mese nel file candidato per il POD in esame.
		*/
	val queryPs1 = queryProp.getProperty("spark.query.queryPs1")
	val queryPs11 = queryProp.getProperty("spark.query.queryPs11")

	/**
		* Query utilizzata per la validazione Flag stato pod e controlla l'esistenza RCU_misure per Mercato libero.
		*/
	val queryPs2 = queryProp.getProperty("spark.query.queryPs2")

	/**
		* Query utilizzata per la validazione Flag POD - area.
		*/
	val queryPs4 = queryProp.getProperty("spark.query.queryPs4")

	/**
		* Estrazione area da pod
		*/
	val queryPs3 = queryProp.getProperty("spark.query.queryPs3")

	/**
		* Query utilizzata per ottenere gli identificativi del distributore e del distributore di riferimento sulla base dati AU.
		*/
	val queryDistrAzienda = queryProp.getProperty("spark.query.queryDistrAzienda")

	/**
		* Query che ottiene i dati di aggregazione con il timestamp maggiore per ottenere le misure più recenti.
		*/
	val queryFlussoMisureQuartiTimeStampMax = queryProp.getProperty("spark.query.flusso_misure_quarti.time_stamp_max")


	val perdita_380 = prop.getProperty("spark.app.perdita.tensione.380").toDouble
	val perdita_220 = prop.getProperty("spark.app.perdita.tensione.220").toDouble
	val perdita_150 = prop.getProperty("spark.app.perdita.tensione.150").toDouble
	val perdita_1_35 = prop.getProperty("spark.app.perdita.tensione.1_35").toDouble
	val perdita_1 = prop.getProperty("spark.app.perdita.tensione.1").toDouble

	/**
		* Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
		* @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
		*/
	def main(args: Array[String]) {

		val commandLineOptions = new CommandLineOptions()
		val commonsCliUtils = new CommonsCliUtils()
		val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObj = commonsCliUtils.getArgsAggregati(commandLine)


		val conf = new SparkConf()
			.setAppName( argsObj.appName +" New " )
			.set("spark.shuffle.service.enabled", "false")
			.set("spark.dynamicAllocation.enabled", "false")
			.set("spark.io.compression.codec", "snappy")
			.set("spark.rdd.compress", "true")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

			.setMaster( argsObj.master )

		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
		sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

		val minPartitions =  sc.getConf.get("spark.flusso.misure.min.partitions")

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

		val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

		val annoAggr:String =  argsObj.anno
		val meseAggr:String =  argsObj.mese
		val ghigliottina:Int = argsObj.annomesegiornodir
		val quarti = sc.getConf.get("spark.flusso.misure.quarti")
		val orarie =  sc.getConf.get("spark.aggregazioni.misure.orarie")
		val tMillis = System.currentTimeMillis()
		val dataelaborazione = new java.sql.Timestamp( tMillis )
		val annomese = annoAggr + meseAggr
		val uidElab = sdf.format(new java.util.Date()).toLong
		log.info("***** Inizio processo " + argsObj.appName + " *****")

		log.info("***** current user " + System.getProperty("user.name") + "****")
		log.info(propertiesC.printEnvVar)

		log.info("*** anno: " + annoAggr)
		log.info("*** mese: " + meseAggr)
		log.info("*** annomese: " + annomese)
		log.info("*** ghigliottina: " + ghigliottina)
		log.info("*** quarti: " + quarti)
		log.info("*** orarie: " + orarie)
		log.info("*** dataelaborazione: " + dataelaborazione)
		log.info("*** versione_orarie: " + uidElab)
		log.info("*** minPartitions: " + minPartitions)

		log.info("*** queryPs1: " + queryPs1)
		log.info("*** queryPs11: " + queryPs11)
		log.info("*** queryPs2: " + queryPs2)
		log.info("*** queryPs3: " + queryPs3)
		log.info("*** queryPs4: " + queryPs4)
		log.info("*** queryDistrAzienda: " + queryDistrAzienda)

		log.info(s"*** flaguddpod: ${flaguddpodApp}")
		log.info(s"*** trattamento: ${trattamentoApp}")
		log.info(s"*** stato: ${statoApp}")
		log.info(s"*** validato: ${validatoApp}")
		log.info(s"*** flagarea: ${flagAreaApp}")
		log.info(s"*** distr_azienda: ${distrAziendaApp}")
		log.info(s"*** estrai_area: ${estraiAreaApp}")

		val db = _dbDest

		log.info("*** database di destinazione : "+ db)
    log.info("*** sc.master: " + sc.master)

		/*
     * creazione viste stato pod - trattamento
     */
		creazioneVistaStatoPod(annomese)


		val distr_oracle = sc.broadcast(this.getAllDistrAzienda())

		val queryQuarti:String = if(commandLine.hasOption(commandLineOptions.distrAgg.getOpt)){
			val dul = commandLine.getOptionValue(commandLineOptions.distrAgg.getOpt).split(',').toList
			s" and pivadistributorequarti in (${dul.map ( x => "'" + x + "'").mkString(",") })"

		}else  if(commandLine.hasOption(commandLineOptions.uteAgg.getOpt)){
			val dul = commandLine.getOptionValue(commandLineOptions.uteAgg.getOpt).split(',').toList
			s" and pivautentequarti in (${dul.map ( x => "'" + x + "'").mkString(",") })"

		}else if(commandLine.hasOption(commandLineOptions.noDistrAgg.getOpt)){
			val dul = commandLine.getOptionValue(commandLineOptions.noDistrAgg.getOpt).split(',').toList
			s" and pivadistributorequarti not in (${dul.map ( x => "'" + x + "'").mkString(",") })"

		}else  if(commandLine.hasOption(commandLineOptions.noUteAgg.getOpt)){
			val dul = commandLine.getOptionValue(commandLineOptions.noUteAgg.getOpt).split(',').toList
			s" and pivautentequarti not in (${dul.map ( x => "'" + x + "'").mkString(",") })"

		}else {
			""
		}


		val filesEsclusi = prop.getProperty("spark.app.aggregazioni.file.esclusi")
		log.info(s"*** fileEsclusi: ${filesEsclusi}")
		val filesEsclusiRdd = sc.textFile(filesEsclusi).collect().toList

		val filesEsclusiQuery = if(filesEsclusiRdd.size > 0){
			s" and nomefile not in (${filesEsclusiRdd.map ( x => "'" + x + "'").mkString(",") })"
		}else{
			""
		}
    //val filesEsclusiQuery=""

    val whereCond = s"annoquarti=${annoAggr.toInt} and mesequarti=${meseAggr.toInt} and annomesegiornodir <= ${ ghigliottina } ${ queryQuarti } ${ filesEsclusiQuery }"


		var query = queryFlussoMisureQuartiTimeStampMax.replace("WHERE_CONDITIONS", s" where ${whereCond}")

		//val tt = hiveCtx.sql(query)

    log.info(s"query spark.query.flusso_misure_quarti.time_stamp_max \n${query}")


	//	val tmp ="aggregato_periodico"+annomese
		//tt.registerTempTable(tmp)

		val annomese_aggr=annoAggr.toString+(("0" + meseAggr.toString) takeRight 2)

    val query_tmp =
      s"""
           LEFT OUTER JOIN (SELECT nomefile nf,pod,annomese from au.annullamento_pod_aggroraria where annomese=${annomese_aggr} and not(nomefile is null and pod is null)) mis_ann
           ON lcase(nomefile) = lcase(nvl(mis_ann.nf,nomefile)) and podquarti = nvl(mis_ann.pod,podquarti)
       """.stripMargin

    query = queryFlussoMisureQuartiTimeStampMax.replace("WHERE_CONDITIONS", s" ${query_tmp} where ${whereCond} and (mis_ann.annomese is null ) ")



    log.info("***** max_time_stamp OK")



    log.info(s"Estrazioni misure da escludere per il periodo ${annoAggr} dalla tabella au.annullamento_pod_aggroraria")
		val dtt=hiveCtx.sql(query)
   // dtt.cache()
		val tmp2 ="aggregato_periodico_a"+annomese
		dtt.registerTempTable(tmp2)


		val q_data_validare1=s"SELECT DISTINCT ${annoAggr} anno,${meseAggr} mese , ${uidElab} versione ,podquarti,areaquarti,codcontrdispquarti,pivadistributorequarti" +
      s" FROM ${tmp2} quarti "

		log.info("Avvio validazioni")

		val dt_tmp_pods_area = hiveCtx.sql(q_data_validare1)

    val rdd_pods_area_codcontr = dt_tmp_pods_area.map(erow=> {

      val anno:Int = erow.getInt(0)
      val mese:Int = erow.getInt(1)
      val versione:Long = erow.getLong(2)

      val pod: String = erow.getString(3)
      val areaquarti: String = erow.getString(4)
      val codcontrdisp: String = erow.getString(5)
      val pivadistrquarti = erow.getString(6)




      val area: String = if (estraiAreaApp) {
        estrazioneArea(pod)
      } else {
        areaquarti
      }

      val valUdd = if (!flaguddpodApp) {
        getNIDUdd(codcontrdisp) // ("SK","SK","SK") //
      } else
        ("NULL", "NULL", "NULL") //DOVRA ESSERE CALCOLATO NELLA PROCEDURA DI AGGREGAZIONE

      val valFlag = if (statoApp || trattamentoApp) {
        validazioneStatoPod(pod)
      } else {
        ("SK", "SK")
      }

      val valFlagArea = if (flagAreaApp) {
        validazioneArea(pod, area)
      } else {
        "SK"
      }
      val distrAzienda = if (distrAziendaApp) {
        getDistrAzienda(pivadistrquarti, distr_oracle)
      } else {
        ("SK", "SK")
      }

      val validazioni = List(pod, areaquarti, codcontrdisp, pivadistrquarti,area, valUdd._1,
        valUdd._2, valUdd._3, valFlag._1, valFlag._2, valFlagArea, distrAzienda._1, distrAzienda._2,
        anno,mese,versione)

      Row.fromSeq(validazioni)
    })



    val dfQS_Validazioni = hiveCtx.createDataFrame(rdd_pods_area_codcontr, schemaOreValidazioni)
    /*val tblname="tbl_validazioni"
    dfQS_Validazioni.registerTempTable(tblname)
   */
    log.info("***** creazione DataFrame validazioni Aggregato orario")

    val tblname=s"${db}.aggregazioni_misure_orarie_validazioni"
    hiveCtx.sql(s"TRUNCATE TABLE ${tblname}")

    val orarie_validazioni=orarie+"_validazioni"
    log.info("Scrittura in " + orarie_validazioni)
    dfQS_Validazioni
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .save(orarie_validazioni)
    log.info("***** insert validazioni OK")

    hiveCtx.sql(s"MSCK REPAIR TABLE ${tblname}")


    val ddt2=hiveCtx.sql(s"""
      SELECT AGGR.*,
      nvl(VAL.area_estratta,'')area_estratta,nvl(VAL.udd_Res,'')udd_Res,nvl(VAL.udd,'')udd,nvl(VAL.udd_Piva,'')udd_Piva,
      nvl(VAL.stato,'')stato,nvl(VAL.trattamento,'')trattamento,nvl(VAL.val_area,'')val_area,
      nvl(VAL.id_distr,'')id_distr,nvl(VAL.id_distrRif,'')id_distrRif
      FROM (select * from ${tmp2} DISTRIBUTE BY podquarti,areaquarti,codcontrdispquarti,pivadistributorequarti) AGGR
      LEFT OUTER JOIN (select * from ${tblname} DISTRIBUTE BY pod,areaquarti,codcontrdisp,pivadistrquarti) VAL
      ON CONCAT(nvl(AGGR.podquarti,''),nvl(AGGR.areaquarti,''),nvl(AGGR.codcontrdispquarti,''),nvl(AGGR.pivadistributorequarti,'')) =
      CONCAT(nvl(VAL.pod,''),nvl(VAL.areaquarti,''),nvl(VAL.codcontrdisp,''),nvl(VAL.pivadistrquarti,''))
      """)

		val rdd = ddt2.map { r =>

			val pivaDistrib: String = "000000".concat(r.getAs[String]("pivadistributorequarti")) takeRight 11
			val pivautente: String = r.getAs[String]("pivautentequarti") //"000000".concat(r.getString(1))  takeRight 11
		  val pod: String = r.getAs[String]("podquarti")
			val anno: Int = r.getAs[Int]("annoquarti")
			val mese: Int = r.getAs[Int]("mesequarti") //s(4).toString
		  val giorno: Int = r.getAs[Int]("giornoquarti")

			val validato: String = r.getAs[String]("validato")
      val nomeFile: String = r.getAs[String]("nomefile")
			val codcontrdisp: String = r.getAs[String]("codcontrdispquarti")
			val coduc: String = r.getAs[String]("coducquarti")
			val tipoE: Int = r.getAs[Int]("tipodato_e")
			val tipoS: Int = r.getAs[Int]("tipodato_s")
			val tensione: Double = r.getAs[Double]("tensione")
			val trattO: String = r.getAs[String]("trattamento_o")
      val potcontrimpl: Double = Try(r.getAs[Double]("potcontrimpl")) getOrElse(0D)
			val potdisp: Double = Try(r.getAs[Double]("potdisp")) getOrElse(0D)
			val cifreatt: Int = Try(r.getAs[Int]("cifreatt")) getOrElse(0)
			val cifrerea: Int = Try(r.getAs[Int]("cifrerea")) getOrElse(0)
			val raccolta: String = r.getAs[String]("raccolta")
			val potmax: Double = r.getAs[Double]("potmax")
      val perdita_tmp=Try(r.getAs[Double]("perdita")) getOrElse(0D)
			var perdita: Double = if ( perdita_tmp== 219.999) perdita_220 else perdita_tmp

			val tensionetmp = tensione / 1000.0

			if (perdita == 0.0 && (tensionetmp != 0.0)) {
				if (tensionetmp.toInt < 1) {
					perdita = perdita_1 // 0.104 // 10.4%
				} else if (tensionetmp.toInt >= 1 && tensionetmp.toInt <= 35) {
					perdita = perdita_1_35 // 0.04 // 4%
				} else if (tensionetmp.toInt <= 150) {
					perdita = perdita_150 // 0.018 // 1.8%
				} else if (tensionetmp.toInt <= 220) {
					perdita = perdita_220 // 0.011 // 1.1
				} else if (tensionetmp.toInt > 220) {
					perdita = perdita_380 // 0.007
				} else {
					perdita = tensionetmp
				}
			}


			val annomesegiornodir: Int = r.getInt(22)
			val timestamp: Long = r.getLong(123)


      val area_estratta=r.getAs[String]("area_estratta").trim
      val udd_Res=r.getAs[String]("udd_Res").trim
      val udd=r.getAs[String]("udd").trim
      val udd_Piva=r.getAs[String]("udd_Piva").trim
      val statox=r.getAs[String]("stato").trim
      val trattamentox=r.getAs[String]("trattamento").trim
      val val_area=r.getAs[String]("val_area").trim
      val id_distr=r.getAs[String]("id_distr").trim
      val id_distrRif=r.getAs[String]("id_distrRif").trim

			/*val area: String = if (estraiAreaApp) {
				if(area_estratta!="")area_estratta  else estrazioneArea(pod)
			} else {
        r.getAs[String]("areaquarti")
			}

			val valFlag = if (statoApp || trattamentoApp) {
				if(statox!="")(statox, trattamentox) else validazioneStatoPod(pod)
			} else {
				("SK", "SK")
			}

			val valFlagArea = if (flagAreaApp) {
				if(val_area!="") val_area else validazioneArea(pod, area)
			} else {
				"SK"
			}

			val distrAzienda = if (distrAziendaApp) {
				if(id_distr!="") (id_distr, id_distrRif) else getDistrAzienda(pivaDistrib,distr_oracle)
			} else {
				("SK", "SK")
			}*/

      val area: String = area_estratta

      val valUdd = if (flaguddpodApp) {
        validazioneOrariePodUdd(pod, anno.toString, ("0".concat(mese.toString) takeRight 2), ("0".concat(giorno.toString) takeRight 2))
      } else {
        (udd_Res,udd,udd_Piva)
      }

      val valFlag = (statox, trattamentox)

      val valFlagArea = val_area

      val distrAzienda =(id_distr, id_distrRif)


			// S = scartato
			//UDD da query
			val flaguddpod = valUdd._1 //valUdd.flaguddpod// scarta se N
		  val nIdUdd = valUdd._2 //valUdd.nIdUdd
	  	val tPiva = valUdd._3 //valUdd.nIdUdd

			val stato = valFlag._1 //valFlag.statopod
	  	val trattamento = valFlag._2 //valFlag.isttrat //scarto se non è o
			//controllo tag validato S ok N ko            scarto se no

			//distributore-distributore di riferimento
			val nIdDistr: String = distrAzienda._1
			val nIdDistrRif: String = distrAzienda._2

			/*
       * se i singoli controlli sono attivati dal file di configurazione faccio i controlli altrimenti li considero validati
       */
			val flaguddpodVal = if (flaguddpodApp) {
				flaguddpod.equals("Y")
			} else {
				true
			}
			val trattamentoVal = if (trattamentoApp) {
				trattamento.toUpperCase.equals("Y")
			} else {
				true
			}
			val validatoVal = if (validatoApp) {
				validato.toUpperCase().equals("S")
			} else {
				true
			}
			val flagAreaVal = if (flagAreaApp) {
				valFlagArea.toUpperCase().equals("Y")
			} else {
				true
			}

			val flagValidazioni = if (flaguddpodVal && trattamentoVal && validatoVal && flagAreaVal) {
				"Y"
			} else {
				s"${if (!flaguddpodVal) "F" else ""}${if (!trattamentoVal) "T" else ""}${if (!validatoVal) "V" else ""}${if (!flagAreaVal) "A" else ""}"
			}

			val info = List(pivaDistrib, pivautente, pod, anno, mese, giorno, area, validato, nomeFile, codcontrdisp, coduc, tipoE, tipoS, tensione, trattO, potcontrimpl, potdisp, cifreatt, cifrerea, raccolta, potmax, perdita, annomesegiornodir)

			val hnValues = (23 to 122).map(r.getDouble(_)).grouped(4).map(_.sum * (1 + perdita)).toList

      /*var l :List[Double] = List()
      val h1=(r.getAs[Double](s"e1") + r.getAs[Double](s"e2") + r.getAs[Double](s"e3") + r.getAs[Double](s"e4"))* (1 + perdita)
      val h2=(r.getAs[Double](s"e5") + r.getAs[Double](s"e6") + r.getAs[Double](s"e7") + r.getAs[Double](s"e8"))* (1 + perdita)
      val h3=(r.getAs[Double](s"e9") + r.getAs[Double](s"e10") + r.getAs[Double](s"e11") + r.getAs[Double](s"e12"))* (1 + perdita)
      val h4=(r.getAs[Double](s"e13") + r.getAs[Double](s"e14") + r.getAs[Double](s"e15") + r.getAs[Double](s"e16"))* (1 + perdita)
      val h5=(r.getAs[Double](s"e17") + r.getAs[Double](s"e18") + r.getAs[Double](s"e19") + r.getAs[Double](s"e20"))* (1 + perdita)
      val h6=(r.getAs[Double](s"e21") + r.getAs[Double](s"e22") + r.getAs[Double](s"e23") + r.getAs[Double](s"e24"))* (1 + perdita)
      val h7=(r.getAs[Double](s"e25") + r.getAs[Double](s"e26") + r.getAs[Double](s"e27") + r.getAs[Double](s"e28"))* (1 + perdita)
      val h8=(r.getAs[Double](s"e29") + r.getAs[Double](s"e30") + r.getAs[Double](s"e31") + r.getAs[Double](s"e32"))* (1 + perdita)
      val h9=(r.getAs[Double](s"e33") + r.getAs[Double](s"e34") + r.getAs[Double](s"e35") + r.getAs[Double](s"e36"))* (1 + perdita)
      val h10=(r.getAs[Double](s"e37") + r.getAs[Double](s"e38") + r.getAs[Double](s"e39") + r.getAs[Double](s"e40"))* (1 + perdita)
      val h11=(r.getAs[Double](s"e41") + r.getAs[Double](s"e42") + r.getAs[Double](s"e43") + r.getAs[Double](s"e44"))* (1 + perdita)
      val h12=(r.getAs[Double](s"e45") + r.getAs[Double](s"e46") + r.getAs[Double](s"e47") + r.getAs[Double](s"e48"))* (1 + perdita)
      val h13=(r.getAs[Double](s"e49") + r.getAs[Double](s"e50") + r.getAs[Double](s"e51") + r.getAs[Double](s"e52"))* (1 + perdita)
      val h14=(r.getAs[Double](s"e53") + r.getAs[Double](s"e54") + r.getAs[Double](s"e55") + r.getAs[Double](s"e56"))* (1 + perdita)
      val h15=(r.getAs[Double](s"e57") + r.getAs[Double](s"e58") + r.getAs[Double](s"e59") + r.getAs[Double](s"e60"))* (1 + perdita)
      val h16=(r.getAs[Double](s"e61") + r.getAs[Double](s"e62") + r.getAs[Double](s"e63") + r.getAs[Double](s"e64"))* (1 + perdita)
      val h17=(r.getAs[Double](s"e65") + r.getAs[Double](s"e65") + r.getAs[Double](s"e67") + r.getAs[Double](s"e68"))* (1 + perdita)
      val h18=(r.getAs[Double](s"e69") + r.getAs[Double](s"e70") + r.getAs[Double](s"e71") + r.getAs[Double](s"e72"))* (1 + perdita)
      val h19=(r.getAs[Double](s"e73") + r.getAs[Double](s"e74") + r.getAs[Double](s"e75") + r.getAs[Double](s"e76"))* (1 + perdita)
      val h20=(r.getAs[Double](s"e77") + r.getAs[Double](s"e78") + r.getAs[Double](s"e79") + r.getAs[Double](s"e80"))* (1 + perdita)
      val h21=(r.getAs[Double](s"e81") + r.getAs[Double](s"e82") + r.getAs[Double](s"e83") + r.getAs[Double](s"e84"))* (1 + perdita)
      val h22=(r.getAs[Double](s"e85") + r.getAs[Double](s"e86") + r.getAs[Double](s"e87") + r.getAs[Double](s"e88"))* (1 + perdita)
      val h23=(r.getAs[Double](s"e89") + r.getAs[Double](s"e90") + r.getAs[Double](s"e91") + r.getAs[Double](s"e92"))* (1 + perdita)
      val h24=(r.getAs[Double](s"e93") + r.getAs[Double](s"e94") + r.getAs[Double](s"e95") + r.getAs[Double](s"e96"))* (1 + perdita)
      val h25=(r.getAs[Double](s"e97") + r.getAs[Double](s"e98") + r.getAs[Double](s"e99") + r.getAs[Double](s"e100"))* (1 + perdita)
      val hnValues = List(h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11,h12,h13,h14,h15,h16,h17,h18,h19,h20,h21,h22,h23,h24,h25)
			*/

			val validazioni = List(timestamp, uidElab, dataelaborazione, flaguddpod, stato, trattamento, valFlagArea, nIdUdd, tPiva, nIdDistr, nIdDistrRif, flagValidazioni)

			//val kk_row=anno.toString+mese.toString+pivaDistrib+codcontrdisp

			//(kk_row,Row.fromSeq( info ++ hnValues ++ validazioni ))

			Row.fromSeq(info ++ hnValues ++ validazioni)
		}


		log.info("***** validazione OrariePodUdd, StatoPodArea, DistrAzienda OK")




		//log.info("***** xml scartati OK")

		val dfQS1 = hiveCtx.createDataFrame(rdd, schemaOre)
		log.info("***** creazione DataFrame misure ore OK")


		log.info("Scrittura in " + orarie)
		dfQS1
			.write
			.format("parquet")
			.mode(SaveMode.Append)
			.partitionBy("anno","mese","pivadistributore","versione")
			.save(orarie)
		log.info("***** insert misure ore OK")



		//dtt.unpersist()


		/*
     * aggiorno le partizioni
     */
			hiveCtx.sql("MSCK REPAIR TABLE " + db + ".aggregazioni_misure_orarie")
      //hiveCtx.sql("MSCK REPAIR TABLE au_test.aggregazioni_misure_orarie")


		log.info("***** aggiornamento partizioni OK")
		/*
     * chiudo le connessioni jdbc
     */
		try{  if(conn1 != null && !conn1.isClosed()){  conn1.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
		try{  if(conn2 != null && !conn2.isClosed()){  conn2.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
		try{  if(conn3 != null && !conn3.isClosed()){  conn3.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
		try{  if(conn4 != null && !conn4.isClosed()){  conn4.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
		try{  if(conn5 != null && !conn5.isClosed()){  conn5.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}

		sc.stop()

		log.info("***** Fine processo " + argsObj.appName + " *****")

	}


	/**
		* Crea una vista utilizzata in fase di validazione.
		*
		* @param annomese anno mese di riferimento.
		*/
	def creazioneVistaStatoPod(annomese:String) = {
		val queryJdbc = queryProp.getProperty("spark.query.createview").replaceAll("annomese", annomese)

		Class.forName(driver)
		val connection:Connection = DriverManager.getConnection(url, user, password)
		val ps = connection.createStatement()
		log.info("*** executeUpdate " + ps.executeUpdate(queryJdbc))
		connection.close()
		log.info("***** creazione vista stato pod - trattamento OK")
	}


	def validazioneOrariePodUddMese(pod:String, annomesestr:String) : (String,String,String) = {

		//SELECT distinct 'Y', N_ID_UDD, T_PIVA FROM POD_POD_UDD_STOR_CM WHERE TO_DATE(?,'YYYYMMDD')
		// between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD')) AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD'))
		// and POD_14= substr(?,1,14)
		try{
			val dt = (annomesestr+"01").toInt
			val queryUdd =
			s"""
		    SELECT  N_ID_UDD, T_PIVA ,TO_CHAR(NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')),'YYYYMMDD') D_FINE
				 FROM POD_POD_UDD_STOR_CM
				 WHERE POD_14= substr('${pod}',1,14)
  		""".stripMargin

			val ps1 = conn3.prepareStatement(queryUdd)
			val rs1 = ps1.executeQuery()
			var cc=0
			var rt_val:(String,String,String)=("N","N","N")
			while (rs1.next())
				{
					val dfine=rs1.getInt("D_FINE")
					if(dfine>=dt) {
						cc=cc+1
						rt_val=("Y", rs1.getString("N_ID_UDD"),  rs1.getString("T_PIVA"))
					}
				}
			rt_val = if(cc==0) ("N","N","N")
							 else if(cc>1)("D","N","N")
			         else rt_val


			rs1.close()
			ps1.close()

			rt_val
		}catch{
			case e: Exception => { e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				log.error(" pod:" + pod + " annomese:" + annomesestr  )
				("E", "E", "E")
			}
		}

	}

	/**
		* Esegue una validazione Flag UDD-POD: assegna al flag Y N in  base alla completezza della relazione POD-UDD nel mese.
		* E' presente in un ciclo FOR che per ogni singolo giorno del mese nel file candidato per il POD in esame,
		* controlla che esista la copertura in RCU_POD_UDD, la variabile giorno_valido è un tipo DATE.
		*
		* @param pod identificativo del pod
		* @param anno
		* @param mese
		* @param giorno di appartenenza del pod
		* @return flaguddpod flag di verifica
		*/
	def validazioneOrariePodUdd(pod:String, anno:String, mese:String,giorno:String) : (String,String,String) = {
		//spark.query.queryPs1=SELECT distinct 'Y', N_ID_UDD, T_PIVA FROM POD_POD_UDD_STOR_CM
    // WHERE TO_DATE(?,'YYYYMMDD') between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD'))
    // AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')) and POD_14= substr(?,1,14)
		try{
			val ps1 = conn3.prepareStatement(queryPs1)
			ps1.setString(1, anno + mese + giorno)

			ps1.setString(2, pod)
			val rs1 = ps1.executeQuery()

			val flaguddpod = if(rs1.next()){
				val nIdUdd = rs1.getString("N_ID_UDD")
				val tPiva = rs1.getString("T_PIVA")
				rs1.close()
				ps1.close()
				("Y", nIdUdd, tPiva)
			}else{
				rs1.close()
				ps1.close()
				("N", "N", "N")
			}
			//  								}

			flaguddpod
		}catch{
			case e: Exception => { e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				log.error(" pod:" + pod + " data:" + anno + " " + mese + " "  + giorno)
				("E", "E", "E")
			}
		}

	}

	/**
		* Cerca la chiave primaria tramite il codice codcontrdisp se la trova altrimenti ritorna "N" utilizzato come codice d'errore.
		* @param codcontrdisp
		* @return chiave primaria
		*/
	def getNIDUdd(codcontrdisp:String) : (String,String,String) = {
		//spark.query.queryPs1=SELECT 'Y'  FROM POD_POD_UDD WHERE TO_DATE(?,'YYYYMMDD') between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD'))
		//AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')) and T_PIVA = ? and T_CODICE_POD=?
		try{
			val ps1 = conn3.prepareStatement(queryPs11)
			ps1.setString(1, codcontrdisp)
			val rs1 = ps1.executeQuery()

			val nIdUdd = if(rs1.next()){
				val ret = rs1.getString("N_ID_UDD")
				rs1.close()
				ps1.close()
				("Y", ret, "SK")
			}else{
				rs1.close()
				ps1.close()
				//									ValUdd("N", "N")
				("N", "N", "SK")
			}
			//  								}

			nIdUdd
		}catch{
			case e: Exception => { e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				log.error(s" codcontrdisp: ${codcontrdisp}")
				("E","E","E")
			}
		}

	}


	/**
		* Valida lo stato del pod e il trattamento utilizzando la vista IS_T_TRATTAMENTO.
		* @param pod identificativo del pod
		* @return stato del pod e trattamento
		*/
	def validazioneStatoPod(pod:String) : (String,String) = {
		try{


			//spark.query.queryPs2=SELECT STATO_POD,IS_T_TRATTAMENTO FROM IS_T_TRATTAMENTO_STATO_POD  WHERE T_CODICE_POD= ?
			val ps2 = conn2.prepareStatement(queryPs2)
			ps2.setString(1, pod)
			val rs = ps2.executeQuery()
			if(rs.next() ){
				val statopod:String = rs.getString("STATO_POD")
				val isttrat:String = rs.getString("IS_T_TRATTAMENTO")
				rs.close()
				ps2.close()
				(statopod,isttrat)
			}else{
				rs.close()
				ps2.close()
				("N","N")
			}
		}catch{
			case e: Exception => {e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				("E","E")
			}
		}
	}

	/**
		* Cerca la corrispondenza tra l'area acquisita dal file e quella registrata in RCU.
		* @param pod identificativo del pod
		* @param area area acquisita dal file di misura
		* @return flag di validazione che può essere "Y" o "N"
		*/
	def validazioneArea(pod:String, area:String) : String = {
		try{

			//spark.query.queryPs4=SELECT 'Y' FROM RCU_POD WHERE substr(T_CODICE_POD,1,14) = substr(?,1,14) AND T_AREA_RIF = ?
			val flagArea = {
				val ps2 = conn1.prepareStatement(queryPs4)
				ps2.setString(1, pod)
				ps2.setString(2, area)
				val rs = ps2.executeQuery()
				if(rs.next() ){
					rs.close()
					ps2.close()
					"Y"
				}else{
					rs.close()
					ps2.close()
					//								conn2.close()
					"N"
				}
			}

			flagArea

		}catch{
			case e: Exception => {e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				"E"
			}
		}
	}

	/**
		* Ottiene l'area del pod registrata in RCU.
		* @param pod identificativo del pod
		* @return area
		*/
	def estrazioneArea(pod:String) : String = {
		try{

			//spark.query.queryPs4=SELECT 'Y' FROM RCU_POD WHERE substr(T_CODICE_POD,1,14) = substr(?,1,14) AND T_AREA_RIF = ?
			val flagArea = {
				val ps5 = conn5.prepareStatement(queryPs3)
				ps5.setString(1, pod)
				val rs5 = ps5.executeQuery()
				if(rs5.next() ){
					val ret = rs5.getString(1)
					rs5.close()
					ps5.close()
					ret
				}else{
					rs5.close()
					ps5.close()
					//								conn2.close()
					"N"
				}
			}

			flagArea

		}catch{
			case e: Exception => {e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				"E"
			}
		}
	}


	def getAllDistrAzienda() : Map[String,String] = {
		var dictionary = Map(""->"")
		try{

			val conn:Connection = DriverManager.getConnection(url, user, password)

			//estraggo le definizioni di tutti i distributori
			val ps = conn.prepareStatement("SELECT T_PIVA,N_ID_DISTR, N_ID_DISTR_RIF FROM DISTR_AZ")
			val rs = ps.executeQuery()
			var cc=0
			while(rs.next() ){
				val pivadistr:String = rs.getString("T_PIVA")
				val nIdDistr:String = rs.getString("N_ID_DISTR")
				val nIdDistrRif:String = rs.getString("N_ID_DISTR_RIF")
				dictionary += (pivadistr -> (nIdDistr+"_"+nIdDistrRif))
				cc=cc+1
			}
			rs.close()
			ps.close()

			log.info("Tot distributori letti : " + cc.toString)

			dictionary
		}catch{
			case e: Exception => {e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				dictionary
			}
		}
	}


	/**
		* Cerca gli identificativi del distributore e del distributore di riferimento sulla base dati AU.
		*
		* @param pivadistributore partita iva del distributore del file di misure
		* @return DistrAzienda rappresenta gli identificativi N_ID_DISTR e N_ID_DISTR_RIF
		*/
	def getDistrAzienda(pivadistributore:String,distr_oracle:Broadcast[Map[String,String]]) : (String,String) = {

		try
		{
			if(distr_oracle.value.contains(pivadistributore)){
				val data= distr_oracle.value(pivadistributore).split("_")
				val nIdDistr:String = data(0)
				val nIdDistrRif:String = data(1)
				(nIdDistr,nIdDistrRif)
			}
			else
				("NN","NN")
		}
		catch {
			case e: Exception => {
				e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				("E", "E")
			}
		}
	/*def getDistrAzienda(pivadistributore:String) : (String,String) = {
		try{
			//SELECT N_ID_DISTR,N_ID_DISTR_RIF FROM RCU.DISTR_AZ WHERE T_PIVA=?
			val ps2 = conn4.prepareStatement(queryDistrAzienda)
			ps2.setString(1, pivadistributore)
			val rs = ps2.executeQuery()
			val ret = if(rs.next() ){
				val nIdDistr:String = rs.getString("N_ID_DISTR")
				val nIdDistrRif:String = rs.getString("N_ID_DISTR_RIF")
				rs.close()
				ps2.close()
				(nIdDistr,nIdDistrRif)
			}else{
				rs.close()
				ps2.close()
				//Nomen Nescio
				("NN","NN")
			}
			ret
		}catch{
			case e: Exception => {e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				("E","E")
			}
		}*/
	}
}