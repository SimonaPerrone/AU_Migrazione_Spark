package it.au.misure.sem

import java.io.{File, FileNotFoundException}

import it.au.misure.util.Schemas._
import it.au.misure.util.CreateProperties
import it.au.misure.commons.cli.CommandLine
import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils, TypeDataImportRCU}
import it.au.misure.util.LoggingSupport
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import java.sql.{Connection, DriverManager, ResultSet, Statement, Time}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.broadcast.Broadcast
import java.util.Properties
import java.util.{Calendar, GregorianCalendar, TimeZone}
import java.text.SimpleDateFormat

import it.au.misure.aggregazioni.AggregazioneMisureOrarieDettaglio
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer
import scala.util.Try





object RettificaAggregazioneSEM extends LoggingSupport {

	val format = new SimpleDateFormat("yyyy-MM-dd")

	/**
		* Legge le variabili del file di properties.
		*/
	val propertiesC =new CreateProperties(System.getProperty("user.dir"))
	val prop:Properties = propertiesC.prop

	val verbose:Boolean=  prop.getProperty("spark.app.verbose.sem").toBoolean
	val testlevel:String=  prop.getProperty("spark.app.testlevel")
	val mot3_table:String=  prop.getProperty("spark.app.mot3_table")
	val flussoquarti_table:String=  prop.getProperty("spark.app.flussoquarti_table")
	val flussoquarti_tableoracle:String=  prop.getProperty("spark.app.flussoquarti_table.oracle")
	val calcSemOraclePraticheR_Table:String= prop.getProperty("spark.app.sem.oracle.praticheR_TBL")
	val calcSemOraclePraticheR:Boolean= if (calcSemOraclePraticheR_Table=="") false else true
	val _dbDest:String = prop.getProperty("spark.app.dbdest")
	val _basePath:String =prop.getProperty("spark.app.basepath")


	var anno_iniziale: Int = 0
	var mese_iniziale: Int = 0
	var Tipo_SEM = ""
	var Sessione_SEM=""
	var Sessione_SEM_CMD=""
	var Commit_SEM=false
	var RunExportCommit=false
  var Ghigliottina_Max:String=""
	var FindInvalidazioni:Boolean=false


	var anno_finale :Int = 0
	var mese_finale :Int = 0

	var sc:SparkContext=null
	var hiveCtx:HiveContext=null


	/*
   * connessione impala
   */

	val ConnectionURL:String = prop.getProperty("spark.app.impala.url")
	val driverimpala = prop.getProperty("spark.app.impala.jdbc.driver")
	Class.forName(driverimpala)


	/*
	parametri per SEM e Oracle
	 */

	val url:String = prop.getProperty("spark.app.url")
	val user:String = prop.getProperty("spark.app.user")
	val password:String = prop.getProperty("spark.app.password")
	val driver = prop.getProperty("spark.app.jdbc.driver")
	Class.forName(driver)



	var _annoAggr:Int=0
	var _meseAggr:Int=0
	var _listDistributori_Ghigliottina:String=""

	var isSemForceTot :Boolean =false
	var annomese:String =""
	var queryProp:Properties = propertiesC.query
	var flaguddpodApp:Boolean = false
	var trattamentoApp:Boolean = false
	var statoApp:Boolean = false
	var validatoApp:Boolean = false
	var flagAreaApp:Boolean = false
	var distrAziendaApp:Boolean = false
	var estraiAreaApp:Boolean = false

  var tbl_tmp_terne:String=""
  var tbl_tmp_sem_periodica:String=""
	var tbl_misure_escluse:String=""

	val conn1:Connection = DriverManager.getConnection(url, user, password)
	val conn2:Connection = DriverManager.getConnection(url, user, password)
	val conn3:Connection = DriverManager.getConnection(url, user, password)
	val conn5:Connection = DriverManager.getConnection(url, user, password)

	var queryPs1 = ""
	var queryPs11 = ""
	var queryPs2 = ""
	var queryPs4 = ""
	var queryPs3 = ""
	var queryDistrAzienda = ""
	var queryFlussoMisureQuartiTimeStampMax = ""

	def init(annoAggr :Int, meseAggr:Int,isSemFTot:Boolean,isSemOracle:Boolean, elencoDistributori_Ghigliottina:String,db_dest:String): Unit =
	{
		_annoAggr=annoAggr
		_meseAggr=meseAggr
		_listDistributori_Ghigliottina=elencoDistributori_Ghigliottina
		//_dbDest = db_dest


		annomese =_annoAggr.toString +(("0" + _meseAggr) takeRight 2)
		queryProp = if(!isSemOracle)propertiesC.query_sem(annomese)else propertiesC.query_sem("")
		isSemForceTot=isSemFTot

		flaguddpodApp = prop.getProperty("spark.app.controllo.flaguddpod.sem" + (if (isSemOracle)".oracle" else "")).toBoolean
		trattamentoApp = prop.getProperty("spark.app.controllo.trattamento.sem" + (if (isSemOracle)".oracle" else "")).toBoolean
		statoApp = prop.getProperty("spark.app.controllo.stato.sem" + (if (isSemOracle)".oracle" else "")).toBoolean
		validatoApp = prop.getProperty("spark.app.controllo.validato.sem" + (if (isSemOracle)".oracle" else "")).toBoolean
		flagAreaApp = prop.getProperty("spark.app.controllo.flagarea.sem" + (if (isSemOracle)".oracle" else "")).toBoolean
		distrAziendaApp = prop.getProperty("spark.app.controllo.distr_azienda.sem" + (if (isSemOracle)".oracle" else "")).toBoolean
		estraiAreaApp = prop.getProperty("spark.app.controllo.estrai_area.sem" + (if (isSemOracle)".oracle" else "")).toBoolean

		//log.info("distrAziendaApp : " + distrAziendaApp.toString)

		queryPs1 = queryProp.getProperty("spark.query.queryPs1")
		queryPs11 = queryProp.getProperty("spark.query.queryPs11")
		queryPs2 = queryProp.getProperty("spark.query.queryPs2").replaceAll("IS_T_TRATTAMENTO_STATO_POD",s"IS_T_TRATT${annomese}_STATO_POD")
		queryPs4 = queryProp.getProperty("spark.query.queryPs4")
		queryPs3 = queryProp.getProperty("spark.query.queryPs3")
		queryDistrAzienda = queryProp.getProperty("spark.query.queryDistrAzienda")
		queryFlussoMisureQuartiTimeStampMax = queryProp.getProperty("spark.query.flusso_misure_quarti.time_stamp_max")


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

	def runAggregazione(isMonthOracle:Boolean,pathOrarieEm:String,pathAmSem:String,pathTerneSEM:String,checkSemParz:Boolean,distr_oracle:Broadcast[Map[String,String]]): Unit = {


    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val tMillis = System.currentTimeMillis()
    val dataelaborazione = new java.sql.Timestamp(tMillis)
    //val annomese = _annoAggr + _meseAggr
    val uidElab = sdf.format(new java.util.Date()).toLong

    goAggregazione(uidElab, dataelaborazione, pathOrarieEm, pathAmSem,pathTerneSEM, checkSemParz, distr_oracle,isMonthOracle)


  }

	/**
		* Crea una vista utilizzata in fase di validazione.
		*
		* @param annomese anno mese di riferimento.
		*/
	def creazioneVistaStatoPod(annomese:String) = {
		val queryJdbc = queryProp.getProperty("spark.query.createview").replaceAll("annomese", annomese).replaceAll("IS_T_TRATTAMENTO_STATO_POD",s"IS_T_TRATT${annomese}_STATO_POD")
		//log.info(queryJdbc)
		Class.forName(driver)
		val connection:Connection = DriverManager.getConnection(url, user, password)
		val ps = connection.createStatement()
		log.info("*** executeUpdate view : " + "IS_T_TRATT"+annomese+"_STATO_POD = " + ps.executeUpdate(queryJdbc) )
		connection.close()
		log.info("***** creazione vista stato pod - trattamento OK")
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
	def validazioneOrariePodUdd(pod:String, anno:String, mese:String,giorno:String,qps1:String) : (String,String,String) = {
		//spark.query.queryPs1=SELECT 'Y'  FROM POD_POD_UDD WHERE TO_DATE(?,'YYYYMMDD') between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD'))
		//AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')) and T_PIVA = ? and T_CODICE_POD=?
		try{
			val ps1 = conn3.prepareStatement(qps1)
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
	def getNIDUdd(codcontrdisp:String,ps11:String) : (String,String,String) = {
		//spark.query.queryPs1=SELECT 'Y'  FROM POD_POD_UDD WHERE TO_DATE(?,'YYYYMMDD') between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD'))
		//AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')) and T_PIVA = ? and T_CODICE_POD=?
		try{
			val ps1 = conn3.prepareStatement(ps11)
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
	def validazioneStatoPod(pod:String,qps2:String) : (String,String) = {
		try{


			//spark.query.queryPs2=SELECT STATO_POD,IS_T_TRATTAMENTO FROM IS_T_TRATTAMENTO_STATO_POD  WHERE T_CODICE_POD= ?
			val ps2 = conn2.prepareStatement(qps2)
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
	def validazioneArea(pod:String, area:String,ps4:String) : String = {
		try{

			//spark.query.queryPs4=SELECT 'Y' FROM RCU_POD WHERE substr(T_CODICE_POD,1,14) = substr(?,1,14) AND T_AREA_RIF = ?
			val flagArea = {
				val ps2 = conn1.prepareStatement(ps4)
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
	def estrazioneArea(pod:String,ps3:String) : String = {
		try{

			//spark.query.queryPs4=SELECT 'Y' FROM RCU_POD WHERE substr(T_CODICE_POD,1,14) = substr(?,1,14) AND T_AREA_RIF = ?
			val flagArea = {
				val ps5 = conn5.prepareStatement(ps3)
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

	}

	def runAggregTerna(hiveContext: HiveContext,versione:String ,dataElaborazione:java.sql.Timestamp,tempTbl:String): DataFrame ={



		val query =
			s"""
        select n_id_udd,n_id_distr,n_id_distr_rif,area,giorno as giornoaggr,
        SUM(h1) h1,SUM(h2) h2,SUM(h3) h3,SUM(h4) h4,SUM(h5) h5,SUM(h6) h6,SUM(h7) h7,SUM(h8) h8,SUM(h9) h9,SUM(h10)
        h10,SUM(h11) h11,SUM(h12) h12,SUM(h13) h13,SUM(h14) h14,SUM(h15) h15,SUM(h16) h16,SUM(h17) h17,SUM(h18) h18,
        SUM(h19) h19,SUM(h20) h20,SUM(h21) h21,SUM(h22) h22,SUM(h23) h23,SUM(h24) h24,SUM(h25) h25,anno as annoaggr,
        mese as meseaggr,pivadistributore as pivadistributoreaggr,${versione}  as versione_orarie
        from ${tempTbl} where flag_validazione='Y' and anno = ${_annoAggr} and mese = ${_meseAggr} and sessione ='${Sessione_SEM}'
        GROUP BY n_id_udd,area,pivadistributore,anno,mese,giorno,
        n_id_distr,n_id_distr_rif,flag_validazione
      """.stripMargin
		val view =	hiveContext.sql(query)


		val dfAggr3 = view

		log.info("***** select " + tempTbl + " OK")


		val refill:String => String = ( f => ("0".concat(f.toString()) takeRight 2) )
		val refillUDF = udf(refill)

		val refillDistr:String => String = ( f => ("000000".concat(f)  takeRight 11) )
		val refillDistrUDF = udf(refillDistr)

		val rounding:Double => Double = ( BigDecimal( _ ).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble )
		val roundingUDF = udf(rounding)


		val dfAggr3_x=dfAggr3.withColumn("dataelaborazione",lit(dataElaborazione))

		val dfAggr4 = dfAggr3_x
			.select(
				col("n_id_udd").cast(StringType),
				col("n_id_distr").cast(StringType),
				col("n_id_distr_rif").cast(StringType),
				col("area"),
				refillDistrUDF(col("pivadistributoreaggr")).alias("pivadistributoreaggr"),
				col("giornoaggr"),
				col("dataelaborazione"),
				col("versione_orarie"),
				roundingUDF(col("h1")).alias("h1"),
				roundingUDF(col("h2")).alias("h2"),
				roundingUDF(col("h3")).alias("h3"),
				roundingUDF(col("h4")).alias("h4"),
				roundingUDF(col("h5")).alias("h5"),
				roundingUDF(col("h6")).alias("h6"),
				roundingUDF(col("h7")).alias("h7"),
				roundingUDF(col("h8")).alias("h8"),
				roundingUDF(col("h9")).alias("h9"),
				roundingUDF(col("h10")).alias("h10"),
				roundingUDF(col("h11")).alias("h11"),
				roundingUDF(col("h12")).alias("h12"),
				roundingUDF(col("h13")).alias("h13"),
				roundingUDF(col("h14")).alias("h14"),
				roundingUDF(col("h15")).alias("h15"),
				roundingUDF(col("h16")).alias("h16"),
				roundingUDF(col("h17")).alias("h17"),
				roundingUDF(col("h18")).alias("h18"),
				roundingUDF(col("h19")).alias("h19"),
				roundingUDF(col("h20")).alias("h20"),
				roundingUDF(col("h21")).alias("h21"),
				roundingUDF(col("h22")).alias("h22"),
				roundingUDF(col("h23")).alias("h23"),
				roundingUDF(col("h24")).alias("h24"),
				roundingUDF(col("h25")).alias("h25"),
				col("annoaggr"),
				col("meseaggr")
			)

		//dfAggr4.persist(StorageLevels.MEMORY_ONLY_SER)

		dfAggr4

	}

	def FindTerneInRett(hiveContext:HiveContext,tempTblTernax: String):(DataFrame,ListBuffer[String]) = {

    val temptbl_calc_sem = "rcu_tmo_aggregati_calc_sem_" + annomese
    val temptbl_calc_periodica = "rcu_tmo_aggregati_calcolati_" + annomese
    var tempTbl_sem_periodica: String = "sem_periodica_" + annomese

    var tempTblTerna:String = tempTblTernax
    var tables_tmp:ListBuffer[String]=new ListBuffer[String]()

		val queryFromSEM =
			s"""
             select n_id_distr_rif ,n_id_distr ,n_id_udd, area,
             giornoaggr ,uid_elab,
             h1 ,h2 ,h3 ,h4 ,h5 ,h6 ,h7 ,h8 ,h9 ,h10 ,h11 ,h12 ,h13 ,
             h14 ,h15 ,h16 ,h17 ,h18 ,h19 ,h20 ,h21 ,h22 ,h23 ,h24 ,h25 ,
             sem_storic.data_aggregazione ,1 from_sem
             from
             (
             select n_id_distr_rif , giornoaggr ,uid_elab,
             h1 ,h2 ,h3 ,h4 ,h5 ,h6 ,h7 ,h8 ,h9 ,h10 ,h11 ,h12 ,h13 ,
             h14 ,h15 ,h16 ,h17 ,h18 ,h19 ,h20 ,h21 ,h22 ,h23 ,h24 ,h25 ,
             annoaggr , meseaggr , n_id_distr ,n_id_udd ,area ,
             max(data_aggregazione) over ( partition by annoaggr,meseaggr,n_id_distr,n_id_distr_rif,n_id_udd ,area ) data_aggregazione_1, data_aggregazione
             from ${_dbDest}.rcu_tmo_aggregati_calc_sem
             where annoaggr = ${_annoAggr} and meseaggr =${_meseAggr} and aggr_sottesi = 'N'
            ) as sem_storic
             where sem_storic.data_aggregazione = sem_storic.data_aggregazione_1 
             DISTRIBUTE BY n_id_distr,n_id_distr_rif,n_id_udd,area,giornoaggr
         """.stripMargin


		val dtcalc_sem = hiveContext.sql(queryFromSEM).
      withColumn("Kterna_DAY",concat(col("n_id_distr"),col("n_id_distr_rif"),col("n_id_udd"),col("area"),col("giornoaggr")))
		dtcalc_sem.registerTempTable(temptbl_calc_sem)
    tables_tmp+=temptbl_calc_sem

    if(verbose) {
     log.info(s"""
        Estrazione terne da rcu_tmo_aggregati_calc_sem :
        ${queryFromSEM}
        """.stripMargin)

      log.info(s"Aggiunta chiave in dataframe come vista temporanea ${temptbl_calc_sem} Kterna_DAY = n_id_distr+n_id_distr_rif+n_id_udd+area+giornoaggr")
    }else
      log.info("Estrazione terne da rcu_tmo_aggregati_calc_sem")

		//PRENDO TUTTE LE TERNE PRESENTI NELLA PERIODICA  CON MAX DATAGGREGAZIONE MA NON PRESENTI NELLA TABELLA DELLA SEM
		//TRAMITE UNA LEFT JOIN TRA PERIODICO E SEM ED ESCLUDO LE RIGHE DOVE LA JOIN E PIENA (DA ENTRAMBE LE TABELLE)
		val queryFromPeriodica =
		s"""
          select A.n_id_distr_rif ,A.n_id_distr ,A.n_id_udd, A.area,A.giornoaggr ,A.uid_elab,
          A.h1 ,A.h2 ,A.h3 ,A.h4 ,A.h5 ,A.h6 ,A.h7 ,A.h8 ,A.h9 ,A.h10 ,A.h11 ,A.h12 ,A.h13 ,
          A.h14 ,A.h15 ,A.h16 ,A.h17 ,A.h18 ,A.h19 ,A.h20 ,A.h21 ,A.h22 ,A.h23 ,A.h24 ,A.h25 ,
          A.data_aggregazione ,0 from_sem from
          (
          select n_id_distr_rif , giornoaggr ,uid_elab,h1 ,h2 ,h3 ,h4 ,h5 ,h6 ,h7 ,h8 ,h9 ,h10 ,h11 ,h12 ,h13 ,
          h14 ,h15 ,h16 ,h17 ,h18 ,h19 ,h20 ,h21 ,h22 ,h23 ,h24 ,h25 ,annoaggr , meseaggr , n_id_distr ,n_id_udd ,area ,
          max(data_aggregazione) over ( partition by  annoaggr,meseaggr,n_id_distr,n_id_distr_rif,n_id_udd ,area ) data_aggregazione_1, data_aggregazione
          from ${_dbDest}.rcu_tmo_aggregati_calcolati
          where annoaggr = ${_annoAggr} and meseaggr =${_meseAggr} and aggr_sottesi = 'N'
          ) as A
          where A.data_aggregazione = A.data_aggregazione_1 
          DISTRIBUTE BY n_id_distr,n_id_distr_rif,n_id_udd,area,giornoaggr
      """.stripMargin

		val dtcalc_periodica = hiveContext.sql(queryFromPeriodica).
      withColumn("Kterna_DAY",concat(col("n_id_distr"),col("n_id_distr_rif"),col("n_id_udd"),col("area"),col("giornoaggr")))
		dtcalc_periodica.registerTempTable(temptbl_calc_periodica)
    tables_tmp+=temptbl_calc_periodica

    if(verbose) {
      log.info(s"""
        Estrazione terne da rcu_tmo_aggregati_calcolati :
        ${queryFromPeriodica}
        """.stripMargin)

      log.info(s"Aggiunta chiave in dataframe come vista temporanea ${temptbl_calc_periodica} Kterna_DAY = n_id_distr+n_id_distr_rif+n_id_udd+area+giornoaggr")
    }else
      log.info("Estrazione terne da rcu_tmo_aggregati_calcolati")

    val queryjoin =
      s"""
          select A.n_id_distr_rif ,A.n_id_distr ,A.n_id_udd, A.area,A.giornoaggr ,A.uid_elab,
          A.h1 ,A.h2 ,A.h3 ,A.h4 ,A.h5 ,A.h6 ,A.h7 ,A.h8 ,A.h9 ,A.h10 ,A.h11 ,A.h12 ,A.h13 ,
          A.h14 ,A.h15 ,A.h16 ,A.h17 ,A.h18 ,A.h19 ,A.h20 ,A.h21 ,A.h22 ,A.h23 ,A.h24 ,A.h25 ,
          A.data_aggregazione , A.from_sem , A.Kterna_DAY  FROM ${temptbl_calc_periodica} A
          left outer join ${temptbl_calc_sem} B ON A.Kterna_DAY = B.Kterna_DAY
          where B.Kterna_DAY IS NULL
      """.stripMargin

    val dtcalc_periodica_join = hiveContext.sql(queryjoin)
    val period_cal_join ="period_cal_join_" + annomese
    dtcalc_periodica_join.registerTempTable(period_cal_join)
    tables_tmp+=period_cal_join

    if(verbose) {
      log.info(s"""
        Estrazione terne per singolo giorno da rcu_tmo_aggregati_calcolati non presenti in rcu_tmo_aggregati_calc_sem  :
        ${queryjoin}
        """.stripMargin)

    }else log.info("Estrazione terne per singolo giorno da rcu_tmo_aggregati_calcolati non presenti in rcu_tmo_aggregati_calc_sem ")

    //OTTENGO TUTTE LE TERNE FACENDO UNA UNION TRA LA TABELLE DELLE SEM PREFILTRATA E LA PERIODICA PREFILTRATA

    if(verbose) log.info("Fusione delle terne provenienti da rcu_tmo_aggregati_calc_sem + rcu_tmo_aggregati_calcolati in vista temporanea : " + tempTbl_sem_periodica)
    else log.info("Fusione delle terne provenienti da rcu_tmo_aggregati_calc_sem + rcu_tmo_aggregati_calcolati")

    val dt_view_sem_periodica = dtcalc_sem.unionAll(dtcalc_periodica_join).
      withColumn("EA_SP", concat(col("h1"),col("h2"),col("h3"),col("h4"),col("h5"),col("h6"),col("h7"),col("h8"),col("h9"),col("h10"),col("h11"),
        col("h12"),col("h13"),col("h14"),col("h15"),col("h16"),col("h17"),col("h18"),col("h19"),col("h20"),col("h21"),col("h22"),col("h23"),col("h24"),col("h25")))

		dt_view_sem_periodica.registerTempTable(tempTbl_sem_periodica)
    tables_tmp+=tempTbl_sem_periodica



    //SCRITTURA TABELLE tempTblTerna + tempTbl_sem_periodica


    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val tMillis = System.currentTimeMillis()
    val dataelaborazione = new java.sql.Timestamp(tMillis)
    //val annomese = _annoAggr + _meseAggr
    val uidElabTMP = sdf.format(new java.util.Date()).toLong


    val thread1 = new Thread {
      override def run {
				//SCRITTURA TERNE APPENA AGGREGATE IN TABELLA TEMPORANEA
        log.info("Avvio scrittura terne in tabella temporanea aggregazioni_misure_am_sem_tmp - path :" + tbl_tmp_terne)
        hiveContext.sql(s"TRUNCATE TABLE ${_dbDest}.aggregazioni_misure_am_sem_tmp")
        hiveContext.sql(s"MSCK REPAIR TABLE ${_dbDest}.aggregazioni_misure_am_sem_tmp")
        hiveContext.refreshTable(s"${_dbDest}.aggregazioni_misure_am_sem_tmp")

       val dtT= hiveContext.sql(
          s"""
            select n_id_udd ,n_id_distr ,n_id_distr_rif ,area ,
            pivadistributoreaggr , giornoaggr , dataelaborazione ,versione_orarie ,
            h1 ,h2 ,h3 ,h4 ,h5 ,h6 ,h7 ,h8 ,h9 ,h10 ,h11 ,h12 ,h13 ,
            h14 ,h15 ,h16 ,h17 ,h18 ,h19 ,h20 ,h21 ,h22 ,h23 ,h24 ,h25 ,
            annoaggr , meseaggr ,sessione ,Kterna ,EA_TR ,Kterna_DAY
            from ${tempTblTerna}
          """.stripMargin)

        dtT
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .save(tbl_tmp_terne)

          hiveContext.sql(s"MSCK REPAIR TABLE ${_dbDest}.aggregazioni_misure_am_sem_tmp")
          hiveContext.refreshTable(s"${_dbDest}.aggregazioni_misure_am_sem_tmp")

          tempTblTerna=s"${_dbDest}.aggregazioni_misure_am_sem_tmp"
      }
    }

    val thread2 = new Thread {
      override def run {

        log.info("Avvio scrittura sem storic + periodica in tabella temporanea aggregazioni_misure_am_semperiodica_tmp - path :" + tbl_tmp_sem_periodica)

        hiveContext.sql(s"TRUNCATE TABLE ${_dbDest}.aggregazioni_misure_am_semperiodica_tmp")
        hiveContext.sql(s"MSCK REPAIR TABLE ${_dbDest}.aggregazioni_misure_am_semperiodica_tmp")
        hiveContext.refreshTable(s"${_dbDest}.aggregazioni_misure_am_semperiodica_tmp")

        dt_view_sem_periodica
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .save(tbl_tmp_sem_periodica)

        hiveContext.sql(s"MSCK REPAIR TABLE ${_dbDest}.aggregazioni_misure_am_semperiodica_tmp")
        hiveContext.refreshTable(s"${_dbDest}.aggregazioni_misure_am_semperiodica_tmp")

        tempTbl_sem_periodica=s"${_dbDest}.aggregazioni_misure_am_semperiodica_tmp"

      }
    }

    thread1.start()
    thread2.start()


    if(thread1.isAlive)
      thread1.join()

    if(thread2.isAlive)
      thread2.join()

    log.info("Scrittura parallela tabelle temporanee completata")
    log.info(s"${tempTblTerna} + ${tempTbl_sem_periodica} ")

    //restituisce tutte le terne in cui trovo rettifiche per singolo giorno 
    //con verifica su tutte le 25 ore
    //SE NELLA QUERY NON INSERISCO LA COLONNA Kterna_DAY
    //NELLA SUCCESSIVA QUERY NON C'è BISOGNO DI INSERIRE IL DISTINCT
    //NEL CASO IN CUI NON VOGLIO INDICARE IL GIORNO IN CUI E' AVVENUTA LA RETTIFICA
    //SI PRESSUPPONE CHE NON ESISTANO TERNE CHE STIANO PARZIALMENTE SU rcu_tmo_aggregati_calcolati E rcu_tmo_aggregati_calc_sem
    //OPPURE TERNE PARZIALMENTE VALORIZZATE , CIOE CON GIORNI MANCANTI
    //ALTRIMENTI SI VERRABBERO A CREARE DEI DUPLICATI NEL DETTAGLIO PER GIORNO

    val queryCheckTerne_Day =
      s"""
         SELECT DISTINCT Kterna,Kterna_DAY,type_rett
         FROM (
         select TR.Kterna,TR.Kterna_DAY,
         CASE
         WHEN  nvl(SP.n_id_distr,0) = 0 THEN 'T_NEW'
         WHEN  nvl(SP.n_id_distr,0) <> 0 AND from_sem =1 and  TR.EA_TR <> SP.EA_SP then 'T_SEM'
         WHEN  nvl(SP.n_id_distr,0) <> 0 AND from_sem =0 and  TR.EA_TR <> SP.EA_SP then 'T_PER'
         ELSE '' END type_rett
         from (select Kterna_DAY,Kterna,EA_TR from ${tempTblTerna} ) TR
         left outer join ${tempTbl_sem_periodica} SP  ON TR.Kterna_DAY = SP.Kterna_DAY
         ) AS TBL
         WHERE type_rett <> ''
         DISTRIBUTE BY Kterna,Kterna_DAY
      """.stripMargin

		val dt_TerneWithRett_Days = hiveContext.sql(queryCheckTerne_Day)
    val terne_with_rett_days="terne_with_rett_days_"+annomese
    dt_TerneWithRett_Days.registerTempTable(terne_with_rett_days)
    tables_tmp+=terne_with_rett_days

    if(verbose) {
      log.info(s"""
        Individuazione terne con differenze di energia per ogni singolo giorno del mese in elaborazione
        ${queryCheckTerne_Day}
        """.stripMargin)
    }else log.info("Individuazione terne con differenze di energia per ogni singolo giorno del mese in elaborazione")


    val queryAll=
      s"""
         select DISTINCT  TR.n_id_udd,TR.n_id_distr,TR.n_id_distr_rif,TR.area,
         TR.pivadistributoreaggr,TR.giornoaggr,TR.dataelaborazione,TR.versione_orarie,
         TR.h1 ,TR.h2 ,TR.h3 ,TR.h4 ,TR.h5 ,TR.h6 ,TR.h7 ,TR.h8 ,TR.h9 ,TR.h10 ,TR.h11 ,TR.h12 ,TR.h13 ,
         TR.h14 ,TR.h15 ,TR.h16 ,TR.h17 ,TR.h18 ,TR.h19 ,TR.h20 ,TR.h21 ,TR.h22 ,TR.h23 ,TR.h24 ,TR.h25,
         CASE WHEN nvl(TR_RETT_DY.type_rett,'') = 'T_NEW' THEN '1' ELSE '0' END KT_new ,
         CASE WHEN nvl(TR_RETT_DY.type_rett,'') = 'T_SEM' THEN '1' ELSE '0' END KT_sem ,
         CASE WHEN nvl(TR_RETT_DY.type_rett,'') = 'T_PER' THEN '1' ELSE '0' END KT_periodica ,
         TR.annoaggr,TR.meseaggr,TR.sessione
         from ${tempTblTerna} TR
         INNER JOIN (SELECT DISTINCT A.Kterna FROM ${terne_with_rett_days} A) TR_RETT ON TR.Kterna = TR_RETT.Kterna
         LEFT OUTER JOIN ${terne_with_rett_days} TR_RETT_DY ON  TR.Kterna_DAY = TR_RETT_DY.Kterna_DAY

       """.stripMargin

    if(verbose) {
      log.info(s"""
        Estrazione terne da scrivere :
        ${queryAll}
        """.stripMargin)
    }else log.info("Estrazione terne da scrivere")


    val dt_TerneRett = hiveContext.sql(queryAll)
    return (dt_TerneRett,tables_tmp)



	}

	def runSemParz(hiveContext:HiveContext,tblAggrOra:String,dtAggrOra:DataFrame,checkSemParz:Boolean,uidElab:Long,misure_escluse:String,isMonthOracle:Boolean):(DataFrame,DataFrame,ListBuffer[String]) = {

		//la seguente query mi trova le terne associate ai pod rettificati
		//nell'aggregazione oraria appena eseguita +
		//le terne apperteneti al pod nella tabella aggregazioni_misure_orarie_ver
		log.info("*** Avvio individuzione terne ***")

		//il dataframe ${tblAggrOra} ha già il pod a 14 caratteri

    var tables_tmp:ListBuffer[String]=new ListBuffer[String]()
		val td1=hiveContext.sql(s" select CONCAT(n_id_distr ,'_', n_id_udd ,'_', area ,'_', n_id_distr_rif) K_TERNA,t_ore.pod,CONCAT(t_ore.anno,t_ore.mese,t_ore.giorno,t_ore.pod) AS KC from ${tblAggrOra} t_ore") //DISTRIBUTE BY KC

    log.info("Caching temporanea pod aggregati")
    val td1x=td1.persist(StorageLevels.MEMORY_AND_DISK_SER)
    val tblpods="tbl_pods_sem_tmp_"+annomese
    td1x.registerTempTable(tblpods)
    tables_tmp+=tblpods


    if(verbose) {
      log.info(
        s"""Individuazione pod aggregati per giorno (vista temporanea: ${tblpods}) :
             select CONCAT(n_id_distr ,'_', n_id_udd ,'_', area ,'_', n_id_distr_rif) K_TERNA,t_ore.pod,CONCAT(t_ore.anno,t_ore.mese,t_ore.giorno,t_ore.pod) AS KC from ${tblAggrOra} t_ore
             """)

      log.info("Aggiunga chiave KC in " + tblpods + " : col(anno)+col(mese)+col(giorno)+col(pod)")
			log.info("Aggiunga chiave K_TERNA in " + tblpods + " : CONCAT(n_id_distr ,'_', n_id_udd ,'_', area ,'_', n_id_distr_rif)")
    }else log.info("Individuazione pod aggregati per giorno")


   //val tblpods_distinct=s"(select distinct t_ore.pod from ${tblAggrOra} t_ore)"
   val tblpods_distinct=s"(select distinct t_ore.pod from ${tblpods} t_ore)"



    var tbl_ver_sem=""
    var queryFindTerne:String=""

    var dtVer_Sem:DataFrame=null

		var tbl_verifica=if(calcSemOraclePraticheR && isMonthOracle)s"${_dbDest}.${calcSemOraclePraticheR_Table}" else s"${_dbDest}.aggregazioni_misure_orarie_ver"
		var tbl_verifica_where=if(calcSemOraclePraticheR && isMonthOracle)s" and ver.rank = 1 " else ""
		val queryMisEsclusi=if(misure_escluse!="") " and CONCAT(SUBSTR(pod,1,14),time_stamp,pivadistributore,pivautente,codcontrdisp) NOT IN (" + misure_escluse +")" else ""


    val tbl_orarie_ver="aggregazioni_misure_orarie_ver_"+annomese

    hiveContext.sql(s"DROP TABLE IF EXISTS ${tbl_orarie_ver}")
    val query_Ver=
      s"""
         select *,CONCAT(ver.anno,ver.mese,ver.giorno,SUBSTR(ver.pod,1,14)) AS KC,
         CONCAT(ver.n_id_distr ,'_', ver.n_id_udd ,'_', ver.area ,'_', ver.n_id_distr_rif) K_TERNA
         from ${tbl_verifica} ver
         WHERE ver.anno=${_annoAggr} and ver.mese =${_meseAggr} and ver.flag_validazione='Y' ${tbl_verifica_where} ${queryMisEsclusi}
         DISTRIBUTE BY KC,K_TERNA
      """.stripMargin

    hiveContext.sql(s"CREATE TABLE ${tbl_orarie_ver} STORED AS PARQUET AS " + query_Ver)
    val dt_ver = hiveContext.sql(s"select * from ${tbl_orarie_ver}")


    log.info("Estrazione misure da aggregazioni_misure_orarie_ver per l'anno e mese in elaborazione")

    if(verbose) {
			log.info(query_Ver)

      log.info("Aggiunga chiave KC in vista temporanea " + tbl_orarie_ver + " : col(anno)+col(mese)+col(giorno)+col(pod)")
    }

		//PER I MESI ORACLE VIENE UTILIZZATA UNA VISTA IN CUI SONO PRESENTI LE RIGHE DELLA SEM E PERIODICA
		if(checkSemParz && !isMonthOracle) {
			log.info("*** Individuazione terne da aggregazioni_misure_orarie_sem_committed ***")


			dt_ver.registerTempTable(tbl_orarie_ver)
			tables_tmp+=tbl_orarie_ver

      hiveContext.sql(s"DROP TABLE IF EXISTS tmp_aggregazioni_misure_orarie_sem_committed_${annomese}")
      val querytmp=
        s"""
          CREATE TABLE tmp_aggregazioni_misure_orarie_sem_committed_${annomese} STORED AS PARQUET AS
          select  pivadistributore  ,pivautente , SUBSTR(pod,1,14)pod , giorno , area ,validato ,nomefile , codcontrdisp ,
          coduc ,tipodato_e ,tipodato_s ,tensione ,trattamento_o ,potcontrimpl ,potdisp ,cifreatt ,cifrerea ,
          raccolta ,potmax ,perdita ,annomesegiornodir ,
          h1 ,h2 ,h3 ,h4 ,h5 ,h6 ,h7 ,h8 ,h9 ,h10 ,h11 ,h12 ,
          h13 ,h14 ,h15 ,h16 ,h17 ,h18 ,h19 ,h20 ,h21 ,h22 ,h23 ,h24 ,h25 ,
          time_stamp ,versione ,dataelaborazione ,flaguddpod , stato , trattamento , flagarea , n_id_udd ,
          t_piva ,n_id_distr , n_id_distr_rif , flag_validazione,anno,mese,sessione,
          CONCAT(sem_com.anno,sem_com.mese,sem_com.giorno,sem_com.pod) AS KC,
          CONCAT(sem_com.n_id_distr ,'_',  sem_com.n_id_udd ,'_', sem_com.area ,'_', sem_com.n_id_distr_rif) K_TERNA,
		      uid_elab
		      from ${_dbDest}.aggregazioni_misure_orarie_sem_committed sem_com
		      where anno=${_annoAggr} and mese =${_meseAggr} and sessione like '%S2%' ${queryMisEsclusi}
        """.stripMargin
      hiveContext.sql(querytmp)

      if(verbose)
        log.info("Estrazione aggregazioni orarie sem committate da aggregazioni_misure_orarie_sem_committed sessione S2 con memorizzazione in tmp_aggregazioni_misure_orarie_sem_committed")

      hiveContext.sql(s"DROP TABLE IF EXISTS tmp_aggr_sem_${annomese}")
      val querytmp2=
        s"""
           CREATE TABLE tmp_aggr_sem_${annomese} stored as parquet as select max(uid_elab)max_uid_elab,n_id_distr,n_id_udd, area
           from tmp_aggregazioni_misure_orarie_sem_committed_${annomese} group by n_id_distr,n_id_udd, area
         """.stripMargin
      hiveContext.sql(querytmp2)

      if(verbose)
        log.info("Estrazione massima data di elaborazione terne orarie sem committate (n_id_distr,n_id_udd, area)")

			//INDIVIDUAZIONE ULTIME TERNE COMMITTATE RELATIVE ALL'ULTIMA SEM(sessione 2)
			val q_agr_ora_sem =
				s""" select sem.pivadistributore,sem.pivautente, sem.pod, sem.anno, sem.mese,sem.giorno, sem.area, sem.validato ,sem.nomefile, sem.codcontrdisp,sem.coduc,
          sem.tipodato_e, sem.tipodato_s,sem.tensione, sem.trattamento_o, sem.potcontrimpl, sem.potdisp, sem.cifreatt, sem.cifrerea, sem.raccolta, sem.potmax,
          sem.perdita, sem.annomesegiornodir, sem.h1,sem.h2,sem.h3,sem.h4,sem.h5,sem.h6,sem.h7,sem.h8,sem.h9,sem.h10,sem.h11,sem.h12,sem.h13,sem.h14,
          sem.h15,sem.h16,sem.h17,sem.h18,sem.h19,sem.h20,sem.h21,sem.h22,sem.h23,sem.h24,sem.h25,sem.time_stamp, sem.versione,sem.dataelaborazione,
          sem.flaguddpod, sem.stato,sem.trattamento,sem.flagarea, sem.n_id_udd, sem.t_piva, sem.n_id_distr, sem.n_id_distr_rif ,sem.flag_validazione,
          sem.KC,sem.K_TERNA
          from (select  sem_com.*
		      from tmp_aggregazioni_misure_orarie_sem_committed_${annomese} sem_com
          INNER JOIN tmp_aggr_sem_${annomese} TMP
          ON sem_com.uid_elab=TMP.max_uid_elab AND sem_com.n_id_distr = TMP.n_id_distr AND sem_com.n_id_udd = TMP.n_id_udd AND sem_com.area = TMP.area
		     ) as sem  DISTRIBUTE BY KC
  """.stripMargin

      if(verbose) {
        log.info(q_agr_ora_sem)
      }


      val terne_sem_committed="terne_sem_committed_"+annomese
      hiveContext.sql(s"DROP TABLE IF EXISTS ${terne_sem_committed}")
      hiveContext.sql(s"CREATE TABLE ${terne_sem_committed} STORED AS PARQUET AS " + q_agr_ora_sem)
      hiveContext.sql(s"DROP TABLE IF EXISTS tmp_aggregazioni_misure_orarie_sem_committed_${annomese}")
      hiveContext.sql(s"DROP TABLE IF EXISTS tmp_aggr_sem_${annomese}")

      val td2 = hiveContext.sql(s"SELECT * FROM ${terne_sem_committed}")

			td2.registerTempTable(terne_sem_committed)
      tables_tmp+=terne_sem_committed

			/*
			 PRENDO I DATI A PARITA DI POD PER IL MESE E ANNO E GIORNO IN ESAME
			 DALLA SEM COMMITTATA ALTRIMENTI DA aggregazioni_misure_orarie_ver

        ESTRAZIONE CURVE ORARIE DA VER ESCLUDENDO LE CURVE GIA ESTRATTE DA SEM
       */


			//per l'anno e mese in corso viene ricercato se esiste un file di testo contenente
			// una join custom tra le terne committate e la vista della periodica

			val file_queryJoinCustomTerne = prop.getProperty("spark.app.join.ternecommitted.sem.custom") + s"join_terne_sem_custom_${annomese}.txt"
			val hdfsConf = new Configuration()
			val fs = FileSystem.get(hdfsConf)
			val path = new Path(file_queryJoinCustomTerne)

			val query_join_terne :String = if(fs.exists(path)){

				val stream = fs.open(path)
				def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
				val query : String = readLines.takeWhile(_ != null).mkString("\n")
				if(query.trim=="")
					{
						s"""
		        FROM ${tbl_orarie_ver} ver
            left outer JOIN (select KC FROM ${terne_sem_committed}) sem_c ON ver.KC=sem_c.KC
            where sem_c.KC is null
            """
					}
				else
				  query
			}else {
				s"""
		        FROM ${tbl_orarie_ver} ver
            left outer JOIN (select KC FROM ${terne_sem_committed}) sem_c ON ver.KC=sem_c.KC
            where sem_c.KC is null
         """
			}

			val queryGetOrarieFromSEM_Ver =
				s"""
          SELECT ver.pivadistributore,ver.pivautente, SUBSTR(ver.pod,1,14)pod, ver.anno, ver.mese,ver.giorno, ver.area, ver.validato ,ver.nomefile, ver.codcontrdisp,ver.coduc,
          ver.tipodato_e, ver.tipodato_s,ver.tensione, ver.trattamento_o, ver.potcontrimpl, ver.potdisp, ver.cifreatt, ver.cifrerea, ver.raccolta, ver.potmax,
          ver.perdita, ver.annomesegiornodir, ver.h1,ver.h2,ver.h3,ver.h4,ver.h5,ver.h6,ver.h7,ver.h8,ver.h9,ver.h10,ver.h11,ver.h12,ver.h13,ver.h14,
          ver.h15,ver.h16,ver.h17,ver.h18,ver.h19,ver.h20,ver.h21,ver.h22,ver.h23,ver.h24,ver.h25,ver.time_stamp, ver.versione,ver.dataelaborazione,
          ver.flaguddpod, ver.stato,ver.trattamento,ver.flagarea, ver.n_id_udd, ver.t_piva, ver.n_id_distr, ver.n_id_distr_rif ,ver.flag_validazione,
          ver.KC,ver.K_TERNA
          ${query_join_terne}
  """.stripMargin


      log.info("Estrazione misure da aggregazioni_misure_orarie_ver non presenti in aggregazioni_misure_orarie_sem_committed con fusione")
			if(verbose) {
				log.info(queryGetOrarieFromSEM_Ver)
			}

      dtVer_Sem =hiveContext.sql(queryGetOrarieFromSEM_Ver).unionAll(td2).persist(StorageLevels.MEMORY_ONLY_SER)

      tbl_ver_sem = "aggregazioni_misure_orarie_ver_sem_"+annomese
      dtVer_Sem.registerTempTable(tbl_ver_sem)
      tables_tmp+=tbl_ver_sem

		}else {


      dtVer_Sem=dt_ver.persist(StorageLevels.MEMORY_ONLY_SER)
      tbl_ver_sem=tbl_orarie_ver
			dtVer_Sem.registerTempTable(tbl_ver_sem)
			tables_tmp+=tbl_ver_sem

		}


    queryFindTerne =
      s"""
         select distinct K_TERNA from
          (
           select A1.K_TERNA from ${tblpods} A1
           union all
           select ver.K_TERNA
           from ${tbl_ver_sem} ver
					 inner join ${tblpods_distinct} tbl_pods
					 on  SUBSTR(ver.pod,1,14)  = SUBSTR(tbl_pods.pod,1,14)
          ) as tbl
      """.stripMargin

    //queryFindTerne = "("+queryFindTerne+")"

		if(verbose) {
			log.info(
				s"""Individuazione terne tra aggregazioni_misure_orarie_ver ed i pod orari aggregati   :
                ${queryFindTerne}
             """)
		}

		log.info("*** Individuzione pod rimanenti in aggregazioni_misure_orarie_ver da terne aggregate  ***")


   val dtTerne = hiveContext.sql(queryFindTerne)

    val dtTernex=dtTerne.persist(StorageLevels.MEMORY_AND_DISK_SER)
    queryFindTerne =s"tbl_terneaggregate_sem_tmp_"+annomese
    dtTernex.registerTempTable(queryFindTerne)
    tables_tmp+=queryFindTerne


		//la seguente query prende da aggregazioni_misure_orarie_ver
		// tutti i pod per le terne ricavate sopra escludendo i pod già calcolati

		val queryGetAllPod=s"""
    select
    ver.pivadistributore,ver.pivautente , SUBSTR(ver.pod,1,14)pod ,ver.anno , ver.mese, ver.giorno , ver.area ,
    ver.validato ,ver.nomefile , ver.codcontrdisp ,
    ver.coduc ,ver.tipodato_e ,ver.tipodato_s ,ver.tensione ,ver.trattamento_o ,ver.potcontrimpl ,
    ver.potdisp ,ver.cifreatt ,ver.cifrerea ,ver.raccolta ,ver.potmax ,ver.perdita ,ver.annomesegiornodir ,
    ver.h1 ,ver.h2 ,ver.h3 ,ver.h4 ,ver.h5 ,ver.h6 ,ver.h7 ,ver.h8 ,ver.h9 ,ver.h10 ,ver.h11 ,ver.h12 ,ver.h13 ,
    ver.h14 ,ver.h15 ,ver.h16 ,ver.h17 ,ver.h18 ,ver.h19 ,ver.h20 ,ver.h21 ,ver.h22 ,ver.h23 ,ver.h24 ,ver.h25 ,
    ver.time_stamp ,${uidElab} versione, ver.versione versione_import,ver.dataelaborazione ,ver.flaguddpod , ver.stato , ver.trattamento , ver.flagarea , ver.n_id_udd ,
    ver.t_piva ,ver.n_id_distr , ver.n_id_distr_rif , ver.flag_validazione,'0' as da_rett
    from ${tbl_ver_sem} ver
    inner join ${queryFindTerne} terne_aggregate on ver.K_TERNA = terne_aggregate.K_TERNA
    left outer join ${tblpods} tbl_pods on ver.KC = tbl_pods.KC
    where tbl_pods.pod is null
    """.stripMargin

    if(verbose) {
      log.info(s"${queryGetAllPod}")
    }

    //on ver.anno = tbl_pods.anno and ver.mese =tbl_pods.mese and ver.giorno = tbl_pods.giorno and ver.pod  = tbl_pods.pod
    //where ver.anno=${_annoAggr} and ver.mese =${_meseAggr} and flag_validazione='Y' and tbl_pods.pod is null

		val dtPodReimportati=hiveContext.sql(queryGetAllPod)

		log.info("*** Fusione dei pod calcolati con i pod ricavati per l'anno : " +_annoAggr +" e mese : " + _meseAggr+" ***")
		val dtPodTot=dtPodReimportati.unionAll(dtAggrOra)


		return (dtPodTot,dtVer_Sem,tables_tmp)

	}



  def checkDataAndBuildRows(dt:DataFrame,uidElab:Long, dataelaborazione:java.sql.Timestamp,isMonthOracle:Boolean,distr_oracle:Broadcast[Map[String,String]],pars_validz:Broadcast[Map[String,String]]): RDD[Row] ={


		val rdd = dt.map { r =>


			val flaguddpodApp_B:Boolean=pars_validz.value("flaguddpodApp").toBoolean
			val trattamentoApp_B:Boolean=pars_validz.value("trattamentoApp").toBoolean
			val statoApp_B:Boolean=pars_validz.value("statoApp").toBoolean
			val validatoApp_B:Boolean=pars_validz.value("validatoApp").toBoolean
			val flagAreaApp_B:Boolean=pars_validz.value("flagAreaApp").toBoolean
			val distrAziendaApp_B:Boolean=pars_validz.value("distrAziendaApp").toBoolean
			val estraiAreaApp_B:Boolean=pars_validz.value("estraiAreaApp").toBoolean

			val queryPs1_B =pars_validz.value("queryPs1")
			val queryPs11_B =pars_validz.value("queryPs11")
			val queryPs2_B =pars_validz.value("queryPs2")
			val queryPs4_B =pars_validz.value("queryPs4")
			val queryPs3_B =pars_validz.value("queryPs3")


			val pivaDistrib: String = "000000".concat(r.getString(0)) takeRight 11
			val pivautente: String = r.getString(1) //"000000".concat(r.getString(1))  takeRight 11
			val pod: String = r.getString(2).substring(0,14)
			val anno: Int = r.getInt(3)
			val mese: Int = r.getInt(4) //s(4).toString
			val giorno: Int = Try(r.getInt(5)).getOrElse(0)
			if(giorno==0){
        val l= List()
        Row.fromSeq(l)
      }else {
        //    						val area:String =  r.getString(6).toString
        val validato: String = r.getString(7)
        val nomeFile: String = r.getString(8)
        val codcontrdisp: String = r.getString(9)
        val coduc: String = r.getString(10)
        val tipoE: Int = r.getInt(11)
        val tipoS: Int = r.getInt(12)
        val tensione: Double = r.getDouble(13)
        val trattO: String = r.getString(14)
        val potcontrimpl: Double = if (r.isNullAt(15)) 0 else r.getDouble(15)
        val potdisp: Double = if (r.isNullAt(16)) 0 else r.getDouble(16)
        val cifreatt: Int = if (r.isNullAt(17)) 0 else r.getInt(17)
        val cifrerea: Int = r.getInt(18)
        val raccolta: String = r.getString(19)
        val potmax: Double = r.getDouble(20)

        val perdita: Double = if (r.getDouble(21) == 219.999) 0.011 else r.getDouble(21)
        val annomesegiornodir: Int = r.getInt(22)
        val timestamp: Long = r.getLong(123)


        val area: String = if (estraiAreaApp_B) {
          estrazioneArea(pod, queryPs3_B)
        } else {
          r.getString(6).toString
        }


        val valUdd = if (flaguddpodApp_B) {
          if (isMonthOracle) {
            val tipo_pratica = r.getAs[String]("tipo_pratica").trim()
            if (tipo_pratica == "R")
              validazioneOrariePodUdd(pod, anno.toString, ("0".concat(mese.toString) takeRight 2), ("0".concat(giorno.toString) takeRight 2), queryPs1_B)
            else
              getNIDUdd(codcontrdisp, queryPs11_B)
          }
          else {
            validazioneOrariePodUdd(pod, anno.toString, ("0".concat(mese.toString) takeRight 2), ("0".concat(giorno.toString) takeRight 2), queryPs1_B)
          }
        } else {
          getNIDUdd(codcontrdisp, queryPs11_B) // ("SK","SK","SK") //
        }
        val valFlag = if (statoApp_B || trattamentoApp_B) {
          validazioneStatoPod(pod, queryPs2_B)
        } else if (isMonthOracle && trattO.trim == "O")
          ("Y", "Y")
        else {
          ("SK", "SK")
        }
        val valFlagArea = if (flagAreaApp_B) {
          validazioneArea(pod, area, queryPs4_B)
        } else {
          "SK"
        }
        val distrAzienda = if (distrAziendaApp_B) {
          getDistrAzienda(pivaDistrib, distr_oracle)
        } else {
          ("SK", "SK")
        }

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
        val flaguddpodVal = if (flaguddpodApp_B) {
          flaguddpod.equals("Y")
        } else {
          true
        }
        val trattamentoVal = if (trattamentoApp_B || isMonthOracle) {
          trattamento.toUpperCase.equals("Y")
        } else {
          true
        }
        val validatoVal = if (validatoApp_B) {
          validato.toUpperCase().equals("S")
        } else {
          true
        }
        val flagAreaVal = if (flagAreaApp_B) {
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
        val validazioni = List(timestamp, uidElab, uidElab, dataelaborazione, flaguddpod, stato, trattamento, valFlagArea, nIdUdd, tPiva, nIdDistr, nIdDistrRif, flagValidazioni)

        Row.fromSeq(info ++ hnValues ++ validazioni)
      }
		}

		rdd.filter(r=>r.length>0)
	}


	// Aggregazione per i mesi da agosto 2018 in poi (flusso_misure_quarti)


	def writePodsOnOracle(dtOrarie:DataFrame): Unit ={

		log.info("Avvio scrittura elenco pods su Oracle PRT_TMO_AGGR_SEM_POD_CONS")

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

    def tStamp(s: Long): Option[java.sql.Timestamp] = {
      val str = Option(s).getOrElse(return None)
      val parsedDate = sdf.parse(s.toString)
      val timestamp = new java.sql.Timestamp(parsedDate.getTime)
      Some(timestamp)
    }

    def toInt(s: String): Option[Int] = {
      val str = Option(s).getOrElse(return None)
      Some(Try(str.toString.toInt).getOrElse(0))
    }


    val toTimeStamp = udf[Option[java.sql.Timestamp], Long](tStamp)
    val toIntUDF = udf[Option[Int], String](toInt)


    val aggrCalc = dtOrarie
			.select(
        col("n_id_distr").cast(StringType).alias("N_ID_DISTR"),
        col("n_id_udd").cast(StringType).alias("N_ID_UDD"),
				col("t_area_rif").alias("T_AREA_RIF"),
				col("annomese").alias("ANNOMESE"),
				col("codice_pod").alias("CODICE_POD"),
        col("n_id_distr_rif").cast(StringType).alias("N_ID_DISTR_RIF"),
				col("n_id_file").alias("N_ID_FILE"),
        toTimeStamp(col("d_ricezione")).alias("D_RICEZIONE"),
				col("tipo_pratica").alias("TIPO_PRATICA"),
				col("consumo").cast(StringType).alias("CONSUMO"),
				col("uid_elab").cast(StringType).alias("UID_ELAB"),
				col("sessione").alias("SESSIONE"),
				col("tensione").cast(StringType).alias("TENSIONE"),
				col("flg_da_aggregare").alias("FLG_DA_AGGREGARE"),
				col("perdita").cast(StringType).alias("PERDITA")
			)


		val connectionProperties = new Properties()
		connectionProperties.put("user", user)
		connectionProperties.put("password", password)
		connectionProperties.setProperty("Driver", driver)

		aggrCalc
			.write
			.mode(SaveMode.Append)
			.jdbc(url, "TMPOD_CLOUD.PRT_TMO_AGGR_SEM_POD_CONS", connectionProperties)

    log.info("Scrittura elenco pods su Oracle PRT_TMO_AGGR_SEM_POD_CONS Completata")
	}
	def writeTernaOnOracle(dfTerna:DataFrame): Unit ={


		log.info("Avvio scrittura terne su Oracle PRT_TMO_AGGREGATI_CALC_SEM")


		val refill:String => String = ( f => ("0".concat(f.toString()) takeRight 2) )
		val refillUDF = udf(refill)

		val rounding:Double => Double = ( BigDecimal( _ ).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble )
		val roundingUDF = udf(rounding)

		val toint:Int => Int = ( _.toInt )
		val toIntUDF = udf(toint)

		val aggrCalc = dfTerna
			.select(
				col("n_id_distr").cast(StringType).alias("N_ID_DISTR"),
				col("area").alias("T_AREA_RIF"),
				toIntUDF(concat(col("annoaggr"),
					refillUDF(col("meseaggr")))).alias("ANNOMESE"),
				col("n_id_udd").cast(StringType).alias("N_ID_UDD"),
				col("giornoaggr").alias("GIORNO"),
				roundingUDF(col("h1")).alias("N_H1"),
				roundingUDF(col("h2")).alias("N_H2"),
				roundingUDF(col("h3")).alias("N_H3"),
				roundingUDF(col("h4")).alias("N_H4"),
				roundingUDF(col("h5")).alias("N_H5"),
				roundingUDF(col("h6")).alias("N_H6"),
				roundingUDF(col("h7")).alias("N_H7"),
				roundingUDF(col("h8")).alias("N_H8"),
				roundingUDF(col("h9")).alias("N_H9"),
				roundingUDF(col("h10")).alias("N_H10"),
				roundingUDF(col("h11")).alias("N_H11"),
				roundingUDF(col("h12")).alias("N_H12"),
				roundingUDF(col("h13")).alias("N_H13"),
				roundingUDF(col("h14")).alias("N_H14"),
				roundingUDF(col("h15")).alias("N_H15"),
				roundingUDF(col("h16")).alias("N_H16"),
				roundingUDF(col("h17")).alias("N_H17"),
				roundingUDF(col("h18")).alias("N_H18"),
				roundingUDF(col("h19")).alias("N_H19"),
				roundingUDF(col("h20")).alias("N_H20"),
				roundingUDF(col("h21")).alias("N_H21"),
				roundingUDF(col("h22")).alias("N_H22"),
				roundingUDF(col("h23")).alias("N_H23"),
				roundingUDF(col("h24")).alias("N_H24"),
				roundingUDF(col("h25")).alias("N_H25"),
				col("dataelaborazione").alias("D_DATA_AGGREGAZIONE"),
				lit("N").alias("T_AGGR_SOTTESI"),//default 'N'
				col("n_id_distr_rif").cast(StringType).alias("N_ID_DISTR_RIF"),
				col("uid_elab").alias("UID_ELAB"),
				col("versione_orarie").alias("UID_ELAB_ORARIE"),
				col("sessione").alias("SESSIONE")
			)


		val connectionProperties = new Properties()
		connectionProperties.put("user", user)
		connectionProperties.put("password", password)
		connectionProperties.setProperty("Driver", driver)

		aggrCalc
			.write
			.mode(SaveMode.Append)
			.jdbc(url, "TMPOD_CLOUD.PRT_TMO_AGGREGATI_CALC_SEM", connectionProperties)


    log.info("Scrittura terne su Oracle PRT_TMO_AGGREGATI_CALC_SEM Completata")
	}
	def goAggregazione(uidElab:Long, dataelaborazione:java.sql.Timestamp,pathOrarieSEM:String ,pathAmSEM:String,pathTerneSEM:String,checkSemParz:Boolean,distr_oracle:Broadcast[Map[String,String]],isMonthOracle:Boolean): Unit = {


		try {

      //val hiveCtx2=hiveCtx.newSession()
			val hiveCtx2=hiveCtx

      val hdfsConf = new Configuration()
      val fshdfs = FileSystem.get(hdfsConf)

      //AVVIO CANCELLAZIONE PARTIZIONE
      try{

        val q_drop_part:String=s"ALTER TABLE ${_dbDest}.aggregazioni_misure_orarie_sem DROP IF EXISTS PARTITION(anno=${_annoAggr},mese=${_meseAggr},sessione='${Sessione_SEM}')"
        hiveCtx2.sql(q_drop_part)
      }
      catch {
        case e: Exception =>
          val msg =e.getMessage
      }

      try{
        val q_drop_part2:String=s"ALTER TABLE ${_dbDest}.aggregazioni_misure_am_sem DROP IF EXISTS PARTITION(annoaggr=${_annoAggr},meseaggr=${_meseAggr},sessione='${Sessione_SEM}')"
        hiveCtx2.sql(q_drop_part2)
        }
      catch {
      case e: Exception =>
        val msg = e.getMessage
      }

      try {
        val q_drop_part3: String = s"ALTER TABLE ${_dbDest}.prt_tmo_mo_terne_sem DROP IF EXISTS PARTITION(anno=${_annoAggr},mese=${_meseAggr},sessione='${Sessione_SEM}')"
        hiveCtx2.sql(q_drop_part3)
      }
      catch {
      case e: Exception =>
        val msg = e.getMessage
      }

        hiveCtx2.sql(s"MSCK REPAIR TABLE ${_dbDest}.aggregazioni_misure_orarie_sem")
        hiveCtx2.refreshTable(_dbDest+".aggregazioni_misure_orarie_sem")

        hiveCtx2.sql(s"MSCK REPAIR TABLE ${_dbDest}.aggregazioni_misure_am_sem")
        hiveCtx2.refreshTable(_dbDest+".aggregazioni_misure_am_sem")

        hiveCtx2.sql(s"MSCK REPAIR TABLE ${_dbDest}.prt_tmo_mo_terne_sem")
        hiveCtx2.refreshTable(_dbDest+".aggregazioni_misure_am_sem")


      creazioneVistaStatoPod(annomese)

			//spark.app.aggregazioni.file.sem.esclusi restituisce il path dei file da escludere
			val filesEsclusi = if (!isMonthOracle)
				prop.getProperty("spark.app.aggregazioni.file.sem.esclusi") + s"file_da_escludere_${annomese}.txt"
			else
				prop.getProperty("spark.app.aggregazioni.file.sem.esclusi") + s"file_da_escludere_oracle.txt"
			log.info(s"*** fileEsclusi: ${filesEsclusi}")

			val filesEsclusiRdd = sc.textFile(filesEsclusi).collect().toList

			val filesEsclusiQuery = if (filesEsclusiRdd.size > 0) {
				s" and nomefile not in (${filesEsclusiRdd.map(x => "'" + x + "'").mkString(",")})"
			} else {
				""
			}

			val misure_escluse_list=getInvalidazioni(_annoAggr,_meseAggr)
			val misure_escluse = if(misure_escluse_list!="") " AND CONCAT(SUBSTR(podquarti,1,14),time_stamp,pivadistributorequarti,pivautentequarti,codcontrdispquarti) NOT IN (" + misure_escluse_list + ")" else ""

			val whereCond =
        if(isMonthOracle){
					/*val whereFilter:String = if(calcSemOraclePraticheR) {
						if(_annoAggr==2017)
						" and (tipo_pratica ='R' AND annomesegiornodir BETWEEN 20180421 AND " + (if(Ghigliottina_Max!="0") Ghigliottina_Max else "20190420") + " )"
						else if(_annoAggr==2018 && _meseAggr >=1 && _meseAggr <7)
							" and (tipo_pratica ='R' AND annomesegiornodir BETWEEN 20181021 AND " + (if(Ghigliottina_Max!="0") Ghigliottina_Max else "20190420") + ")"
						else
						" and (tipo_pratica ='R' AND annomesegiornodir <= " + (if(Ghigliottina_Max!="0") Ghigliottina_Max else "20190420") + ")"
					} else
						{
							if(Ghigliottina_Max!="0") s" and annomesegiornodir <= ${Ghigliottina_Max} "
							else ""
						}*/
					val annox = anno_iniziale + 1
					val whereFilter:String = if(calcSemOraclePraticheR) {
					   s" and (tipo_pratica ='R' AND annomesegiornodir BETWEEN ${anno_iniziale}0421 AND " + (if(Ghigliottina_Max!="0") Ghigliottina_Max else s"${annox}0420") + " )"
					} else
					{
						if(Ghigliottina_Max!="0") s" and annomesegiornodir <= ${Ghigliottina_Max} "
						else ""
					}


            s"annoquarti=${_annoAggr} and mesequarti=${_meseAggr} and (pivadistributorequarti is not null) ${whereFilter} ${filesEsclusiQuery}  ${misure_escluse}"

        }
        else if (!isSemForceTot) {
				if (!checkSemParz)//il flag checkSemParz mi indica i mesi da giugno dell'anno iniziale della SEM2 a ritroso per 5 anni  per la SEM1 è sempre false
          {
          val annomesegiornofinale=if(Ghigliottina_Max!="0")
            Ghigliottina_Max
          else if(Tipo_SEM=="SEM2")
            (anno_iniziale+1).toString+"0420"
          else
            (anno_iniziale).toString+"1020"

					s"annoquarti=${_annoAggr} and mesequarti=${_meseAggr} and (((nomefile like '%PDO%' and annomesegiornodir > ${_listDistributori_Ghigliottina} and annomesegiornodir <= ${annomesegiornofinale}) or (nomefile like '%RFO%' and annomesegiornodir <= ${annomesegiornofinale})) and pivadistributorequarti is not null)  ${filesEsclusiQuery}  ${misure_escluse}"
          }
				else //SEM2
				{
					var annomesegiorno_da = ""
					var annomesegiorno_a = ""

					if (_annoAggr == anno_iniziale)
					{
						//dal 21 ottobre dell'anno iniziale della SEM2 al 20 aprile dell'anno successivo(cioè anno corrente di lancio)
						annomesegiorno_da = s"${anno_iniziale}1021"
						val annox = anno_iniziale + 1
						annomesegiorno_a = s"${annox}0420"
					}
					else //per gli anni precedenti a quello di start della SEM2 considero tutte le curve arrivate tra il 21/04/anno_iniziale SEM2 ed il 20/04/dell'anno successivo(cioè anno corrente di lancio)
					{
						annomesegiorno_da = s"${anno_iniziale}0421"
						val annox = anno_iniziale + 1
						annomesegiorno_a = s"${annox}0420"
					}
          if(Ghigliottina_Max!="0")
            annomesegiorno_a=Ghigliottina_Max

					val filter_s:String=s"((annomesegiornodir >= ${annomesegiorno_da}) and (annomesegiornodir <= ${annomesegiorno_a}))"
					s"annoquarti=${_annoAggr} and mesequarti=${_meseAggr} and (${filter_s} and pivadistributorequarti is not null)  ${filesEsclusiQuery}  ${misure_escluse}"
				}
			} else {
          if(Ghigliottina_Max!="0")
				    s"annoquarti=${_annoAggr} and mesequarti=${_meseAggr} and pivadistributorequarti IN (${_listDistributori_Ghigliottina}) and annomesegiornodir <= ${Ghigliottina_Max} ${filesEsclusiQuery}  ${misure_escluse}"
          else
            s"annoquarti=${_annoAggr} and mesequarti=${_meseAggr} and pivadistributorequarti IN (${_listDistributori_Ghigliottina})  ${filesEsclusiQuery}  ${misure_escluse}"
			}




			//AGGIORNAMENTO PER ESCLUDERE LE MISURE DA ANNULLARE IN FASE DI AGGREGAZIONE ORARIA

			//val query = queryFlussoMisureQuartiTimeStampMax.replace("WHERE_CONDITIONS", s" where ${whereCond}")
			val query_tmp =
				s"""
           LEFT OUTER JOIN (SELECT nomefile nf,pod,annomese from au.annullamento_pod_aggroraria where annomese=${annomese} and not(nomefile is null and pod is null)) mis_ann
           ON lcase(nomefile) = lcase(nvl(mis_ann.nf,nomefile)) and podquarti = nvl(mis_ann.pod,podquarti)
       """.stripMargin

			val query  = queryFlussoMisureQuartiTimeStampMax.replace("WHERE_CONDITIONS", s" ${query_tmp} where ${whereCond} and (mis_ann.annomese is null ) ")

			//FINE AGGIORNAMENTO

			//if(verbose)
			log.info(s"query sem spark.query.flusso_misure_quarti.time_stamp_max \n${query}")

			//val hiveContext = hiveCtx
			//val tt = hiveContext.sql(query)
			val tt = hiveCtx2.sql(query)

			log.info("***** max_time_stamp OK")


      if(verbose)
      {
        log.info(
          s"""
            FLAG VALIDAZIONI APPLICATI AL MESE ${_meseAggr} E ANNO ${_annoAggr}
            trattamentoApp -> ${trattamentoApp.toString} ,
            statoApp-> ${statoApp.toString} ,
            validatoApp-> ${validatoApp.toString} ,
            flagAreaApp-> ${flagAreaApp.toString} ,
            distrAziendaApp-> ${distrAziendaApp.toString} ,
            estraiAreaApp-> ${estraiAreaApp.toString}
          """.stripMargin)
      }
			val pars:Map[String,String] = Map("flaguddpodApp" -> flaguddpodApp.toString ,
				"trattamentoApp" -> trattamentoApp.toString ,
			  "statoApp"-> statoApp.toString ,
				"validatoApp"-> validatoApp.toString ,
				"flagAreaApp"-> flagAreaApp.toString ,
				"distrAziendaApp"-> distrAziendaApp.toString ,
				"estraiAreaApp"-> estraiAreaApp.toString,
			  "queryPs1"-> queryPs1,
				"queryPs11"-> queryPs11,
				"queryPs2"-> queryPs2,
				"queryPs4"-> queryPs4,
				"queryPs3"-> queryPs3)

			 val pars_valid=sc.broadcast(pars)

			 val rdd = checkDataAndBuildRows(tt, uidElab, dataelaborazione,isMonthOracle,distr_oracle,pars_valid)

			val dfQS_orarie =  hiveCtx2.createDataFrame(rdd, schemaOre_sem)

			log.info("***** creazione DataFrame misure ore OK")

      val tempTbl: String = "aggr_orario_" + annomese


      val dfQS1:DataFrame = if((!isSemForceTot && !isMonthOracle) || (isMonthOracle && calcSemOraclePraticheR) ) {
        log.info("***** validazione OrariePodUdd, StatoPodArea, DistrAzienda su calcolo parziale OK")
        dfQS_orarie.withColumn("da_rett", lit("1")).persist(StorageLevels.MEMORY_ONLY_SER)
      }
			else{
				log.info("***** validazione OrariePodUdd, StatoPodArea, DistrAzienda OK")
        dfQS_orarie.persist(StorageLevels.MEMORY_ONLY_SER)
      }



			// da questo punto in poi dobbiamo splittare la logica
			// tra la sem parziale e quella globale(per distributore)
      var tmp_tbls:ListBuffer[String]=new ListBuffer[String]()


      if(!isSemForceTot && !isMonthOracle || (isMonthOracle && calcSemOraclePraticheR)) {

        log.info("*** Avvio procedura di verifica aggregato orario anno : " + _annoAggr.toString+ " e mese : " + _meseAggr.toString + "***")

        dfQS1.registerTempTable(tempTbl)

        val rt_ral=runSemParz(hiveCtx2,tempTbl,dfQS1,checkSemParz,uidElab,misure_escluse_list,isMonthOracle)

        val dfQS1_1=rt_ral._1.withColumn("sessione",lit(Sessione_SEM))
        val dfver_sem=rt_ral._2
        tmp_tbls=rt_ral._3

        val TblRettOrarie: String = _dbDest + ".aggregazioni_misure_orarie_sem"

        log.info("***** Avvio scrittura nel parquet :" + pathOrarieSEM + " ***")

        dfQS1_1
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .partitionBy("anno", "mese", "sessione")
          .save(pathOrarieSEM)


        dfver_sem.unpersist()
        dfQS1.unpersist()

        hiveCtx2.sql("MSCK REPAIR TABLE " + TblRettOrarie)
        hiveCtx2.refreshTable(TblRettOrarie)
        log.info("***** insert misure ore OK")


        hiveCtx2.dropTempTable(tempTbl)
        if (tmp_tbls.length > 0) {
          for (el <- tmp_tbls) {
            hiveCtx2.dropTempTable(el)
          }
        }

        hiveCtx2.sql(s"DROP TABLE IF EXISTS aggregazioni_misure_orarie_ver_${annomese}")
        hiveCtx2.sql(s"DROP TABLE IF EXISTS terne_sem_committed_${annomese}")


        log.info("***** Avvio procedura di raggruppamento per terna anno :" + _annoAggr.toString + " e mese : " + _meseAggr.toString + " ***")

        val dtTerna= runAggregTerna(hiveCtx2,uidElab.toString,dataelaborazione,TblRettOrarie).toDF().
          withColumn("KT_new",lit("1")).
          withColumn("KT_sem",lit("1")).
          withColumn("KT_periodica",lit("1")).
          withColumn("sessione",lit(Sessione_SEM))

        val tmpTerneRett="aggr_terna_rett_calc_" + annomese
        dtTerna.registerTempTable(tmpTerneRett)

        val threadx = new Thread {
          override def run {
            log.info("***** Avvio estrazione terne in rettifica in parallelo ***")
            val q_terne_sem=
              s"""
             SELECT DISTINCT n_id_distr,n_id_udd,area puntodispacciamento,
             '${annomese}' as annomese,'${Tipo_SEM}' as tipo_sem,
             versione_orarie as d_elab ,${_annoAggr} as anno, ${_meseAggr} as mese ,
             '${Sessione_SEM}' as sessione
             FROM ${tmpTerneRett}
          """.stripMargin

            val dt_terne_rett= hiveCtx2.sql(q_terne_sem)

            log.info("***** Avvio scrittura terne nel parquet :" + pathTerneSEM + " ***")
            dt_terne_rett
              .write
              .format("parquet")
              .mode(SaveMode.Append)
              .partitionBy("anno", "mese","sessione")
              .save(pathTerneSEM)


            hiveCtx2.sql("MSCK REPAIR TABLE " + _dbDest + ".prt_tmo_mo_terne_sem")
            hiveCtx2.refreshTable(_dbDest + ".prt_tmo_mo_terne_sem")

          }
        }

        threadx.start

        //dtTerna.persist(StorageLevels.MEMORY_ONLY_SER)

        log.info("***** Avvio scrittura aggregazioni_misure_am_sem nel parquet :" + pathAmSEM + " ***")
        dtTerna
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .partitionBy("annoaggr", "meseaggr","sessione")
          .save(pathAmSEM)



        hiveCtx2.sql("MSCK REPAIR TABLE " + _dbDest + ".aggregazioni_misure_am_sem")
        hiveCtx2.refreshTable(_dbDest + ".aggregazioni_misure_am_sem")

        log.info("***** insert aggregazioni_misure_am_sem su hdfs OK")


        hiveCtx2.dropTempTable(tmpTerneRett)

        if(threadx.isAlive)
          threadx.join()
        log.info("***** insert prt_tmo_mo_terne_sem su hdfs OK")
        // dtTerna.unpersist()

        log.info("***** aggiornamento partizioni OK")
      }
      else{

        //nel caso di calcolo globale l'aggregazione per terna la eseguo sulla temp table dell'aggregato
        //poichè lo scriverò dopo

        //aggiungerò dopo la colonna da_rett e sessione
        //dal seguente dataframe estraggo le terne tramite una tempTable
        //aggiungo la colonna Kterna_DAY per trovare in seguito le righe della'aggregato
        //orario appartenenti alle terne in rettifica evidenziando cosi i pod in rettifica
        val dfQS1_b=dfQS1.
          withColumn("Kterna_DAY",concat(col("n_id_distr"),col("n_id_distr_rif"),col("n_id_udd"),col("area"),col("giorno"))).
          withColumn("sessione",lit(Sessione_SEM))
        dfQS1_b.registerTempTable(tempTbl)


        log.info("***** Avvio procedura di raggruppamento per terna anno :" + _annoAggr.toString + " e mese : " + _meseAggr.toString + " ***")

        val dtTerna=  runAggregTerna(hiveCtx2,uidElab.toString,dataelaborazione,tempTbl).toDF().
          withColumn("sessione",lit(Sessione_SEM))


        log.info("*** Avvio procedura di verifica terne aggregate in rettifica ***")

        val tempTblTerna: String = "aggr_terna_" + annomese

        val dtTerna_1=dtTerna.withColumn("Kterna",concat(col("n_id_distr"),col("n_id_distr_rif"),col("n_id_udd"),col("area"))).
          withColumn("EA_TR", concat(col("h1"),col("h2"),col("h3"),col("h4"),col("h5"),col("h6"),col("h7"),col("h8"),col("h9"),col("h10"),col("h11"),
                    col("h12"),col("h13"),col("h14"),col("h15"),col("h16"),col("h17"),col("h18"),col("h19"),col("h20"),col("h21"),col("h22"),col("h23"),col("h24"),col("h25")))
          .withColumn("Kterna_DAY",concat(col("n_id_distr"),col("n_id_distr_rif"),col("n_id_udd"),col("area"),col("giornoaggr")))

       // dtTerna_1.persist(StorageLevels.MEMORY_ONLY_SER)
        dtTerna_1.registerTempTable(tempTblTerna)


        if(verbose)
          {
            log.info("Aggiunta chiave in terne estratte : " +
              " Kterna = n_id_distr+n_id_distr_rif+n_id_udd+area "+
              " Kterna_DAY = n_id_distr+n_id_distr_rif+n_id_udd+area+giornoaggr" +
              " ")
          }
        //avvio procedura per verificare le terne in rettifica
        val rtv_val = FindTerneInRett(hiveCtx2,tempTblTerna)

        val dtTernaWithRett=rtv_val._1
        tmp_tbls=rtv_val._2

        //dtTernaWithRett.persist(StorageLevels.MEMORY_ONLY_SER)


        log.info("***** Avvio scrittura aggregazioni_misure_am_sem nel parquet :" + pathAmSEM + " ***")
        dtTernaWithRett
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .partitionBy("annoaggr", "meseaggr","sessione")
          .save(pathAmSEM)

        //dtTernaWithRett.unpersist()
        //dtTerna_1.unpersist()


        hiveCtx2.sql("MSCK REPAIR TABLE " + _dbDest + ".aggregazioni_misure_am_sem")
        hiveCtx2.refreshTable(_dbDest + ".aggregazioni_misure_am_sem")
        log.info("***** insert aggregazioni_misure_am_sem su hdfs OK")

        val threadx = new Thread {
          override def run {
            log.info("***** Avvio estrazione terne in rettifica in parallelo ***")
            val q_terne_sem =
              s"""
             SELECT DISTINCT n_id_distr,n_id_udd,area puntodispacciamento,
             '${annomese}' as annomese,'${Tipo_SEM}' as tipo_sem,
             versione_orarie as d_elab ,annoaggr as anno, meseaggr as mese , sessione
             FROM ${_dbDest}.aggregazioni_misure_am_sem
             where annoaggr=${_annoAggr} and meseaggr=${_meseAggr} and sessione='${Sessione_SEM}'
						 and (KT_new ='1' OR KT_sem='1' OR KT_periodica ='1')
          """.stripMargin

            val dt_terne_rett = hiveCtx2.sql(q_terne_sem)

            log.info("***** Avvio scrittura terne nel parquet :" + pathTerneSEM + " ***")
            dt_terne_rett
              .write
              .format("parquet")
              .mode(SaveMode.Append)
              .partitionBy("anno", "mese", "sessione")
              .save(pathTerneSEM)

            hiveCtx2.sql("MSCK REPAIR TABLE " + _dbDest + ".prt_tmo_mo_terne_sem")
            hiveCtx2.refreshTable(_dbDest + ".prt_tmo_mo_terne_sem")

          }
        }

        threadx.start

        log.info("*** Avvio procedura di individuazione pod orari in rettifica ***")

        val queryTerneWritted=s"""
                  select n_id_distr,n_id_distr_rif,n_id_udd,area,giornoaggr
                  from ${_dbDest}.aggregazioni_misure_am_sem
                  where annoaggr=${_annoAggr} and meseaggr=${_meseAggr} and sessione='${Sessione_SEM}'
                  and (KT_new ='1' OR KT_sem='1' OR KT_periodica ='1')
                  """

        val dtTerneWritted=hiveCtx2.sql(queryTerneWritted).
        withColumn("Kterna_DAY",concat(col("n_id_distr"),col("n_id_distr_rif"),col("n_id_udd"),col("area"),col("giornoaggr")))
        val tmpTerneRett="aggr_terna_rett_calc_" + annomese
        dtTerneWritted.registerTempTable(tmpTerneRett)


        //imposto dalle curve orarie la colonna da_rett=1 per tutte le terne restituite
        // che sono in rettifica

        val queryfilterPodOrari=s"""
                            select orarie.pivadistributore,orarie.pivautente , orarie.pod , orarie.giorno , orarie.area ,
                            orarie.validato ,orarie.nomefile , orarie.codcontrdisp ,
                            orarie.coduc ,orarie.tipodato_e ,orarie.tipodato_s ,orarie.tensione ,orarie.trattamento_o ,orarie.potcontrimpl ,
                            orarie.potdisp ,orarie.cifreatt ,orarie.cifrerea ,orarie.raccolta ,orarie.potmax ,orarie.perdita ,orarie.annomesegiornodir ,
                            orarie.h1 ,orarie.h2 ,orarie.h3 ,orarie.h4 ,orarie.h5 ,orarie.h6 ,orarie.h7 ,orarie.h8 ,orarie.h9 ,orarie.h10 ,orarie.h11 ,orarie.h12 ,orarie.h13 ,
                            orarie.h14 ,orarie.h15 ,orarie.h16 ,orarie.h17 ,orarie.h18 ,orarie.h19 ,orarie.h20 ,orarie.h21 ,orarie.h22 ,orarie.h23 ,orarie.h24 ,orarie.h25 ,
                            orarie.time_stamp ,orarie.versione,orarie.versione_import,orarie.dataelaborazione ,orarie.flaguddpod , orarie.stato , orarie.trattamento , orarie.flagarea , orarie.n_id_udd ,
                            orarie.t_piva ,orarie.n_id_distr , orarie.n_id_distr_rif , orarie.flag_validazione,
                            case when nvl(terne.Kterna_DAY,'')='' THEN '0' ELSE '1' END as da_rett,
                            orarie.anno , orarie.mese,orarie.sessione
                            from ${tempTbl} orarie  left outer join
                             ${tmpTerneRett} terne on orarie.Kterna_DAY = terne.Kterna_DAY
                            """
        val dfQS1_1=hiveCtx2.sql(queryfilterPodOrari)

        val TblRettOrarie: String = _dbDest + ".aggregazioni_misure_orarie_sem"

        log.info("***** Avvio scrittura aggregato orario nel parquet :" + pathOrarieSEM + " ***")

        dfQS1_1
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .partitionBy("anno", "mese", "sessione")
          .save(pathOrarieSEM)

        hiveCtx2.dropTempTable(tmpTerneRett)
        hiveCtx2.dropTempTable(tempTbl)
        hiveCtx2.dropTempTable(tempTblTerna)
        if (tmp_tbls.length > 0) {
          for (el <- tmp_tbls) {
            hiveCtx2.dropTempTable(el)
          }
        }

        dfQS1.unpersist()

        hiveCtx2.sql("MSCK REPAIR TABLE " + TblRettOrarie)
        hiveCtx2.refreshTable(TblRettOrarie)
        log.info("***** insert misure ore OK")


        if(threadx.isAlive)
          threadx.join()

        log.info("***** insert prt_tmo_mo_terne_sem su hdfs OK")

        log.info("***** aggiornamento partizioni OK")


      }


			log.info("CALCOLO SEM PER ANNO :" + _annoAggr + " E MESE " + _meseAggr + " COMPLETATA")

			return





		}
		catch {
			case e: Exception =>
				e.printStackTrace()

		}
	}


	/*
  DA COMPLETARE
 */
	def GoExportPodCommitted(uidElab:String,anno:Int,mese:Int,sessione:String,dbdest:String,pathRootCSV:String,versione_orarie:String ): Unit ={

		 val aggrdett_pod = AggregazioneMisureOrarieDettaglio
		 val pathCsvFileDir =aggrdett_pod.setDirectories(pathRootCSV,anno.toString,mese.toString,uidElab.toString)

		log.info("*** Avvio organizzazione dati per esportazione in csv in :" + pathCsvFileDir + aggrdett_pod.pathCSV_Dest)

		val whereVersioneCsv=if(versione_orarie=="0") "" else s" and uid_elab = ${versione_orarie}"

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
         n_id_file , CAST(perdita as STRING) perdita , tipo_pratica , sessione ,
         CONCAT(anno,LPAD(mese,2,0))annomese , nvl(sem_corrente,'')sem_corrente ,
         CONCAT('dettaglio','_${uidElab}') k_nome
         from  $dbdest.prt_tmo_aggr_sem_pod_cons
         where  anno = ${anno} and  mese = ${mese} and  sessione = '${sessione}' ${whereVersioneCsv}  order by codice_pod,n_id_file
         """

		hiveCtx.sql(queryAll)

		val query =s"""
         INSERT INTO prt_tmo_aggr_periodica_sem_pod_cons_csv PARTITION(k_nome)
         SELECT 'CODICE_POD' codice_pod  ,'ENERGIA' consumo  ,'TENSIONE' tensione ,
         'NOME_FILE' n_id_file , 'COEFF_PERDITA' perdita ,'TIPO_PRATICA' tipo_pratica  , 'SESSIONE' sessione,
         'ANNOMESE' annomese ,'SEM_CORRENTE' sem_corrente ,
         CONCAT(az.t_piva,'_',az1.t_piva,'_',udd.t_codice_terna,'_', t_area_rif,'_',anno,LPAD(mese,2,0),'_${uidElab}') k_nome
         from  $dbdest.prt_tmo_aggr_sem_pod_cons pods
          inner join rcu.rcu_azienda_p az on pods.n_id_distr=az.n_id_azienda
          inner join rcu.rcu_udd_p udd on pods.n_id_udd = udd.n_id_udd
          inner join rcu.rcu_azienda_p az1 on  pods.n_id_udd = az1.n_id_azienda
         where  anno = ${anno} and  mese = ${mese} and  sessione = '${sessione}' ${whereVersioneCsv}
         group by CONCAT(az.t_piva,'_',az1.t_piva,'_',udd.t_codice_terna,'_', t_area_rif,'_',anno,LPAD(mese,2,0),'_${uidElab}')
         UNION ALL
         SELECT codice_pod  , regexp_replace(CAST(format_number(consumo,3) AS STRING),',','') consumo  ,CAST(CAST(tensione AS INT) as STRING)tensione ,
         n_id_file , CAST(perdita as STRING) perdita , tipo_pratica ,  sessione ,
         CONCAT(anno,LPAD(mese,2,0))annomese ,nvl(sem_corrente,'')sem_corrente ,
         CONCAT(az.t_piva,'_',az1.t_piva,'_',udd.t_codice_terna,'_',t_area_rif,'_',anno,LPAD(mese,2,0),'_${uidElab}') k_nome
         from  ${dbdest}.prt_tmo_aggr_sem_pod_cons pods
          inner join rcu.rcu_azienda_p az on pods.n_id_distr=az.n_id_azienda
          inner join rcu.rcu_udd_p udd on pods.n_id_udd = udd.n_id_udd
          inner join rcu.rcu_azienda_p az1 on  pods.n_id_udd = az1.n_id_azienda
         where  anno = ${anno} and  mese = ${mese} and  sessione = '${sessione}' ${whereVersioneCsv}  order by codice_pod,n_id_file
         """

//az.t_piva  as pivadistributore, az1.t_piva as pivautente, udd.t_codice_terna  as codcontrdisp

		hiveCtx.sql(query)

		log.info("*** Organizzazione dati per esportazione in csv effettuato")

		val annomese_str= anno.toString + (("0" + mese.toString) takeRight 2)
		val suffix_sem=if( Sessione_SEM_CMD.trim =="S1" )s"${Tipo_SEM}_PRE" else s"${Tipo_SEM}_SEM"

		aggrdett_pod.writeCsvAndZip(hiveCtx,locationcsv,pathCsvFileDir,annomese_str,true,suffix_sem)

	}

 def GoCommitSem(uidElab:Long,anno:Int,mese:Int,sessione:String,dbdest:String ,pathOrarieSEM:String ,pathAmSEM:String,pathAggrPodCons:String,pathTerneSEM:String): Unit =
	{
		try {
			val queryOrarie =
				s"""
                     SELECT * FROM ${dbdest}.aggregazioni_misure_orarie_sem
										where anno = ${anno} and mese =${mese} and sessione = '${sessione}'
					          """.stripMargin

			val queryAM =
				s"""
                     SELECT * FROM ${dbdest}.aggregazioni_misure_am_sem
										 where annoaggr = ${anno} and meseaggr =${mese} and sessione = '${sessione}'
					 					 and (KT_new ='1' OR KT_sem='1' OR KT_periodica ='1')
					          """.stripMargin

      val queryTerne =
        s"""
                     SELECT * FROM ${dbdest}.prt_tmo_mo_terne_sem
										 where anno = ${anno} and mese =${mese} and sessione = '${sessione}'
					          """.stripMargin

			val dtOrarie = hiveCtx.sql(queryOrarie).withColumn("uid_elab", lit(uidElab))

			val conImpala = DriverManager.getConnection(ConnectionURL)
			val ghigliottina=getDataGhiottina(conImpala,anno,mese,Array())
			conImpala.close()

			//case when da_rett = 0 then 'P' else 'R' END tipo_pratica,
			val queryPods =
				s"""
          SELECT n_id_distr,n_id_udd,area t_area_rif ,annomese, codice_pod,n_id_distr_rif,
          nomefile n_id_file,d_ricezione,tipo_pratica,ROUND(SUM(consumo),3) consumo,
          tensione,flg_da_aggregare,perdita,sem_corrente, sessione,uid_elab
          FROM (
           SELECT n_id_distr, n_id_udd, NVL(area,'NO_AREA')area, concat(ANNO,LPAD(mese,2,0)) annomese, SUBSTR(POD,1,14) codice_pod, N_ID_DISTR_rif, case
           when nomefile like '%2G%' AND nomefile LIKE '%PDO%' then 'PDO2G'
           when nomefile like '%2G%' AND nomefile LIKE '%RFO%' then 'RFO2G'
           ELSE nomefile END AS nomefile,
           max(time_stamp) over ( partition by anno,mese,pivadistributore,pod) AS d_ricezione,
					 case when (nomefile like '%PDO%' and annomesegiornodir > ${ghigliottina}) or (nomefile like '%RFO%') then 'R' else 'P' end tipo_pratica,
           (H1+H2+H3+H4+H5+H6+H7+H8+H9+H10+H11+H12+H13+H14+H15+H16+H17+H18+H19+H20+H21+H22+H23+H24+H25)/(1+nvl(PERDITA,0)) consumo,
            VERSIONE AS uid_elab,sessione , tensione, FLAG_VALIDAZIONE AS flg_da_aggregare, perdita,
					 case when da_rett = 1 then 'S' else 'N' end sem_corrente
					 FROM
           ${dbdest}.aggregazioni_misure_orarie_sem where anno = ${anno} and mese =${mese} and sessione = '${sessione}') X GROUP BY
            n_id_distr,n_id_udd,area,annomese, codice_pod,n_id_distr_rif,nomefile,d_ricezione,tipo_pratica, uid_elab, sessione, tensione, flg_da_aggregare, perdita,sem_corrente
   			""".stripMargin

			val dtPodOrarie=hiveCtx.sql(queryPods)

      if(verbose) {
				log.info("*** Stampa query estrazione dettaglio pods")
				log.info(queryPods)
			}

			log.info("Avvio scrittura aggregazioni_misure_orarie_sem su " + pathOrarieSEM)
			dtOrarie
				.write
				.format("parquet")
				.mode(SaveMode.Append)
				.partitionBy("anno", "mese", "sessione", "uid_elab")
				.save(pathOrarieSEM)

			hiveCtx.sql(s"MSCK REPAIR TABLE ${dbdest}.aggregazioni_misure_orarie_sem_committed")


			val dtAM = hiveCtx.sql(queryAM).withColumn("uid_elab", lit(uidElab))

			log.info("Avvio scrittura aggregazioni_misure_am_sem su " + pathAmSEM)

			dtAM
				.write
				.format("parquet")
				.mode(SaveMode.Append)
				.partitionBy("annoaggr", "meseaggr", "sessione", "uid_elab")
				.save(pathAmSEM)

			hiveCtx.sql(s"MSCK REPAIR TABLE ${dbdest}.aggregazioni_misure_am_sem_committed")

			writeTernaOnOracle(dtAM)

      log.info("Avvio scrittura prt_tmo_aggr_sem_pod_cons su " + pathAggrPodCons)

       dtPodOrarie.withColumn("anno",lit(anno)).withColumn("mese",lit(mese))
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("anno", "mese", "sessione", "uid_elab")
        .save(pathAggrPodCons)

      hiveCtx.sql(s"MSCK REPAIR TABLE ${dbdest}.prt_tmo_aggr_sem_pod_cons")

			writePodsOnOracle(dtPodOrarie)

      val dtTerne = hiveCtx.sql(queryTerne).withColumn("uid_elab", lit(uidElab))

      log.info("Avvio scrittura prt_tmo_mo_terne_sem su " + pathTerneSEM)

      dtTerne
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("anno", "mese", "sessione", "uid_elab")
        .save(pathTerneSEM)

      hiveCtx.sql(s"MSCK REPAIR TABLE ${dbdest}.prt_tmo_mo_terne_sem_committed")
		}
		catch {
			case e: Exception => e.printStackTrace()
		}


	}


	def Commit_ExportSem(pathOrarieSEM:String ,pathAmSEM:String,pathAggrPodCons:String,pathTerneSEM:String,dbdest:String,annomesi:Array[String],uidElabSelExport:String): Unit ={

		var cicla =true
		var annoSem=anno_iniziale
		var meseSem=mese_iniziale

		val mesefinSem=mese_finale
		val annofinSem=anno_finale
		val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
		val uidElab = sdf.format(new java.util.Date()).toLong


		val pathCSV:String = if(RunExportCommit) {
			val x =prop.getProperty("spark.app.dettpod.aggregato.sem.path")
			log.info("*** Root file system path scrittura csv : " + x)
			x
		} else ""

		while(cicla) {


			val rtval=checkannomesipar(annoSem, meseSem,annomesi)
			if(rtval!=0) {

				if(Commit_SEM) {
					log.info("COMMIT SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ")
					GoCommitSem(uidElab, annoSem, meseSem, Sessione_SEM, dbdest, pathOrarieSEM, pathAmSEM, pathAggrPodCons, pathTerneSEM)
					log.info("COMMIT SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ESEGUITA CON SUCCESSO ***")
				}
				if(RunExportCommit){ //EXPORT IN CSV POD COMMITTED
					log.info("EXPORT PODS COMMITTED SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ")
          GoExportPodCommitted(uidElab.toString,annoSem,meseSem,Sessione_SEM,dbdest,pathCSV,uidElabSelExport)
					log.info("EXPORT PODS COMMITTED SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ESEGUITA CON SUCCESSO ***")
				}
			}

			meseSem = if (meseSem - 1 == 0) 12 else (meseSem - 1)
			annoSem = if (meseSem == 12) (annoSem - 1) else annoSem

			if (meseSem == mesefinSem && annoSem == annofinSem)
				cicla = false
		}

		//ultimo mese
		val rtval=checkannomesipar(annoSem, meseSem,annomesi)
		if(rtval!=0) {

			if(Commit_SEM) {
				log.info("COMMIT SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ")
				GoCommitSem(uidElab, annoSem, meseSem, Sessione_SEM, dbdest, pathOrarieSEM, pathAmSEM, pathAggrPodCons, pathTerneSEM)
				log.info("COMMIT SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ESEGUITA CON SUCCESSO ")
			}
			if(RunExportCommit){//EXPORT IN CSV POD COMMITTED
				log.info("EXPORT PODS COMMITTED SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ")
				GoExportPodCommitted(uidElab.toString,annoSem,meseSem,Sessione_SEM,dbdest,pathCSV,uidElabSelExport)
				log.info("EXPORT PODS COMMITTED SEM SESSIONE : " + Sessione_SEM + " ANNO " + annoSem + " MESE " + meseSem + " ESEGUITA CON SUCCESSO ***")
			}

		}


	}

	private def CalcSem(pathOrarieSEM:String ,pathAmSEM:String,pathTerneSEM:String,annoStartCheskSemParz:Int,meseStartCheskSemParz:Int,annomesi:Array[String],distr_oracle:Broadcast[Map[String,String]]): Unit ={

		var cicla =true
		var annoSem=anno_iniziale
		var meseSem=mese_iniziale

		val mesefinSem=mese_finale
		val annofinSem=anno_finale

		val meseInizialeClouderaOracle=7
		val annoInizialeClouderaOracle=2018

		var isPeriodoOracle=false

		var distr_ghigliottina:String =""
		var canGoSem:Boolean=false

		while(cicla) {

			canGoSem = false
			distr_ghigliottina=""

			if(isPeriodoOracle)
				{
					val rtval=checkannomesipar(annoSem,meseSem,annomesi)
					if(rtval!=0)
					{
						if(calcSemOraclePraticheR)
							log.info("ANNO " + annoSem + " MESE " + meseSem + " CALCOLO CON DATI PROVENIENTE DA ORACLE SU PRATICHE R")
						else
							log.info("ANNO " + annoSem + " MESE " + meseSem + " CALCOLO CON DATI PROVENIENTE DA ORACLE")

					canGoSem = true
					}
				}
			else if (!isSemForceTot) {
       //eseguo la sem secondo procedimento standard con calcolo parziale fino ad agosto 2018
				val conImpala = DriverManager.getConnection(ConnectionURL)
				val ghigliottina = getDataGhiottina(conImpala, annoSem, meseSem,annomesi)

				log.info("ANNO " + annoSem + " MESE " + meseSem + " GHIGLIOTTINA " + ghigliottina.toString)

				if (ghigliottina > 0) {
					distr_ghigliottina = ghigliottina.toString
					canGoSem = true
				}
			}
			else { //sem con aggregazione globale per distributore
				distr_ghigliottina = getDistributoriInRettificaCloudera(annoSem, meseSem,annomesi)

				log.info("ANNO " + annoSem + " MESE " + meseSem + " DISTRIBUTORI " + distr_ghigliottina)

				if (distr_ghigliottina != "") {
					canGoSem = true
				}

			}

			if (canGoSem) {
				//lancio aggregazione oraria + aggregazione x terna

				val checkSemParz=if(Tipo_SEM == "SEM1")
					                  false
				                 else {
					                  if (!isSemForceTot && (annoStartCheskSemParz > annoSem || (annoStartCheskSemParz == annoSem && meseStartCheskSemParz >= meseSem)))
															 true
					                  else
						                   false
				                      }


				if(FindInvalidazioni){
					if(isPeriodoOracle)
						goInvalidazione(annoSem,meseSem,flussoquarti_tableoracle,mot3_table)
					else
						goInvalidazione(annoSem,meseSem,flussoquarti_table,mot3_table)
				}

				init(annoSem, meseSem, isSemForceTot,isPeriodoOracle, distr_ghigliottina, _dbDest)
				runAggregazione(isPeriodoOracle, pathOrarieSEM, pathAmSEM,pathTerneSEM,checkSemParz,distr_oracle)

			}
			meseSem = if (meseSem - 1 == 0) 12 else (meseSem - 1)
			annoSem = if (meseSem == 12) (annoSem - 1) else annoSem

			if (annoSem < annoInizialeClouderaOracle  || (meseSem <= meseInizialeClouderaOracle && annoSem == annoInizialeClouderaOracle))
				isPeriodoOracle=true

			if (meseSem == mesefinSem && annoSem == annofinSem)
				cicla = false
		}


		canGoSem = false
		distr_ghigliottina=""

		 if(isPeriodoOracle)
		 {
			 val rtval=checkannomesipar(annoSem,meseSem,annomesi)
			 if(rtval!=0)
			 {
				 if(calcSemOraclePraticheR)
					 log.info("ANNO " + annoSem + " MESE " + meseSem + " CALCOLO CON DATI PROVENIENTE DA ORACLE SU PRATICHE R")
				 else
					 log.info("ANNO " + annoSem + " MESE " + meseSem + " CALCOLO CON DATI PROVENIENTE DA ORACLE")
				 canGoSem = true
			 }

		 }else if (!isSemForceTot) {//sem con aggregazione parziale
			val conImpala = DriverManager.getConnection(ConnectionURL)
				val ghigliottina=getDataGhiottina(conImpala,annoSem, meseSem,annomesi)

				log.info("ANNO " + annoSem + " MESE " + meseSem + " GHIGLIOTTINA " + ghigliottina.toString)

				if (ghigliottina > 0) {
					distr_ghigliottina = ghigliottina.toString
					canGoSem = true
				}
			}
			else {
				distr_ghigliottina = getDistributoriInRettificaCloudera(annoSem, meseSem, annomesi)

				log.info("ANNO " + annoSem + " MESE " + meseSem + " DISTRIBUTORI " + distr_ghigliottina)
				//lancio aggregazione oraria + aggregazione x terna
				if (distr_ghigliottina != "") {
					canGoSem = true
				}

			}

			if (canGoSem) {


				val checkSemParz=if(Tipo_SEM == "SEM1")
					                 false
				                  else {
					                  if (!isSemForceTot && (annoStartCheskSemParz > annoSem || (annoStartCheskSemParz == annoSem && meseStartCheskSemParz >= meseSem)))
															 true
														else
															 false
				                   }

				if(FindInvalidazioni){
					if(isPeriodoOracle)
						goInvalidazione(annoSem,meseSem,flussoquarti_tableoracle,mot3_table)
					else
						goInvalidazione(annoSem,meseSem,flussoquarti_table,mot3_table)
				}

				init(annoSem, meseSem, isSemForceTot,isPeriodoOracle, distr_ghigliottina, _dbDest)
				runAggregazione(isPeriodoOracle, pathOrarieSEM, pathAmSEM,pathTerneSEM,checkSemParz,distr_oracle)

			}



	}


	private def checkannomesipar(anno:Int,mese:Int,annomesi:Array[String]): Int ={

		val annomese_tmp =anno.toString +(("0" + mese) takeRight 2)
		if(annomesi.length > 0){
			log.info("*** verifica anno mese " + annomese_tmp + " tra i mesi da calcolare da riga comando ***")
			if(!annomesi.contains(annomese_tmp))
			{
				log.info("*** anno e mese " + annomese_tmp + " non trovato e quindi skippato **** ")
				return 0
			}
		}
		return 1
	}

	private def getDataGhiottina(impalaConnection:Connection,anno:Int,mese:Int,annomesi:Array[String]): Int = {

		val rtval=checkannomesipar(anno,mese,annomesi)
		if(rtval==0)return 0

		//istruzione temporanea
		//if (!(anno == 2018 && mese == 8)) return 0

		val conImpala = impalaConnection

		val getGhigliottine: String =
			s"""
																		select anno ,mese,d_ghigliottina from (
                                    select cast(substr(cast(annomese_aggregato as char(6)),1,4) as int) anno ,
			                              cast(substr(cast(annomese_aggregato as char(6)),5,2) as int) mese ,
                                    cast(d_ghigliottina as int)d_ghigliottina from data_ghigliottina)
																		as tbl where (anno = ${anno} and mese = ${mese} )
																		order by anno desc,mese desc
																	 """
		val stm = conImpala.createStatement()
		val rs = stm.executeQuery(getGhigliottine)
		var distributori = ""

		var ghigliottina:Int=0
		if (rs.next) {
			val annoq = rs.getInt("anno")
			val meseq = rs.getInt("mese")
			ghigliottina = rs.getInt("d_ghigliottina")


		} else {
			log.info("Attenzione non è stato possibile ricavare la data di ghigliottina per l'anno : " + anno.toString + " e mese " + mese.toString)
			ghigliottina = -1
		}

		rs.close()
		stm.close()


		return ghigliottina
	}

	private def getDistributoriInRettificaCloudera(anno:Int,mese:Int,annomesi:Array[String]): String ={


		val conImpala = DriverManager.getConnection(ConnectionURL)

		val ghigliottina=getDataGhiottina(conImpala,anno,mese,annomesi)


		var distributori =""

		//caso temporaneo per test
		if(ghigliottina==0) {
			return ""
		}

		if(ghigliottina != -1){

			val stmt2= conImpala.createStatement()
			//OTTENGO TUTTI I DISTRIBUTORI PER ANNO E MESE IN ESAME CON POD TARDIVI E/O RFO E CHE QUINDI DOVRANNO
			//ESSERE RICALCOLATI AI FINI DELLA SEM

			log.info("select distinct pivadistributorequarti from " + flussoquarti_table +" T where validato = 'S' and annoquarti = "+anno.toString+" and mesequarti = "+ mese.toString +" and ((T.annomesegiornodir > "+ghigliottina.toString+") or (nomefile like '%RFO%')) and pivadistributorequarti is not null")

			val rs2 = stmt2.executeQuery("select distinct pivadistributorequarti from " + flussoquarti_table +" T where validato = 'S' and annoquarti = "+anno.toString+" and mesequarti = "+ mese.toString +" and ((T.annomesegiornodir > "+ghigliottina.toString+") or (nomefile like '%RFO%')) and pivadistributorequarti is not null")

			while (rs2.next) {
				distributori = distributori + "'"+rs2.getString("pivadistributorequarti")+"',"
			}

			if(distributori!="")
				distributori=distributori.substring(0,distributori.length-1)

			stmt2.close()
			rs2.close()

		}

		conImpala.close

		distributori



	}



	//OTTENGO TUTTE LE DATE DI GHIGLIOTTINA PRESENTI SU CLOUDERA(FINO AD AGOSTO 2018)
	def getAllGhigliottine(anno_in:String , mese_in:String,anno_fin:String,mese_fin:String,stm: Statement): HashMap[String, Int] = {

		val getGhigliottine :String =s"""
																		select anno ,mese,d_ghigliottina from (
                                    select cast(substr(cast(annomese_aggregato as char(6)),1,4) as int) anno ,
			                              cast(substr(cast(annomese_aggregato as char(6)),5,2) as int) mese ,
                                    cast(d_ghigliottina as int)d_ghigliottina from data_ghigliottina)
																		as tbl where (anno <= ${anno_in} and mese <= ${mese_in} ) and
																		( anno >= ${anno_fin}  and mese >= ${mese_fin} )
																		order by anno desc,mese desc
																	 """




		val rs = stm.executeQuery(getGhigliottine)

		val hashMapDD: HashMap[String, Int] =HashMap()

		while (rs.next) {
			val annoq = rs.getInt("anno")
			val meseq = rs.getInt("mese")
			val ghigliottina = rs.getInt("d_ghigliottina")


			hashMapDD+= ((annoq.toString()+"-"+meseq.toString) -> ghigliottina)

		}

		rs.close()


		hashMapDD

	}

	private def runInvalidazione(annomesi:Array[String]): Unit ={

		var cicla =true
		var annoSem=anno_iniziale
		var meseSem=mese_iniziale

		val mesefinSem=mese_finale
		val annofinSem=anno_finale

		val meseInizialeClouderaOracle=7
		val annoInizialeClouderaOracle=2018

		var isPeriodoOracle=false

		var canGoToInvalidate:Boolean=false

		while(cicla) {

			canGoToInvalidate = false

				val rtval=checkannomesipar(annoSem,meseSem,annomesi)
				if(rtval!=0) {
					log.info("RICERCA INVALIDAZIONI NELL'ANNO " + annoSem + " E MESE " + meseSem + " ")
					canGoToInvalidate = true
				}


			if (canGoToInvalidate) {

				if(isPeriodoOracle)
					goInvalidazione(annoSem,meseSem,flussoquarti_tableoracle,mot3_table)
				else
					goInvalidazione(annoSem,meseSem,flussoquarti_table,mot3_table)

			}
			meseSem = if (meseSem - 1 == 0) 12 else (meseSem - 1)
			annoSem = if (meseSem == 12) (annoSem - 1) else annoSem

			if (annoSem < annoInizialeClouderaOracle  || (meseSem <= meseInizialeClouderaOracle && annoSem == annoInizialeClouderaOracle))
				isPeriodoOracle=true

			if (meseSem == mesefinSem && annoSem == annofinSem)
				cicla = false
		}


		canGoToInvalidate = false

		val rtval=checkannomesipar(annoSem,meseSem,annomesi)
		if(rtval!=0) {
			log.info("RICERCA INVALIDAZIONI NELL'ANNO " + annoSem + " E MESE " + meseSem + " ")
			canGoToInvalidate = true
		}

		if (canGoToInvalidate) {


			if(isPeriodoOracle)
				goInvalidazione(annoSem,meseSem,flussoquarti_tableoracle,mot3_table)
			else
				goInvalidazione(annoSem,meseSem,flussoquarti_table,mot3_table)

		}



	}

	def goInvalidazione(anno :Int, mese:Int,tbl_flusso_quarti :String,tbl_mot3:String): Unit = {

		log.info("Avvio cancellazione anno : " + anno + " , mese : " + mese )

		val q_drop_part:String=s"ALTER TABLE ${_dbDest}.prt_tmo_mo_sem_ret_annullate DROP IF EXISTS PARTITION(anno=${anno},mese=${mese})"
		hiveCtx.sql(q_drop_part)
		hiveCtx.sql(s"MSCK REPAIR TABLE ${_dbDest}.prt_tmo_mo_sem_ret_annullate")
		val a_m =anno.toString +(("0" + mese) takeRight 2)


		val tblmot3:String=if(tbl_mot3.contains("[ANNOMESE]"))_dbDest+".import_ora_clou_"+a_m+"_mot3"
		else _dbDest+"."+tbl_mot3

		//i quarti vengono letti sempre dal db di produzione au

		val queryInvalidazioni: String =
			s"""
			select distinct nomefile,podquarti,time_stamp as timestamp,pivadistributorequarti,pivautentequarti,codcontrdispquarti , motivazione
			from
			(
				select quarti.nomefile,quarti.pivadistributorequarti,quarti.pivautentequarti,
				quarti.codcontrdispquarti,quarti.podquarti,
				quarti.time_stamp,mot3.motivazione,
				max(quarti.time_stamp)
				over(partition by quarti.pivadistributorequarti,quarti.codcontrdispquarti,quarti.podquarti) time_stamp_max
				from
				(select pivadistributorequarti,codcontrdispquarti,SUBSTR(podquarti,1,14)podquarti,time_stamp ,motivazione  from
				${tblmot3}  where annoquarti=${anno} and mesequarti=${mese}) mot3
				inner join
				(select pivadistributorequarti,codcontrdispquarti,SUBSTR(podquarti,1,14)podquarti,time_stamp ,pivautentequarti,nomefile  from
				au.${tbl_flusso_quarti}  where annoquarti=${anno} and mesequarti=${mese}) quarti
				on concat(quarti.pivadistributorequarti,quarti.codcontrdispquarti,quarti.podquarti) =
				concat(mot3.pivadistributorequarti,mot3.codcontrdispquarti,mot3.podquarti)
				where quarti.time_stamp < mot3.time_stamp
				) as tbl where time_stamp = time_stamp_max
  		""".stripMargin


		log.info("Estrazione misure da invalidare per l'anno : " + anno + " , mese : " + mese)

		if(verbose)
			log.info("query di ricerca :" + queryInvalidazioni)


		try
			{
		val dt_Invalidazioni= hiveCtx.sql(queryInvalidazioni).withColumn("K_DT",
			concat(col("podquarti"),col("timestamp"),col("pivadistributorequarti"),
				col("pivautentequarti"),col("codcontrdispquarti"))).withColumn("anno",lit(anno)).withColumn("mese",lit(mese))


		log.info("Scrittura misure da invalidare in " + tbl_misure_escluse )
		dt_Invalidazioni
			.write
			.format("parquet")
			.mode(SaveMode.Append)
			.partitionBy("anno", "mese")
			.save(tbl_misure_escluse)

		log.info("Aggiornamento partizioni su tabella " + _dbDest +".prt_tmo_mo_sem_ret_annullate" )

		hiveCtx.sql(s"MSCK REPAIR TABLE ${_dbDest}.prt_tmo_mo_sem_ret_annullate")
		hiveCtx.refreshTable(s"${_dbDest}.prt_tmo_mo_sem_ret_annullate")

		log.info("Estrazione misure da invalidare per l'anno : " + anno + " , mese : " + mese +" completata")
			}
		catch{
			case ex:Exception => log.info("Errore in fase di ricerca misure da invalidare causa : " + ex.getMessage)
		}
	}

	def getInvalidazioni(anno :Int, mese:Int):String ={

		val query =s"SELECT K_DT FROM ${_dbDest}.prt_tmo_mo_sem_ret_annullate where anno =${anno} and mese =${mese}"
		val array_Annullati=hiveCtx.sql(query).collect()

		var esclusi:String=""
		for (el <- array_Annullati)  esclusi =esclusi + "'"+el.getAs[String](0)+"',"

		if(esclusi!="")
			esclusi=esclusi.substring(0,esclusi.length-1)

		esclusi
	}



	/**
		* Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
		* @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
		*/
	def main(args: Array[String]) {

		val commandLineOptions = new CommandLineOptions()
		val commonsCliUtils = new CommonsCliUtils()
		val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)

		val genCSVParam= if (commandLine.hasOption(commandLineOptions.writeCsvAggrOrario.getOpt))
		{if(commandLine.getOptionValue(commandLineOptions.writeCsvAggrOrario.getOpt)==null)"" else commandLine.getOptionValue(commandLineOptions.writeCsvAggrOrario.getOpt)}
		else ""

		RunExportCommit = if (commandLine.hasOption(commandLineOptions.writeCsvAggrOrario.getOpt)) !genCSVParam.equals("N") else false

		val argsObjMaster = new CommonsCliUtils().getArgsSEM(commandLine)


		val nameApp =if(RunExportCommit) {
			val appn=if(Commit_SEM) argsObjMaster.appName + " + Esportazione dettaglio pod SEM " else " Esportazione dettaglio pod SEM committed"

			log.info("***** Inizio processo " + appn+ " *****")
			appn
		} else argsObjMaster.appName


		log.info("***** current user " + System.getProperty("user.name") + "****")
		log.info(propertiesC.printEnvVar)



		 anno_iniziale = argsObjMaster.anno.toInt
		 mese_iniziale = argsObjMaster.mese.toInt
		 Tipo_SEM = argsObjMaster.PdoRfo
		 FindInvalidazioni=argsObjMaster.SemInvalidazioni
		 Commit_SEM=argsObjMaster.SemCommit



		val cal = Calendar.getInstance()
		val Year =if(commandLine.hasOption(commandLineOptions.sem_force_anno_start.getOpt)) commandLine.getOptionValue(commandLineOptions.sem_force_anno_start.getOpt).toInt else cal.get(Calendar.YEAR )

		if(FindInvalidazioni && Commit_SEM)
		{
			log.info("Attenzione la funzione di commit non può essere accoppiata alla funzione di invalidazione")
			return
		}
		if(FindInvalidazioni && RunExportCommit)
		{
			log.info("Attenzione la funzione di export pod non può essere accoppiata alla funzione di invalidazione")
			return
		}

		if(!FindInvalidazioni) {
			if (argsObjMaster.semsession == "") {
				log.info("Attenzione bisogna indicare la sessione della SEM  -SMS [S1 oppure S2]")
				return
			}
		}



		Sessione_SEM=if(argsObjMaster.semsession!="")Tipo_SEM + argsObjMaster.semsession+"_"+Year.toString else ""
		Sessione_SEM_CMD=argsObjMaster.semsession


		val makeImportFromOracle:TypeDataImportRCU.Value=if(Commit_SEM || RunExportCommit) TypeDataImportRCU.NONE else  argsObjMaster.importFromOracleSem
		isSemForceTot  =argsObjMaster.semCalcTot



		val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())

		 anno_finale  = if(Tipo_SEM=="SEM1") anno_iniziale else if (Tipo_SEM=="SEM2" && ((anno_iniziale - 5)< 2017)) 2017 else anno_iniziale - 5
		 mese_finale  =1

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
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")



			.setMaster(argsObjMaster.master)


		sc = new SparkContext(conf)

		sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

		sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
		sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


		sc.setLogLevel(argsObjMaster.logLevel)
		//sc.setLogLevel("INFO")

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


		try {


			val orarie_sem = if (Commit_SEM || RunExportCommit) sc.getConf.get("spark.aggregazioni.misure.orarie.sem.commit") else sc.getConf.get("spark.aggregazioni.misure.orarie.sem")
			val am_sem = if (Commit_SEM || RunExportCommit) sc.getConf.get("spark.aggregazioni.misure.am.sem.commit") else sc.getConf.get("spark.aggregazioni.misure.am.sem")
      val terne_sem = if (Commit_SEM || RunExportCommit) sc.getConf.get("spark.aggregazioni.misure.terne.sem.commit") else sc.getConf.get("spark.aggregazioni.misure.terne.sem")
			tbl_misure_escluse=sc.getConf.get("spark.aggregazioni.misure.escluse.sem")

      tbl_tmp_terne = sc.getConf.get("spark.aggregazioni.misure.ternetmp.sem")
      tbl_tmp_sem_periodica = sc.getConf.get("spark.aggregazioni.misure.semperiodicatmp.sem")

			val slash = sc.getConf.get("spark.flusso.misure.slash")
      Ghigliottina_Max=argsObjMaster.annomesegiornodir.toString



      if(Ghigliottina_Max!="0")
        log.info("*** Data di ghiottina massima : " + Ghigliottina_Max)

			log.info("*** Ricerca misure da invalidare: " + FindInvalidazioni +" ***")

			log.info("*** sc.master: " + sc.master)

			//log.info("*** minPartitions: " + minPartitions)
			log.info("*** slash: " + slash)
      log.info("*** verbose: " + verbose.toString)
			//hdfs

			log.info("*** aggregazioni orarie sem: " + orarie_sem)
			log.info("*** aggregazioni am sem : " + am_sem)
      log.info("*** aggregazioni terne sem : " + terne_sem)

			log.info("*** misure da escludere : " + tbl_misure_escluse)


			log.info("*** Tipo SEM: " + Tipo_SEM)
			if(Sessione_SEM!="")
			log.info("*** Sessione SEM :" + Sessione_SEM)

			log.info("*** Commit SEM :" + Commit_SEM)
			log.info("*** Export Pods SEM Committed :" + RunExportCommit)

			log.info("*** Anno iniziale: " + anno_iniziale)
			log.info("*** Mese iniziale: " + mese_iniziale)

			log.info("*** Anno finale: " + anno_finale)
			log.info("*** Mese finale: " + mese_finale)

			if (!Commit_SEM && !RunExportCommit)
				log.info("*** SEM forzata a calcolo totale :" + isSemForceTot + " ***")


			log.info("*** dataelaborazione: " + dataelaborazione)


			log.info("*** verbose: " + verbose)
			log.info("*** testlevel: " + testlevel)


			log.info("*** database di destinazione : " + _dbDest)
			log.info("*** path base di scrittura : " + _basePath)



			val annoStartCheskSemParz: Int = if (mese_iniziale == 12) anno_iniziale else (anno_iniziale + 1)
			val meseStartCheskSemParz: Int = if (mese_iniziale == 12) (mese_iniziale - 6) else 0
			if (meseStartCheskSemParz != 0) {
				if (!Commit_SEM && !RunExportCommit) {
					log.info("*** annoStartCheskSemParz : " + annoStartCheskSemParz + " ***")
					log.info("*** meseStartCheskSemParz : " + meseStartCheskSemParz + " ***")
				}
			}

			val annomesi: Boolean = commandLine.hasOption(commandLineOptions.annomese_sem.getOpt)

			val annomesiList: Array[String] = if (annomesi) {
				val annomesi: String = commandLine.getOptionValue(commandLineOptions.annomese_sem.getOpt)
				log.info("Applicazione della sem ai seguenti mesi(se possibile) : " + annomesi)
				val list_annomesi = annomesi.split(",").filter(x => x.length == 6)
				list_annomesi
			} else Array()


			if (makeImportFromOracle != TypeDataImportRCU.NONE) {
				val rcuimport = new RCU_DataImport()
				rcuimport.importDataFromOracle(makeImportFromOracle, hiveCtx, prop, sc,annomesiList)

			}


			if (Commit_SEM || RunExportCommit) {
				if (Sessione_SEM == "") {
					log.info("Attenzione bisogna indicare la sessione della SEM  -SMS [S1 oppure S2]")
					return
				}
        val aggr_cons_pods = sc.getConf.get("spark.aggregazioni.pod_cons.am.sem.commit")

				val uidElabSelExport = if(RunExportCommit && !genCSVParam.equals("Y"))
				{log.info(s"***** Generazione csv/zip utilizzando la versione oraria di elaborazione della Commit : ${genCSVParam}")
					genCSVParam.toLong.toString}
				else "0"

        log.info("*** aggregazioni pod committed : " + aggr_cons_pods)
        Commit_ExportSem(orarie_sem, am_sem,aggr_cons_pods,terne_sem, _dbDest, annomesiList,uidElabSelExport)
      }
			else {
				if(FindInvalidazioni && Sessione_SEM=="") {
					runInvalidazione(annomesiList)
				}
				else {
					if (Sessione_SEM == "") {
						log.info("Attenzione bisogna indicare la sessione della SEM  -SMS [S1 oppure S2]")
						return
					}
					val distr_amp_dir = sc.broadcast(this.getAllDistrAzienda())
					CalcSem(orarie_sem, am_sem, terne_sem, annoStartCheskSemParz, meseStartCheskSemParz, annomesiList, distr_amp_dir)
				}
			}

		} catch {
			case ex: FileNotFoundException => ex.printStackTrace()
			case e: Exception => e.printStackTrace()
		} finally {
			sc.stop()
		}


		log.info(s"***** Fine processo ${sc.appName} *****")
	}







}
