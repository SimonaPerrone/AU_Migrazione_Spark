package it.au.misure.ingestione

import java.io.{BufferedWriter, File, FileNotFoundException, FileWriter}
import java.lang.management.ManagementFactory
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}
import javax.xml.XMLConstants
import javax.xml.transform.Source
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import it.au.misure.util.{CreateProperties, LoggingSupport}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.reflect.ClassTag
import scala.sys.process._
import scala.util.Try
import scala.xml._


/**

	*
	*/


object CheckAmmissibilita extends LoggingSupport {

	val format = new SimpleDateFormat("yyyy-MM-dd")
	val UTF8_BOM =  "\uFEFF"

	/**
		* Legge le variabili del file di properties.
		*/
	val propertiesC =new CreateProperties(System.getProperty("user.dir"))
	val prop:Properties = propertiesC.prop

	/*
   * connessione db Oracle
   */
	val url:String = prop.getProperty("spark.app.url")
	val user:String = prop.getProperty("spark.app.user")
	val password:String = prop.getProperty("spark.app.password")
	val driver = prop.getProperty("spark.app.jdbc.driver")
	Class.forName(driver)

	/**
		* Perdita di tensione
		*/

	val v2_2G:String = prop.getProperty("spark.app.v2_2G")
	val regex_2G:String = prop.getProperty("spark.app.regex_2G")
	val verbose:String=  prop.getProperty("spark.app.verbose")
	val printdtframe:String=  prop.getProperty("spark.app.printdataframe")
	val testlevel:String=  prop.getProperty("spark.app.testlevel")

	/**
		* Definizione files XSD
		*/
	val xsdProp:Properties = propertiesC.xsdProp

	// xsd vecchio tracciato
	val defdomplextypes:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes")
	val defsimpletypes:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes")
	val flusso1_pdo:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo")
	val flusso1_rfo:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo")
	val flusso1_pdo_v2:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo_v2")
	val flusso1_rfo_v2:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo_v2")

	val defdomplextypes_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes.straniere")
	val defsimpletypes_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes.straniere")
	val flusso1_pdo_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo.straniere")
	val flusso1_rfo_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo.straniere")
	val flusso1_pdo_v2_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo_v2.straniere")
	val flusso1_rfo_v2_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo_v2.straniere")

	//xsd altri flussi vecchio tracciato
	val flusso1_vno:String = xsdProp.getProperty("spark.validazione.xsd.flusso_vno")
	val flusso1_rnv:String = xsdProp.getProperty("spark.validazione.xsd.flusso_rnv")
	val flusso1_pno:String = xsdProp.getProperty("spark.validazione.xsd.flusso_pno")
	val flusso1_rno:String = xsdProp.getProperty("spark.validazione.xsd.flusso_rno")
	val flusso1_snm:String = xsdProp.getProperty("spark.validazione.xsd.flusso_snm")
	val flusso1_sof:String = xsdProp.getProperty("spark.validazione.xsd.flusso_sof")
	val flusso1_sos:String = xsdProp.getProperty("spark.validazione.xsd.flusso_sos")
	val flusso1_snf:String = xsdProp.getProperty("spark.validazione.xsd.flusso_snf")
	val flusso1_sns:String = xsdProp.getProperty("spark.validazione.xsd.flusso_sns")
	val flusso1_rsn:String = xsdProp.getProperty("spark.validazione.xsd.flusso_rsn")


	// xsd nuovo tracciato
	val defdomplextypes_2G:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes_2G")
	val defsimpletypes_2G:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes_2G")
	val flusso_new_pdo:String = xsdProp.getProperty("spark.validazione.xsd.flusso_new_pdo")
	val flusso_new_rfo:String = xsdProp.getProperty("spark.validazione.xsd.flusso_new_rfo")


	val defdomplextypes_2G_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes_2G.straniere")
	val defsimpletypes_2G_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes_2G.straniere")
	val flusso_new_pdo_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso_new_pdo.straniere")
	val flusso_new_rfo_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso_new_rfo.straniere")



	val flusso_smis:String = xsdProp.getProperty("spark.validazione.xsd.flusso_smis")


  val schema_ammissibilita = StructType(Array(
    StructField("nome_file", StringType, true),
    StructField("ammissibile", StringType, true),
    StructField("codice_causale", StringType, true),
    StructField("motivazione", StringType, true),
    StructField("dirpart", StringType, true)
  ))

/**
* controllo checkAmmissibilita attivo per i tipo 9xx
	* mev 20-030
*/
	val isCheckAmmiOn:String = prop.getProperty("spark.app.ammissibilita.checkAmmiOn")

	def getParentSezXML(xml :scala.xml.Elem): scala.xml.NodeSeq ={
		val sezflusso =(xml \\ "FlussoMisure")
		val sezflusso1 =(xml \\ "FlussoMisureR")
		val sezflusso2 =(xml \\ "FlussoDati")

		if( sezflusso.length > 0 ) sezflusso else if( sezflusso1.length > 0 ) sezflusso1  else sezflusso2
	}



	/**
		* identifica i file di misura del nuovo formato 2G
		* controllo che nel file ci siano i tag xml Forfait e GruppoMis e la sezione DatiPdp e non ci deve essere il tag CifreAtt
		* che sono obbligatori nel nuovo formato
		*/
	def isNuovo2G(xmldata: scala.xml.Elem ,codflusso:String) : Boolean = {

		val sezflusso=getParentSezXML(xmldata)
		val datiPod = (sezflusso \\ "DatiPod")

		val isNewFlussoA=datiPod.map(line => {
			val sezDatiPdp = (line \\ "DatiPdp")
			val res = if (sezDatiPdp.length == 0) {
				false
			} else {
				val tmp1 = (sezDatiPdp \ "Forfait").text
				val tmp2 = (sezDatiPdp \ "GruppoMis").text
				val tmp3 = (sezDatiPdp \ "CifreAtt").text + "" +(sezDatiPdp \ "PotContrImp").text + "" +(sezDatiPdp \ "PotDisp").text
				val ret = if ((tmp1.isEmpty() || tmp1 == null || tmp2.isEmpty() || tmp2 == null) || !(tmp3.isEmpty() || tmp3 == null)) {
					false
				} else {
					true
				}
				ret
			}
			res
		}
		)

		val isNewFlusso= isNewFlussoA.filter(f=>f==true)

		val ret = if((isNewFlusso.size > 0 && isNewFlussoA.size==isNewFlusso.size) || codflusso=="SMIS"){
			if(codflusso=="SMIS" || (codflusso.length>=5 && codflusso.toUpperCase().takeRight(2)=="2G"))
				true
			else
				false
		}else{
			false
		}
		ret
	}

	def getAllPivaMercatoLibero() : List[String] = {
		var dictionary = Map(""->"")
		try{

			val conn:Connection = DriverManager.getConnection(url, user, password)

			//estraggo tutte le partite ive degli udd per il mercato libero
			val ps = conn.prepareStatement("select distinct az.t_piva from rcu.rcu_emt x, rcu.rcu_azienda az where x.n_id_emt=az.n_id_azienda")
			val rs = ps.executeQuery()
			var piv_list=""
			while(rs.next() ){
				if(piv_list=="")
				 piv_list =  rs.getString("t_piva")
				else
					piv_list =  piv_list + ";" + rs.getString("t_piva")

			}
			rs.close()
			ps.close()



			val ll=piv_list.split(";").toList
      log.info("*** Numero di piva estratte per controllo su DP0426 : "+ ll.length.toString)

      ll

		}catch{
			case e: Exception => {e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				List()
			}
		}
	}



	def tryToValidation(rdd_input: (String,String),list_udd_piva:Broadcast[List[String]]) : (Boolean,(String,String),String,Boolean,String,String) = {

		val list_udd_piva_2=list_udd_piva.value


    val f=new File(rdd_input._1)
    val dirPath=f.getParent
    val filename=f.getName
    val sp = rdd_input._1.split(File.separator)
    val annomesegiorno_timestamp=((s"${sp(sp.length - 3)}${sp(sp.length - 2)}"))

		if(!filename.toLowerCase().endsWith("xml"))
			{
				//002: Verifichiamo che il file abbia estensione xml
				return (false, (filename, "Il tipo dato non è corrispondente al formato definito"), "002", true,dirPath,annomesegiorno_timestamp)
			}

		if(verbose=="true")
			log.info("***** Avvio Validazione XSD ")


		val noBom = if(rdd_input._2.startsWith(UTF8_BOM)){
			rdd_input._2.substring(1)
		}else{
			rdd_input._2
		}



		try {
			val xml = XML.loadString(noBom)
      val sezflusso=getParentSezXML(xml)
      val cod_flusso = (sezflusso \ "@CodFlusso").text
      val cod_flusso1G=cod_flusso.substring(0,3)
      val isNewFlusso=isNuovo2G(xml,cod_flusso)

			val keys2G = List("PDO2G","PNO2G","VNO2G","SNM2G","RFO2G","RNO2G","RNV2G" ,"RSN2G","SMIS")
			val keys1G = List("PDO","PNO","VNO","SNM","RFO","RNO","RNV" ,"RSN","SOF","SOS","SNF","SNS")


			if(isNewFlusso) {
				if (!keys2G.exists(cod_flusso.contains(_))) {
					//003: Verifichiamo che il codice_flusso non sia tra quelli previsti per i 2G esempio PPP2G
					return (false, (filename, "Il codice univoco della prestazione non è previsto"), "003", isNewFlusso,dirPath,annomesegiorno_timestamp)

				}
			}
			else {
				if (!keys1G.exists(cod_flusso.contains(_))) {
					//003: Verifichiamo che il codice_flusso non sia tra quelli previsti per gli 1G esempio PPP
					return (false, (filename, "Il codice univoco della prestazione non è previsto"), "003", isNewFlusso,dirPath,annomesegiorno_timestamp)
				}
			}


			if(verbose=="true")
				log.info("***** Avvio Validazione nome file " + rdd_input._1)

			//004 : verifica validazione (015 errore generico)
      val rdd = validationXSD(rdd_input,cod_flusso,isNewFlusso)


			if (rdd._1) {
				try {


					val nome_file = if (rdd._2._1.lastIndexOf("/") > -1) rdd._2._1.substring(rdd._2._1.lastIndexOf("/") + 1).split("_")
					else if (rdd._2._1.lastIndexOf("\\") > -1) rdd._2._1.substring(rdd._2._1.lastIndexOf("\\") + 1).split("_")
					else rdd._2._1.split("_")

					if(nome_file.length<6)
						{
							//904 : Verifica che la nomenclatura rispetti la definizione <pivadistributore>_<pivautente>_<annomese>_<codflusso>_<timestamp>_<codcontrdisp>
							return (false, (filename, "Il file non rispetta la struttura prevista"), "904", isNewFlusso,dirPath,annomesegiorno_timestamp)
						}


					val isNewTrack = rdd._4
					val PIvaDistributoreNF = nome_file(0)
					val PIvaUtenteNF = nome_file(1)
					val MeseAnnoNF = nome_file(2)
					val cod_flussoF = nome_file(3)
					val CodContrDispNF_ = ("000000" + nome_file(5).split("\\.")(0)) takeRight 6


          val annoMeseGiornoDir = ((s"${sp(sp.length - 3)}-${sp(sp.length - 2)}") take 7)+"-01"

					// mev21-030
					if (isCheckAmmiOn.equals("true")){
						if((!Try(PIvaDistributoreNF.toLong).isSuccess || PIvaDistributoreNF.length!=11 )  ||
							(PIvaUtenteNF.length>16) ||
							(!Try(MeseAnnoNF.toLong).isSuccess) )
							return (false, (filename, "Il file non rispetta la struttura prevista"), "904", isNewFlusso,dirPath,annomesegiorno_timestamp)
					}

					// mev21-030
					if (isCheckAmmiOn.equals("true")){
						if(cod_flusso.trim().toLowerCase != cod_flussoF.trim().toLowerCase){
							//903 : Verifica in nomenclatura file che il codice del flusso corrisponda al codice flusso all'interno del file xml
							return (false, (filename, "Il codice del flusso non è previsto /coerente"), "903", isNewFlusso,dirPath,annomesegiorno_timestamp)
						}
					}

					//informazioni tag xml
					val PIvaDistributoreTX = ((xml \\ "IdentificativiFlusso" \\ "PIvaDistributore") text)
					val PIvaUtenteTX = (xml \\ "IdentificativiFlusso" \\ "PIvaUtente") text
					val CodContrDispTX = (xml \\ "IdentificativiFlusso" \\ "CodContrDisp") text




					val dataNf = MeseAnnoNF.substring(0, 6)
					val sezflusso = getParentSezXML(xml)
					val datiPod = (sezflusso \ "DatiPod")
					val hasSezMeseAnno : Boolean=((datiPod \ "MeseAnno").length > 0)

					/*val dpRes2 = if (hasSezMeseAnno) {
						datiPod.theSeq.map(y => ((y \ "MeseAnno").text).split("/")).map(x => if(x.length==2) (x(1) + x(0)) else "200001" ).filter(!_.equals(dataNf))
					} else {
						Seq()
					}*/


					val timeZone = prop.getProperty("spark.app.time_zone")
					val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
					val annomese_cur =(cal.get(Calendar.YEAR) + ("0" + (cal.get(Calendar.MONTH) + 1) takeRight 2 )).toInt
					val misura_futura=if(cod_flusso.toUpperCase =="SMIS" || cod_flusso.toUpperCase =="SNS" || cod_flusso.toUpperCase =="SOS" || cod_flusso.toUpperCase()== "SNF" || cod_flusso.toUpperCase() =="SOF") false else { if(annomese_cur < dataNf.toInt ) true else false}

   				val checkMisure=if(cod_flusso1G != "")
						{

              val dpIt = datiPod.theSeq

              val res= dpIt.flatMap { dp =>

                val anno_F = dataNf.take(4)
                val mese_F = dataNf.takeRight(2)

                val meseanno_tmp = if (!(dp \ "MeseAnno").text.trim().equals("")) "00/"+(dp \ "MeseAnno").text else "00/"+mese_F +"/"+anno_F

                val data_misura = if (!(dp \ "DataMisura").text.trim().equals("")) (dp \ "DataMisura").text else null
                val data_prest = if (!(dp \ "DataPrest").text.trim().equals(""))(dp \ "DataPrest").text else null
                var data_voltura = if ((dp \ "DataVoltura").text.trim().equals("")) null else (dp \ "DataVoltura").text
                val tmp = if (data_misura != null) data_misura.split("/")else if (data_prest != null) data_prest.split("/") else if (data_voltura != null) data_voltura.split("/")  else meseanno_tmp.split("/")

                val meseAnno_N = ((dp \ "MeseAnno").text).split("/")

                val anno_N = if(meseAnno_N.length>=2) meseAnno_N(1) else tmp(2)
                val mese_N = if(meseAnno_N.length>=2) meseAnno_N(0) else tmp(1)

								val annomese= anno_N.toString + (("0" + mese_N.toString) takeRight 2)
								val meseAnnoOK=annomese.equals(dataNf)

                val dateformat = new SimpleDateFormat("yyyy-MM-dd")

                val curdate:java.util.Date = dateformat.parse(annoMeseGiornoDir)
                cal.setTime(curdate)
                cal.add(Calendar.MONTH, -60);
                val data_60mesi=cal.getTime

                val strfldate =s"${anno_N}-${mese_N}-01"
                val datefile:java.util.Date=dateformat.parse(strfldate)

                if(datefile.before(data_60mesi))
									Seq((meseAnnoOK,true))
                else
									Seq((meseAnnoOK,false))

              }
							val AM_N_OK=res.filter(f => f._1==false).length >0
							val MS_U60=res.filter(f => f._2==true).length >0
							(AM_N_OK,MS_U60)

						}else (true,false)

					val misura_under_60mesi=checkMisure._2
					val AM_NOTOK=checkMisure._1

					// mev21-030
					var isValid2= (true, "OK", "")
					if (isCheckAmmiOn.equals("true")) {
						 isValid2 = if (PIvaDistributoreNF != PIvaDistributoreTX) {
							(false, "I dati d’intestazione del file non sono coerenti con quelli presenti nel SII : PIvaDistributore non coerente", "905")
						} else if (PIvaUtenteNF != PIvaUtenteTX) {
							(false, "I dati d’intestazione del file non sono coerenti con quelli presenti nel SII : PIvaUtente non coerente", "905")
						} else if (CodContrDispNF_ != CodContrDispTX) {
							(false, "I dati d’intestazione del file non sono coerenti con quelli presenti nel SII : CodContrDisp non coerente", "905")
						} else if (AM_NOTOK) {
							//mev 21-030
							//if(cod_flusso1G=="SNM" || cod_flusso1G=="RSN" || cod_flusso1G=="SOS" || cod_flusso1G=="SNS"  || cod_flusso1G=="SNF" || cod_flusso1G=="SOF")
							//{(true, "OK","")}
							//else
							(false, "I dati d’intestazione del file non sono coerenti con quelli presenti nel SII : MeseAnno non coerente", "905")
						} else if (misura_futura) {
							(false, "I dati del file si riferiscono ad un mese futuro ", "914")
						} else if (misura_under_60mesi) {
							(false, "I dati del file si riferiscono ad un mese antecedente all’attuale per più di 5 anni", "914")
						} else if (CodContrDispTX.toLowerCase().trim == "dp0426") {
							if (!list_udd_piva_2.exists(PIvaUtenteTX.equals(_))) (false, "Partita Iva non prevista per il DP0426", "916")
							else (true, "OK", "")
						} else {
							(true, "OK", "")
						}
					}



					if (isValid2._1) {
						(isValid2._1, (filename,rdd._2._2), "OK", isNewTrack,dirPath,annomesegiorno_timestamp)
					} else {
						(false, (filename, isValid2._2), isValid2._3, isNewTrack,dirPath,annomesegiorno_timestamp)
					}


				} catch {
					case e: Exception =>
						if(verbose=="true")
						e.printStackTrace()

						//015 : errore generico
						(false, (filename, "La richiesta non è eseguibile : " + e.getMessage), "015", false,dirPath,annomesegiorno_timestamp)
				}
			} else {
				(rdd._1, (filename, rdd._2._2), rdd._3, rdd._4,dirPath,annomesegiorno_timestamp)
			}
		}
		catch {
			case e: Exception =>
				//e.printStackTrace()

				//errore generico
				(false, (filename, "La richiesta non è eseguibile :" + e.getMessage), "015", false,dirPath,annomesegiorno_timestamp)
		}
	}

	/**
		* Valida la consistenza file xml tramite dei XSD in termini di vincoli: quali elementi e attributi possono apparire,
		* in quale relazione reciproca, quale tipo di dati può contenere; al fine di accertare se i tipi di dati appartengono al documento xml.
		* @param rdd rappresenta il file xml.
		* @return un valore booleano corrispondente alla validazione e un messaggio d'errore.
		*/
	def validationXSD(rdd: (String,String) ,cod_flusso:String,isNewFlusso:Boolean) : (Boolean,(String,String),String,Boolean) =
	{


		val keys1 = List("PDO","PNO","VNO","SNM")
		val keys2 = List("RFO","RNO","RNV" ,"RSN")

		//MANCA RNM
		val hashMap1: HashMap[String, String] =
			HashMap(("VNO",flusso1_vno), ("RNV",flusso1_rnv),
				("PNO",flusso1_pno), ("RNO",flusso1_rno),
				("SNM",flusso1_snm), ("SOF",flusso1_sof),
				("SOS",flusso1_sos), ("SNF",flusso1_snf),
				("SNS",flusso1_sns), ("RSN",flusso1_rsn), ("SMI",flusso_smis))

		try{
			val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)//CRLF e LF

			val nomeFile = rdd._1
			val cod_flusso1G=cod_flusso.substring(0,3)

			if(verbose=="true") {
				if(cod_flusso.trim().contains("SMIS"))
					log.info("***** Tracciato flusso SMIS - file :" + nomeFile)
				else if (isNewFlusso)
					log.info("***** Versione Tracciato flusso : nuovo ("+ cod_flusso +") - file :" + nomeFile)
				else
					log.info("***** Versione Tracciato flusso : vecchio ("+ cod_flusso +") - file :" + nomeFile)
			}


			val f1:Source = if(isNewFlusso && !cod_flusso.trim().contains("SMIS")){
				new StreamSource(defdomplextypes_2G)
			}else{
				if(((!isNewFlusso && !cod_flusso.contains("PDO") && !cod_flusso.contains("RFO")) || cod_flusso.contains("SMIS"))  && hashMap1.keySet.exists(_ == cod_flusso1G)){
					val tmp=hashMap1(cod_flusso1G)
					val path = new File(tmp).getParent();
					val fx= path +"/FlussiDatiMisuraPrelievoEE-DefComplexTypes.xsd" ;

					new StreamSource(fx)
				}else{
					new StreamSource(defdomplextypes)
				}
			}

			val f2:Source = if(isNewFlusso && !cod_flusso.trim().contains("SMIS")){
				new StreamSource(defsimpletypes_2G)
			}else{
				if(((!isNewFlusso && !cod_flusso.contains("PDO") && !cod_flusso.contains("RFO")) || cod_flusso.contains("SMIS")) && hashMap1.keySet.exists(_ == cod_flusso1G)){
					val tmp=hashMap1(cod_flusso1G)
					val path = new File(tmp).getParent();
					val fx=path+"/FlussiDatiMisuraPrelievoEE-DefSimpleTypes.xsd" ;

					new StreamSource(fx)
				}else {
					new StreamSource(defsimpletypes)
				}
			}


			var f3_ok=true
			val f3 =if(isNewFlusso && keys2.exists(cod_flusso.contains(_)) ) {
				new StreamSource(flusso_new_rfo)
			}else if(isNewFlusso && keys1.exists(cod_flusso.contains(_))) {
				new StreamSource(flusso_new_pdo)
			}else	if(cod_flusso.contains("SMIS")) {
				new StreamSource(flusso_smis)
			}else	if(cod_flusso.contains("RFO") && nomeFile.contains( v2_2G )){
				new StreamSource(flusso1_rfo_v2)
			}else if(cod_flusso.contains("RFO")){
				new StreamSource(flusso1_rfo)
			}else if(cod_flusso.contains("PDO") && nomeFile.contains( v2_2G )) {
				new StreamSource(flusso1_pdo_v2)
			}else if(cod_flusso.contains("PDO")){
				new StreamSource(flusso1_pdo)
			}else if(!isNewFlusso && !cod_flusso.contains("PDO") && !cod_flusso.contains("RFO") && hashMap1.keySet.exists(_ == cod_flusso1G)){
				if(verbose=="true")
					log.info("xsd per flusso " + cod_flusso1G + " : " +hashMap1(cod_flusso1G))

				new StreamSource(hashMap1(cod_flusso1G))
			}else{
				f3_ok=false
				new StreamSource("")
			}

			if(verbose=="true") {
				log.info(f1.getSystemId)
				log.info(f2.getSystemId)
				if (f3_ok) log.info(f3.getSystemId)
			}


			try{
				val sourceList = if(f3_ok)Array(f3,f1,f2)else Array(f1,f2)

				val schema = factory.newSchema( sourceList )
				val validator = schema.newValidator()
				val ss = new StreamSource(new File( rdd._1.replaceFirst("file:///", "/").replaceFirst("file:/", "/").replaceFirst("file://", "/") ))
				validator.validate(ss)
			}catch{
				case e: Exception =>
					val err = e.getMessage()
					if(err.contains("PIVAType")){

						val f11:Source = if(isNewFlusso){
							new StreamSource(defdomplextypes_2G_straniere)
						}else{
							new StreamSource(defdomplextypes_straniere)
						}

						val f22:Source = if(isNewFlusso){
							new StreamSource(defsimpletypes_2G_straniere)
						}else{
							new StreamSource(defsimpletypes_straniere)
						}


						f3_ok=true
						val f33 =if(isNewFlusso && keys2.exists(cod_flusso.contains(_)) ) {
							new StreamSource(flusso_new_rfo_straniere)
						}else if(isNewFlusso && keys1.exists(cod_flusso.contains(_))) {
							new StreamSource(flusso_new_pdo_straniere)
						}else	if(cod_flusso.contains("SMIS")) {
							new StreamSource(flusso_smis)
						}else	if(cod_flusso.contains("RFO") && nomeFile.contains( v2_2G )){
							new StreamSource(flusso1_rfo_v2_straniere)
						}else if(cod_flusso.contains("RFO")){
							new StreamSource(flusso1_rfo_straniere)
						}else if(cod_flusso.contains("PDO") && nomeFile.contains( v2_2G )) {
							new StreamSource(flusso1_pdo_v2_straniere)
						}else if(cod_flusso.contains("PDO")){
							new StreamSource(flusso1_pdo_straniere)
						}else{
							f3_ok=false
							new StreamSource("")
						}



						try {
							val newFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
							val newSourceList = if (f3_ok) Array(f33, f11, f22) else Array(f11, f22)
							val newSchema = newFactory.newSchema(newSourceList)
							val newValidator = newSchema.newValidator()
							val newSs = new StreamSource(new File(rdd._1.replaceFirst("file:///", "/").replaceFirst("file:/", "/").replaceFirst("file://", "/")))
							newValidator.validate(newSs)
						}catch {
							case e2: Exception =>
								val err2 = e2.getMessage()
								//004 : Verifica tramite validazione xsd
								return (false, (rdd._1, "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati : " + err2),"004",isNewFlusso)
						}
					}else{

						if(verbose=="true")
							e.printStackTrace()
						//004 : Verifica tramite validazione xsd
						return (false, (rdd._1, "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati : " + e.getMessage()),"004",isNewFlusso)
					}
			}

			(true,rdd,"OK",isNewFlusso)
		}catch{
			case ex: Exception =>
				if(verbose=="true")
					ex.printStackTrace()
				//015: errore generico
				(false, (rdd._1, "La richiesta non è eseguibile + "+ex.getMessage() ),"015",false)
		}

	}


	def isEmpty[T](rdd : RDD[T]) = {
		try {
			rdd.take(1).length == 0
		}catch {
			case e: Exception =>  true
		}
	}


	def walkTree(file:File):Iterable[File]={
		val children = new Iterable[File]{
			def iterator = if(file.isDirectory) file.listFiles.iterator else Iterator.empty
		}
		Seq(file)++: children.flatMap(walkTree(_))
	}

	def getCountFiles(dir:File,annomesegiornocheck:String): (Long,Boolean)= {
		var count:Long =0
		var isdate_diff=false
		for (f <- walkTree(dir)) {
			if (f.getName.toLowerCase.matches(".*\\.xml")) {
				count = count + 1

				if(!isdate_diff) {
					val annomesegiornodir = f.getParentFile().getParentFile().getName() + f.getParentFile().getName()
					isdate_diff=(!(annomesegiornodir.equals(annomesegiornocheck)))
				}
			}

		}

		return (count,isdate_diff)
	}


	def can_go_ammissibilita(): Boolean =
	{
		val vmName = ManagementFactory.getRuntimeMXBean.getName
		val p = vmName.indexOf("@")
		val cur_pid = vmName.substring(0, p)


		//verifica che non ci sia decompressione o ingestione in corso
		val tmpDec = Try("ps aux" #| "grep it.au.misure.cli.FlussoMisureTool" #| s"grep -v ${cur_pid}" #| "grep Da" !!) getOrElse("")
		val tmpInj = Try("ps aux" #| "grep it.au.misure.cli.FlussoMisureTool" #| s"grep -v ${cur_pid}" #| "grep ia" !!) getOrElse("")


		var pidfound=""

		if(tmpDec!="" && tmpDec.contains(".jar -Da"))
		{
			val vals=tmpDec.split(" ")
			val utente=vals(0)
			for(i<-1 to vals.length-1) {
				if (vals(i).trim() != "" && pidfound ==""){
					pidfound=vals(i)
					if(cur_pid!=pidfound){
						log.info("Attenzione è stato trovato un processo di decompressione ammissibilità in corso , attendere la fine del processo in corso e riprovare")
						log.info("Utente processo decompressione ammissibilità in corso : " + utente)
						log.info("PID processo decompressione ammissibilità in corso : " + pidfound)
						return false
					}

				}
			}

		}

		pidfound=""
		if(tmpInj!="" && tmpInj.contains(".jar -ia"))
		{
			val vals=tmpInj.split(" ")
			val utente=vals(0)
			for(i<-1 to vals.length-1) {
				if (vals(i).trim() != "" && pidfound ==""){
					val pid=vals(i)
					if(cur_pid!=pid) {
						log.info("Attenzione è stato trovato un processo di ammissibilità in corso , attendere la fine del processo in corso e riprovare")
						log.info("Utente processo ammissibilità in corso : " + utente)
						log.info("PID processo ammissibilità in corso : " + pidfound)
						return false
					}

				}
			}

		}
		return true
	}


	/**
		* Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
		* @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
		*/
	def main(args: Array[String]) {

		val commandLineOptions = new CommandLineOptions()
		val commonsCliUtils = new CommonsCliUtils()
		val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObjMaster = new CommonsCliUtils().getArgs(commandLine)



    if(!can_go_ammissibilita) return

    if (!commandLine.hasOption(commandLineOptions.anno.getOpt) || !commandLine.hasOption(commandLineOptions.mese.getOpt) || !commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
      log.info("**** Bisogna specificare l'anno il mese e il giorno ")
			return
    }

		log.info("***** Inizio processo " + argsObjMaster.appName + " *****")
		val nameApp= argsObjMaster.appName


		log.info("***** current user " + System.getProperty("user.name") + "****")
		log.info(propertiesC.printEnvVar)


		val injectionTmp: String = argsObjMaster.injectionTmp+"_ammissibilita"
		val rootDir: String = argsObjMaster.rootDir

		log.info("*** Path file scompattati: " + injectionTmp)
		log.info("*** rootDir: " + rootDir)

    val dirRoothAmm=if(commandLine.hasOption(commandLineOptions.injection2G.getOpt))prop.getProperty("spark.app.ammissibilita.2G.path") else prop.getProperty("spark.app.ammissibilita.1G.path")
    log.info(s"*** Root path generazione file di ammissibilità : ${dirRoothAmm}")

		 val anno_in = argsObjMaster.anno
		 val mese_in = argsObjMaster.mese
		 val giorno_in: String = argsObjMaster.giorno

    log.info(s"*** Avvio ammissibilità sulle misure decompresse utilizzando come timestamp dei file txt ${giorno_in}/${mese_in}/${anno_in}")

    val timeZone = prop.getProperty("spark.app.time_zone")
    val calWr = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
    calWr.set(Calendar.YEAR, anno_in.toInt);
    calWr.set(Calendar.MONTH, mese_in.toInt - 1);
    calWr.set(Calendar.DAY_OF_MONTH, giorno_in.toInt);
    calWr.add(Calendar.DATE,1)

    val anno_wr:String = Integer.toString(calWr.get(Calendar.YEAR))
    val mese_wr:String = "0" + Integer.toString(calWr.get(Calendar.MONTH) + 1) takeRight 2
    val giorno_wr:String = "0" + Integer.toString(calWr.get(Calendar.DAY_OF_MONTH)) takeRight 2

    log.info(s"*** Scrittura su cartella del ${giorno_wr}/${mese_wr}/${anno_wr}")

		val tmpRootDir = s"${injectionTmp}${File.separator}${rootDir}"

		log.info("*** Avvio controllo numero file per calibrazione partizioni ****")

		val totFC=getCountFiles(new File(tmpRootDir),anno_in+mese_in+giorno_in)

		if(totFC._2)
			{
				log.info(s"*** ATTENZIONE NELLA CARTELLA DI DECOMPRESSIONE SONO STATI TROVATI DEI FLUSSI APPARTENENTI AD UN GIORNO DIVERSO DAL ${giorno_in+"/"+mese_in+"/"+anno_in}")
				log.info("*** FERMARE LA PROCEDURA SE NON E' UN LANCIO INTENZIONALE!!!")
				Thread.sleep(8000)

			}
		val totF=totFC._1

		log.info("*** Tot files :" + totF.toString +" ****")

		val conf = new SparkConf()
			.setAppName(nameApp)
			.set("spark.shuffle.service.enabled", "false")
			.set("spark.dynamicAllocation.enabled", "false")
			.set("spark.io.compression.codec", "snappy")
			.set("spark.rdd.compress", "true")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")



			.setMaster(argsObjMaster.master)


		val sc = new SparkContext(conf)
		sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

		//nuove
		sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
		sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

		//fine nuove

		sc.setLogLevel(argsObjMaster.logLevel)

		val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions")

		val sqlCtx = new HiveContext(sc)
		sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
		sqlCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
		sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
		sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
		sqlCtx.setConf("hive.exec.dynamic.partition", "true")
		sqlCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

		//nuove
		sqlCtx.setConf("spark.sql.parquet.mergeSchema", "false")
		sqlCtx.setConf("spark.sql.parquet.filterPushdown", "true")
		sqlCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")


		//fine nuove
		try {


      val slash = sc.getConf.get("spark.flusso.misure.slash")

      log.info("*** sc.master: " + sc.master)


      //XSD
      log.info("*** xsd old defdomplextypes: " + defdomplextypes)
      log.info("*** xsd old defsimpletypes: " + defsimpletypes)
      log.info("*** xsd flusso1_pdo: " + flusso1_pdo)
      log.info("*** xsd flusso1_rfo: " + flusso1_rfo)
      log.info("*** xsd flusso1_pdo_v2: " + flusso1_pdo_v2)
      log.info("*** xsd flusso1_rfo_v2: " + flusso1_rfo_v2)

      //XSD NEW
      log.info("*** xsd new defdomplextypes_new: " + defdomplextypes_2G)
      log.info("*** xsd new defsimpletypes_new: " + defsimpletypes_2G)
      log.info("*** xsd flusso_pdo_new: " + flusso_new_pdo)
      log.info("*** xsd flusso_rfo_new: " + flusso_new_rfo)
      log.info("*** xsd flusso_smis: " + flusso_smis)


      log.info("*** verbose: " + verbose)
      log.info("*** testlevel: " + testlevel)


      val elencoUDDTmp: List[String] = scansionaUDD(tmpRootDir)

      //valore medio di un file 1,52 mb
      val numpartitions = (((totF * 1.52) / (48 * 5))).toInt


      val maxparts = ((sc.defaultParallelism - (sc.defaultParallelism * 0.08)).toInt * 2).toInt
      val partitions = if (numpartitions < 48) 48
      else if (numpartitions > maxparts) maxparts
      else numpartitions

      log.info("*** partizioni massime :" + maxparts.toString)

      val rddUDDTmp = sc.parallelize(elencoUDDTmp, partitions.toInt).setName("Scansiona alberatura temporanea")
      //rddUDDTmp.persist(StorageLevels.MEMORY_ONLY_SER)

      val parts: Int = rddUDDTmp.partitions.size

      log.info("*** numero di partizioni  :" + parts.toString)

      val lists = rddUDDTmp.mapPartitions { partition =>
        val ret = partition
          .map(rddUDDTmpDir => leggiPathXml(new File(rddUDDTmpDir)))
          .flatMap(f => f)
        ret
      }.collect().map(xml => "file://" + slash + xml.getPath).toList

      val lenf = lists.length

      log.info("*** numero di file in processamento :" + lenf.toString)

      val rddValidAll = sc.wholeTextFiles(lists.mkString(","), parts.toInt).setName("Acquisizione files di misurazione")
      log.info("*** num partizioni dopo lettura files : " + rddValidAll.partitions.size.toString)


      val list_piva = sc.broadcast(getAllPivaMercatoLibero())
			log.info("*** utilizzo metodo  tryToValidation " )
			log.info("*** controlli ammissibilita tipo 9xx abilitati:  " + isCheckAmmiOn )
      val rdd2 = rddValidAll.map(tryToValidation(_, list_piva)).setName("Validazione xsd")
      rdd2.persist(StorageLevels.MEMORY_ONLY_SER)

      log.info("*** Validazione File + XSD OK")


      //NOME_FILE;AMMISSIBILE;CODICE_CAUSALE;MOTIVAZIONE;PARTIZIONE PER DIRECTORY SCRITTURA
      val report_nonammissibili = rdd2.map(f => {
        if (!f._1) {
          val dir_tmp = f._5.split(File.separator)
          val dir_tmp2 = dir_tmp.take(dir_tmp.length - 1)
          val dirout = dir_tmp2.mkString(File.separator) + File.separator + mese_wr + giorno_wr
          val time_stampfile=if(f._6=="")anno_in + mese_in + giorno_in else f._6

          val ind = dir_tmp.indexWhere(f => f.equals("DISTRIBUTORE")) + 1
          val fname = if (ind >= 0) {
            val tmp = dir_tmp(ind).split("_")
            if (tmp.length >= 3)
              "ReportAmm_" + tmp(1) + "_" + tmp(2) + "_" + time_stampfile + ".txt"
            else
              "ReportAmm_" + time_stampfile + ".txt"
          }
          else "ReportAmm_" + time_stampfile + ".txt"

          Row(f._2._1 + ";N;" + f._3 + ";" + f._2._2, dirout, fname)
        }
        else
          Row("", "", "")
      }).filter(r => r.getString(0) != "").collect()


      val header = "NOME_FILE;AMMISSIBILE;CODICE_CAUSALE;MOTIVAZIONE"

      var dictfiles: Map[String, String] = Map("" -> "")

      var cc=0
      for (rw <- report_nonammissibili) {
        val txt = rw.getString(0)

        val fdir = rw.getString(1).replace("file:", "")


        val dir = new File(fdir.replace(tmpRootDir, dirRoothAmm))
        if (!dir.exists())
          dir.mkdirs()
        else
          {
            dir.delete()
            dir.mkdirs()
          }


        val fname = new File(dir + File.separator + rw.getString(2))


        if (!fname.exists()) {

          if (fname.createNewFile()) {
            val bw = new BufferedWriter(new FileWriter(fname,true))
            bw.write(header)
            bw.newLine()
            bw.write(txt)
            bw.close()
            cc=cc+1
            dictfiles += (fname.getAbsolutePath -> "OK")
          }
          else log.info(s"Attenzione non sono riuscito a creare il file ${fname.getAbsolutePath}")
        } else {
          if (dictfiles.contains(fname.getAbsolutePath)) {
            val bw = new BufferedWriter(new FileWriter(fname, true))
            bw.newLine()
            bw.write(txt)
            bw.close()
            cc=cc+1
          }
          else{
            fname.delete()
            fname.createNewFile()

            val bw = new BufferedWriter(new FileWriter(fname,true))
            bw.write(header)
            bw.newLine()
            bw.write(txt)
            bw.close()
            dictfiles += (fname.getAbsolutePath -> "OK")
            cc=cc+1
        }
      }

    }


      log.info("*** Numero di file non ammissibili generati : " + (dictfiles.size-1).toString + " ***")
      log.info("*** Numero di file xml non ammissibili : " + (report_nonammissibili.length).toString  + " - scritti : "+ cc.toString + " ***")


     /* val dfData = sqlCtx.createDataFrame(rdd2_report_nonammissibili.filter(r => r.getString(0)!=""), schema_ammissibilita)

      val locationcsv=s"/user/${System.getProperty("user.name")}/au/misure_ee_au/prt_tbl_ammissibilita_csv"

      if (sqlCtx.tableNames.contains("prt_tbl_ammissibilita_csv"))
        sqlCtx.sql("DROP TABLE IF EXISTS prt_tbl_ammissibilita_csv")


      val querytbl=
        s"""
		   CREATE TABLE prt_tbl_ammissibilita_csv
       (
         nome_file STRING , ammissibile STRING  , codice_causale STRING ,
         motivazione STRING
        )
        PARTITIONED BY (dirpart STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\u003B"  STORED AS TEXTFILE
        LOCATION '${locationcsv}'
        """.stripMargin

      sqlCtx.sql(querytbl)

      dropTable=true

      sqlCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sqlCtx.sql("set hive.exec.dynamic.partition=true")
      sqlCtx.sql("set hive.exec.max.dynamic.partitions=100000")


      dfData
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("dirpart")
        .save(locationcsv)*/





		} catch {
			case ex: FileNotFoundException => ex.printStackTrace()
			case e: Exception => e.printStackTrace()
		} finally {


			sc.stop()
		}


		log.info(s"***** Fine processo ${argsObjMaster.appName} *****")
	}

	def delete(f:File):Boolean = {
		if (f.isDirectory()) {
			for (c <- f.listFiles())
				delete(c);
		}
		if (!f.delete()){
			false
		}else{
			true
		}
	}

	def leggiPathXml(f:File): Array[File] = {
		val ret = f.listFiles()
		if(ret.length > 0 && ret(0) != null && ret(0).isDirectory() ){
			ret.map(xf => leggiPathXml(xf) ).flatMap(f => f)
		}else{
			ret
		}
	}

	def scansionaUDD(injectionPath:String) : List[String] = {
		val abW2 = new ArrayBuffer[(String)]()

		//cartella principale
		for (xmlPrincipaleDir <- new File(injectionPath).listFiles()) {
			if(xmlPrincipaleDir.exists() && xmlPrincipaleDir.isDirectory() && xmlPrincipaleDir.listFiles().length > 0){
				//cartella del distributore
				val pivaDistrDir = xmlPrincipaleDir.getPath()

				try{
					for (xmlUddDir <- new File(pivaDistrDir + File.separator + "DISTRIBUTORE").listFiles()) {
						if( xmlUddDir.exists() && xmlUddDir.isDirectory() &&  xmlUddDir.listFiles().length > 0){
							//cartella del sotteso
							val uddDir = xmlUddDir.getPath()

							if(verbose=="true")
								log.info(uddDir)

							abW2.+=(uddDir)
						}
					}

				}catch{
					case e: Exception =>  e.printStackTrace()
				}

			}
		}
		abW2.toList
	}

}
