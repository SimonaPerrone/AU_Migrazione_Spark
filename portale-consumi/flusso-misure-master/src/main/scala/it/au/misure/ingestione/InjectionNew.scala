package it.au.misure.ingestione

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.lang.management.ManagementFactory
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}
import javax.xml.XMLConstants
import javax.xml.transform.Source
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import it.au.misure.util.{CreateProperties, LoggingSupport}
import it.au.misure.util.Schemas._
import it.au.misure.cli.TypeDataToElab
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.xml._
import scala.reflect.ClassTag
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import sys.process._

/**
	* ==Flusso Misure Inserimento Misure Quarti==
	* E' il processo di ingesione degli xml dal repository. Acquisisce i file di misura ed esegue alcune validazioni assegnando ad ogni record
	* un flag. Le validazioni si riferiscono alla congruenza della coppia Udd-Pod, alla determinazione dello stato e del tipo di trattamento e alla congruenza con l'area di appartenenza.
	* Il processo inserisce le informazioni ricavate dalla lettura dei file xml e dalle validazioni delle informazioni in essi
	* contenuti nella tabella hdfs denominata ''flusso_misure_quarti''.
	*
	*
	* @see [[https://spark.apache.org/docs/latest/job-scheduling.html]]
	*
	*
	*/


object InjectionNew extends LoggingSupport {

	val format = new SimpleDateFormat("yyyy-MM-dd")
	val UTF8_BOM =  "\uFEFF"

	/**
		* Legge le variabili del file di properties.
		*/
	val propertiesC =new CreateProperties(System.getProperty("user.dir"))
	val prop:Properties = propertiesC.prop

	val _dbDest:String = prop.getProperty("spark.app.dbdest")
	/**
		* Perdita di tensione
		*/
	val perdita_380 = prop.getProperty("spark.app.perdita.tensione.380").toDouble
	val perdita_220 = prop.getProperty("spark.app.perdita.tensione.220").toDouble
	val perdita_150 = prop.getProperty("spark.app.perdita.tensione.150").toDouble
	val perdita_1_35 = prop.getProperty("spark.app.perdita.tensione.1_35").toDouble
	val perdita_1 = prop.getProperty("spark.app.perdita.tensione.1").toDouble
	val v2_2G:String = prop.getProperty("spark.app.v2_2G")
	val regex_2G:String = prop.getProperty("spark.app.regex_2G")
	val verbose:String=  prop.getProperty("spark.app.verbose")
	val printdtframe:String=  prop.getProperty("spark.app.printdataframe")
	val testlevel:String=  prop.getProperty("spark.app.testlevel")

	/**
		* Definizione files XSD
		*/
	val xsdProp:Properties = propertiesC.xsdProp
	val xsdProp_old:Properties = propertiesC.xsdProp_old

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
	val tbl_misurequarti:String = "flusso_misure_quarti"//prop.getProperty("spark.app.flussoquarti_table")
	/**
		* controllo checkAmmissibilita attivo per i tipo 9xx
		* mev 21-030
		*/
	val isCheckAmmiOn:String = prop.getProperty("spark.app.ammissibilita.checkAmmiOn")

	def getRowsEaEr(elxml: scala.xml.NodeSeq,commonFields : List[Any] , mese :Int,Kr_Ka : Double,isEa: Boolean,cod_flusso:String="",key_estensione_quarti:String=""):Seq[(List[Any])] = {

		val el = elxml.theSeq

		//passaggio ora solare/legale (attività che esegue solo a ottobre) -Er
		val oraLegale = if (mese == 10) {
			val iResDst = el.map { e =>
				val t = (e \ (s"@Dst") text)

				val tt = if (t.equals("2")) {

					val e1_12_dist2 = (1 to 12).map { q =>
						val el = Try((e \ (s"@E$q") text).replace(",", ".").toDouble)
						el.getOrElse(0D) * Kr_Ka
					}.toList

					e1_12_dist2
				} else {
					Nil
				}

				(t, tt)
			}.filter(f => f._1.equals("2"))

			val ret = if (iResDst.size > 0) {
				iResDst(0)._2
			} else {
				Nil
			}
			ret
		} else {
			Nil
		}

		//passaggio ora solare/legale (attività che si esegue solo a ottobre)
		val iRes = if (mese.toInt == 10) {
			val iResLeg = el.map { e =>
				val giorno = e.text

				val t = (e \ (s"@Dst") text)
				val eValues = if (t.equals("2")) {
					(false, Nil)
				} else if (t.equals("3")) {

					val e9_12_dist3 = (9 to 12).map { q =>
						val el = Try((e \ (s"@E$q") text).replace(",", ".").toDouble)
						el.getOrElse(0D) * Kr_Ka
					} toList

					val e13_96_dist3 = (13 to 96).map { q =>
						val el = Try((e \ (s"@E$q") text).replace(",", ".").toDouble)
						el.getOrElse(0D) * Kr_Ka
					} toList

					val e1_12_dist2 = oraLegale

					(true, e1_12_dist2 ++ e13_96_dist3 ++ e9_12_dist3)
				} else {
					val ret = (1 to 100).map { q =>
						val el = Try((e \ (s"@E$q") text).replace(",", ".").toDouble)
						el.getOrElse(0D) * Kr_Ka
					} toList

					(true, ret)
				}

				if(cod_flusso!="" || key_estensione_quarti!=""){ //per tabella flusso_misure_quarti
					if (isEa) (eValues._1, commonFields ++ List(giorno.toInt,cod_flusso,key_estensione_quarti) ++ eValues._2) else (eValues._1, eValues._2 ++ commonFields)
				}
				else {
					if (isEa) (eValues._1, commonFields ++ List(giorno.toInt) ++ eValues._2) else (eValues._1, eValues._2 ++ commonFields)
				}

			}.filter(f => f._1).map(_._2)

			iResLeg
		} else {
			val iResSol = el.map { e =>
				val giorno = e.text
				val eValues = (1 to 100).map { q =>
					val el = Try((e \ (s"@E$q") text).replace(",", ".").toDouble)
					el.getOrElse(0D) * Kr_Ka
				} toList

				if(cod_flusso!="" || key_estensione_quarti!=""){//per tabella flusso_misure_quarti
					if(isEa)(commonFields ++ List(giorno.toInt,cod_flusso,key_estensione_quarti) ++ eValues) else (eValues ++ commonFields)
				}else{
				if(isEa)(commonFields ++ List(giorno.toInt) ++ eValues) else (eValues ++ commonFields)
				}
			}

			iResSol
		}

		iRes
	}

	def getParentSezXML(xml :scala.xml.Elem): scala.xml.NodeSeq ={
		val sezflusso =(xml \\ "FlussoMisure")
		val sezflusso1 =(xml \\ "FlussoMisureR")
		val sezflusso2 =(xml \\ "FlussoDati")

		if( sezflusso.length > 0 ) sezflusso else if( sezflusso1.length > 0 ) sezflusso1  else sezflusso2
	}

	/**
		* Esegue il parsing dei file xml ed esegue le validazioni richiamando le funzioni ''validazioneOrariePodUdd'' e ''validazioneStatoPod''.
		* @param rdd1 rappresenta il file xml
		* @return una lista di record contenti le informazioni del file xml
		*/
	def letturaEValidazioni_vecchioFormato(rdd1: (Boolean,(String,String),String,Boolean), dataelaborazione:java.sql.Timestamp , xml :scala.xml.Elem ): List[(Row)] = {
		try {

			/*val noBom = if(rdd1._2._2.startsWith(UTF8_BOM)){
        rdd1._2._2.substring(1)
      }else{
        rdd1._2._2
      }
        val xml = XML.loadString(noBom)*/
			//val xml = rdd1._5
			val sezflusso=getParentSezXML(xml)
			val datiPod = (sezflusso \\ "DatiPod")

			val cod_flusso = (sezflusso \ "@CodFlusso").text
			val pivadistributore = (xml \\ "IdentificativiFlusso" \\ "PIvaDistributore") text
			val pivautente = (xml \\ "IdentificativiFlusso" \\ "PIvaUtente") text
			val codContrDisp = (xml \\ "IdentificativiFlusso" \\ "CodContrDisp") text

			val dpIt = datiPod.theSeq


			val dpRes = dpIt.flatMap { dp =>

				val pod = (dp \\ "Pod").text

				val nomefile = rdd1._2._1
				val nomefileT =  new File( nomefile ).getName().toUpperCase()
				val sp = nomefile.split("/")
				val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt

				val timeStamp:Long = nomefile.substring(nomefile.lastIndexOf("/")).split("_")(4).toLong


				/*val meseAnno = ((dp \\ "MeseAnno").text).split("/")
				val anno = meseAnno(1)
				val mese = meseAnno(0)
        */

				val annoMese_F=nomefile.substring(nomefile.lastIndexOf("/")).split("_")(2)
				val anno_F = annoMese_F.take(4)
				val mese_F = annoMese_F.takeRight(2)

				val meseAnno_N = ((dp \ "MeseAnno").text).split("/")

				val meseanno_tmp = if (!(dp \ "MeseAnno").text.trim().equals("")) "00/"+(dp \ "MeseAnno").text else "00/"+mese_F +"/"+anno_F
				val data_misura = if (!(dp \ "DataMisura").text.trim().equals("")) (dp \ "DataMisura").text else null

				val tmp = if (data_misura != null) data_misura.split("/") else meseanno_tmp.split("/")

				val anno = if(meseAnno_N.length>=2) meseAnno_N(1).toInt else tmp(2).toInt
				val mese = if(meseAnno_N.length>=2) meseAnno_N(0).toInt else tmp(1).toInt

				val tipo_rettifica = if (!(dp \ "TipoRettifica").text.trim().equals(""))(dp \ "TipoRettifica").text else  null
				val motivazione =if((dp \ "Motivazione").text.trim().equals("")) null else (dp \ "Motivazione").text


				// SEZIONE DatiPod -> DatiPdp
				val sezDatiPdp=(dp \\ "DatiPdp")

        val area = if((sezDatiPdp \ "PuntoDispacciamento").text=="") "NEW_F1G_" +  cod_flusso else (sezDatiPdp \ "PuntoDispacciamento").text

        val tensioneVista = Try((sezDatiPdp \ "Tensione").text.replace(",", ".").toDouble).getOrElse(0D)
				val tensione = tensioneVista / 1000.0

				// <Tensione>132000</Tensione> / 1000 = V
				val perditatens = if (tensione == 0.0){
					0
				}else if(tensione.toInt < 1){
					perdita_1 // 0.104 // 10.4%
				}else if(tensione.toInt >= 1 && tensione.toInt <= 35){
					perdita_1_35 // 0.04 // 4%
				}else if(tensione.toInt <= 150){
					perdita_150 // 0.018 // 1.8%
				}else if(tensione.toInt <= 220){
					perdita_220 // 0.011 // 1.1
				}else if(tensione.toInt > 220){
					perdita_380 // 0.007
				}else {
					tensione
				}

				val trattamento_o = (sezDatiPdp \\ "Trattamento").text
				val potcontrimpl = Try((sezDatiPdp \ "PotContrImp").text.replace(",", ".").toDouble).getOrElse(0D)
				val potdisp = Try((sezDatiPdp \ "PotDisp").text.replace(",", ".").toDouble).getOrElse(0D)
				val cifreatt = Try(((sezDatiPdp \ "CifreAtt")text).toInt).getOrElse(0)
				val cifrerea = Try(((sezDatiPdp \ "CifreRea")text).toInt).getOrElse(0)

				// SEZIONE DatiPod -> Curva
        val sezCurvatmp=(dp \\ "Curva")
        val sezCurva= if( sezCurvatmp.length > 0)sezCurvatmp else (dp \\ "Misura")


				val tipodato = (sezCurva \ "TipoDato").text
				val tipodatoE = if(tipodato == "E"){ 1 } else { 0 }
				val tipodatoS = if(tipodato == "S"){ 1 } else { 0 }

				val raccolta = (sezCurva \ "Raccolta").text
				val validatoTmp = (sezCurva \ "Validato").text//).getOrElse("S")
        val potmax = if (!(sezCurva \ "PotMax").text.trim().equals("")) (sezCurva \ "PotMax").text.replace(",", ".").toDouble else null

        val validato = if(validatoTmp.trim().equals("")){
					"S"
				}else {
					validatoTmp
				}

				val coduc = "UC_" + codContrDisp + "_" + area


				val commonFields = List(codContrDisp, coduc, pod, pivautente, pivadistributore, area, anno.toInt, mese.toInt,  //dataTime,
					tipodatoE,tipodatoS,tensioneVista,trattamento_o,potcontrimpl,potdisp,cifreatt,cifrerea,raccolta,validato,potmax,
					perditatens,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp,motivazione,tipo_rettifica)

				val tipodatoA=0
				val ka=null
				val kr=null
				val kp=null

				val key_estensione_quarti=null

        if(sezCurva.length==0) {

          val eaValues =List.fill(100)(null)
          val erValues =List.fill(100)(null)

          val row_non_oraria=(commonFields ++ List(null,cod_flusso,key_estensione_quarti) ++ eaValues ++ erValues ++ List())
          Seq(Row.fromSeq(row_non_oraria))

        }else {
          val ea = (sezCurva \ "Ea")
          val itRes = getRowsEaEr(ea, commonFields, mese.toInt, 1, true, cod_flusso, key_estensione_quarti)

          val er = (sezCurva \ "Er")
          val irRes = getRowsEaEr(er, List(), mese.toInt, 1, false)

          val curva = for ((a, r) <- (itRes zip irRes)) yield Row.fromSeq(a ++ r)
          curva
        }




				//						 itRes
			}


			dpRes.toList

		} catch {
			case e: Exception => {
				e.printStackTrace()
        throw e
			}
		}
	}

	def letturaEValidazioni_vecchioFormatoAltriFlussi(rdd1: (Boolean,(String,String),String,Boolean), dataelaborazione:java.sql.Timestamp ,xml: scala.xml.Elem): List[(Row)] = {
		try {


			//val xml = rdd1._5
			val sezflusso=getParentSezXML(xml)
			val datiPod = (sezflusso \\ "DatiPod")

			val cod_flusso = (sezflusso \ "@CodFlusso").text
			val pivautente = (xml \\ "IdentificativiFlusso" \ "PIvaUtente") text
			val pivadistributore = (xml \\ "IdentificativiFlusso" \ "PIvaDistributore") text
			val codContrDisp = (xml \\ "IdentificativiFlusso" \ "CodContrDisp") text


			val dpIt = datiPod.theSeq

			val dpRes = dpIt.flatMap { dp =>

				//SEZIONE DatiPod
				val pod = (dp \\ "Pod").text
				val nomefile = rdd1._2._1
				val nomefileT = new File(nomefile).getName().toUpperCase()
				val sp = nomefile.split("/")
				val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt

				val timeStamp: Long = nomefile.substring(nomefile.lastIndexOf("/")).split("_")(4).toLong


				val cod_prat_att = if (!(dp \ "CodPratAtt").text.trim().equals("")) (dp \ "CodPratAtt").text else null

				val annoMese_F=nomefile.substring(nomefile.lastIndexOf("/")).split("_")(2)
				val anno_F = annoMese_F.take(4)
				val mese_F = annoMese_F.takeRight(2)

        val meseAnno_N = ((dp \ "MeseAnno").text).split("/")

				val meseanno_tmp = if (!(dp \ "MeseAnno").text.trim().equals("")) "00/"+(dp \ "MeseAnno").text else "00/"+mese_F +"/"+anno_F
				val data_misura = if (!(dp \ "DataMisura").text.trim().equals("")) (dp \ "DataMisura").text else null
				val data_inizio = if (!(dp \ "DataInizio").text.trim().equals("")) (dp \ "DataInizio").text else null
				var data_voltura = if ((dp \ "DataVoltura").text.trim().equals("")) null else (dp \ "DataVoltura").text
        val data_rilevazione=if((dp \ "DataRilevazione").text.trim().equals("")) null else (dp \ "DataRilevazione").text

				val tmp = if (data_misura != null) data_misura.split("/")else if (data_voltura != null) data_voltura.split("/")  else meseanno_tmp.split("/")

        val anno = if(meseAnno_N.length>=2) meseAnno_N(1).toInt else tmp(2).toInt
        val mese = if(meseAnno_N.length>=2) meseAnno_N(0).toInt else tmp(1).toInt

        if((cod_flusso.substring(0,3)=="VNO" || cod_flusso.substring(0,3)=="RNV") && data_voltura == null && data_misura!=null)
          data_voltura=data_misura

				val motivazione = if ((dp \ "Motivazione").text.trim().equals("")) null else (dp \ "Motivazione").text

				//SEZIONE DatiPod->DatiPdp

				val sezDatiPdp = (dp \\ "DatiPdp")

				val area = if (!(sezDatiPdp \ "PuntoDispacciamento").text.trim().equals("")) (sezDatiPdp \ "PuntoDispacciamento").text else "OLD_F_" + cod_flusso

				val coduc = "UC_" + codContrDisp + "_" + area

				val trattamento_o = if (!(sezDatiPdp \ "Trattamento").text.trim().equals("")) (sezDatiPdp \ "Trattamento").text else null
				val tensioneVista = Try(((sezDatiPdp \ "Tensione").text.replace(",", ".")).toDouble).getOrElse(0D)
				val tensione = tensioneVista / 1000.0

				// <Tensione>132000</Tensione> / 1000 = V
				val perditatens = if (tensione == 0.0){
					0
				}else if(tensione.toInt < 1){
					perdita_1 // 0.104 // 10.4%
				}else if(tensione.toInt >= 1 && tensione.toInt <= 35){
					perdita_1_35 // 0.04 // 4%
				}else if(tensione.toInt <= 150){
					perdita_150 // 0.018 // 1.8%
				}else if(tensione.toInt <= 220){
					perdita_220 // 0.011 // 1.1
				}else if(tensione.toInt > 220){
					perdita_380 // 0.007
				}else {
					tensione
				}

				val potcontrimpl = Try((sezDatiPdp \ "PotContrImp").text.replace(",", ".").toDouble).getOrElse(0D)
				val potimp = Try((sezDatiPdp \ "PotImp").text.replace(",", ".").toDouble).getOrElse(0D)
				val potdisp = Try((sezDatiPdp \ "PotDisp").text.replace(",", ".").toDouble).getOrElse(0D)
				val cifreatt = Try(((sezDatiPdp \ "CifreAtt")text).toInt).getOrElse(0)
				val cifrerea = Try(((sezDatiPdp \ "CifreRea")text).toInt).getOrElse(0)
				val cifrepot = Try(((sezDatiPdp \ "CifrePot")text).toInt).getOrElse(0)

				val cod_tariffa = if (!(sezDatiPdp \ "CodiceTariffa").text.trim().equals(""))(sezDatiPdp \ "CodiceTariffa").text else null
				val serv_tutela = if (!(sezDatiPdp \ "ServizioTutela").text.trim().equals(""))(sezDatiPdp \ "ServizioTutela").text else null
				val prestazioni = if (!(sezDatiPdp \ "Prestazioni").text.trim().equals(""))(sezDatiPdp \ "Prestazioni").text else null

				val ka = Try(((sezDatiPdp \ "Ka").text.replace(",", ".")).toDouble).getOrElse(1D)
				val kr = Try(((sezDatiPdp \ "Kr").text.replace(",", ".")).toDouble).getOrElse(1D)
				val kp = Try(((sezDatiPdp \ "Kp").text.replace(",", ".")).toDouble).getOrElse(1D)

				val matr_att = if (!(sezDatiPdp \ "MatrAtt").text.trim().equals(""))(sezDatiPdp \ "MatrAtt").text else null
				val matr_rea = if (!(sezDatiPdp \ "MatrRea").text.trim().equals(""))(sezDatiPdp \ "MatrRea").text else null
				val matr_pot = if (!(sezDatiPdp \ "MatrPot").text.trim().equals(""))(sezDatiPdp \ "MatrPot").text else null
				val datainst_mis_att = if (!(sezDatiPdp \ "DataInstMisAtt").text.trim().equals(""))(sezDatiPdp \ "DataInstMisAtt").text else null
				val datainst_mis_rea = if (!(sezDatiPdp \ "DataInstMisRea").text.trim().equals(""))(sezDatiPdp \ "DataInstMisRea").text else null
				val datainst_mis_pot = if (!(sezDatiPdp \ "DataInstMisPot").text.trim().equals(""))(sezDatiPdp \ "DataInstMisPot").text else null


				var gruppomis = (sezDatiPdp \ "GruppoMis").text
				if(gruppomis.trim().equals("")) gruppomis="SI"

				var forfait = (dp \\ "DatiPdp" \ "Forfait").text
				if(forfait.trim().equals("")) forfait="NO"

				val motivazione_stima = null
				val tipo_rettifica=null

				val data_prest = null
				val codprat_sii=null

				//SEZIONE DatiPod->Misura oppure DatiPod->Curva
				val sezMisura_tmp =(dp \\ "Misura")
				val sezCurva =(dp \\ "Curva")

				if( sezMisura_tmp.length > 0 || sezCurva.length >0 ){

					val sezMisura=if(sezMisura_tmp.length>0)sezMisura_tmp else sezCurva

					val raccolta = if (!(sezMisura\ "Raccolta").text.trim().equals("")) (sezMisura\ "Raccolta").text else null
					val tipodato = (sezMisura\ "TipoDato").text

					val tipodatoE = if(tipodato == "E"){ 1 } else { 0 }
					val tipodatoS = if(tipodato == "S"){ 1 } else { 0 }
					val tipodatoA = if(tipodato == "A"){ 1 } else { 0 }



					val validatoTmp = (sezMisura \ "Validato").text//).getOrElse("S")
					val potmax = if (!(sezMisura \ "PotMax").text.trim().equals("")) (sezMisura \ "PotMax").text.replace(",", ".").toDouble else null

					val validato = if(validatoTmp.trim().equals("")){
						"S"
					}else {
						validatoTmp
					}



					val eaf1 = Try((sezMisura \ "EaF1").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf2 = Try((sezMisura \ "EaF2").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf3 = Try((sezMisura \ "EaF3").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf4 = null
					val eaf5 = null
					val eaf6 = null

					val erf1 = Try((sezMisura \ "ErF1").text.replace(",", ".").toDouble).getOrElse(null)
					val erf2 = Try((sezMisura \ "ErF2").text.replace(",", ".").toDouble).getOrElse(null)
					val erf3 = Try((sezMisura \ "ErF3").text.replace(",", ".").toDouble).getOrElse(null)
					val erf4 = null
					val erf5 = null
					val erf6 = null

					val potf1 = Try((sezMisura \ "PotF1").text.replace(",", ".").toDouble).getOrElse(null)
					val potf2 = Try((sezMisura \ "PotF2").text.replace(",", ".").toDouble).getOrElse(null)
					val potf3 = Try((sezMisura \ "PotF3").text.replace(",", ".").toDouble).getOrElse(null)
					val potf4 = null
					val potf5 = null
					val potf6 = null

					val EaM = Try((sezMisura \ "EaM").text.replace(",", ".").toDouble).getOrElse(null)
					val ErM = Try((sezMisura \ "ErM").text.replace(",", ".").toDouble).getOrElse(null)
					val PotM = Try((sezMisura \ "PotM").text.replace(",", ".").toDouble).getOrElse(null)
					//fine campi nuovi

					val keyrow= cod_flusso + "_" + pivadistributore + "_"+pivautente +"_" +codContrDisp +"_" + pod + "_" + mese +"_"+ anno + "_" + trattamento_o

					val isnew_flusso =0
					val DataInizioPeriodo =null

					val commonFields = List(anno,mese,cod_flusso,pivadistributore,codContrDisp,area,
						isnew_flusso, coduc, pod, pivautente,data_misura,data_inizio,data_voltura, motivazione,trattamento_o,tensioneVista,perditatens,potcontrimpl,potimp,potdisp,cifreatt,
						cifrerea,cifrepot,cod_tariffa,serv_tutela,prestazioni,ka,kr,kp,matr_att,matr_rea,matr_pot, datainst_mis_att,datainst_mis_rea,datainst_mis_pot,gruppomis,
						forfait,raccolta,tipodatoE,tipodatoS,tipodatoA,validato,potmax,tipo_rettifica,data_rilevazione,data_prest,cod_prat_att,codprat_sii,motivazione_stima,
						DataInizioPeriodo,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp
					)


					val ea = (sezMisura \ "Ea")
					val er = (sezMisura \ "Er")

					val itRes = getRowsEaEr(ea, commonFields, mese, ka,true)
					val irRes = getRowsEaEr(er, List(eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM), mese, kr,false)

					val curva = if(itRes != Nil && irRes !=Nil) {
						for ((a, r) <- (itRes zip irRes)) yield Row.fromSeq(a ++ r)
					}else
					{
						val eaValues =List.fill(100)(null)
						val erValues =List.fill(100)(null)
						val row_non_oraria=(commonFields ++ List(null) ++ eaValues ++ erValues ++ List(eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM))
						Seq(Row.fromSeq(row_non_oraria))
					}
					curva

				}else{ 	//SEZIONE DatiPod->Consumo(Se Forfait = SI)

					val sezConsumo=(dp \\ "Consumo")
					//nuovo schema

					val keyrow= cod_flusso + "_" +  pivadistributore + "_"+pivautente +"_" +codContrDisp +"_" + pod + "_" + mese +"_"+ anno + "_" + trattamento_o

					val isnew_flusso =0
					val DataInizioPeriodo = (sezConsumo \ "DataInizioPeriodo").text

					val raccolta = if (!(sezConsumo\ "Raccolta").text.trim().equals("")) (sezConsumo\ "Raccolta").text else null
					val tipodato = (sezConsumo\ "TipoDato").text

					val tipodatoE = if(tipodato == "E"){ 1 } else { 0 }
					val tipodatoS = if(tipodato == "S"){ 1 } else { 0 }
					val tipodatoA = if(tipodato == "A"){ 1 } else { 0 }

					val validatoTmp = (sezConsumo \ "Validato").text//).getOrElse("S")
					val potmax = if (!(sezConsumo \ "PotMax").text.trim().equals("")) (sezConsumo \ "PotMax").text.replace(",", ".").toDouble else null

					val validato = if(validatoTmp.trim().equals("")){
						"S"
					}else {
						validatoTmp
					}



					val commonFields = List(anno,mese,cod_flusso,pivadistributore,codContrDisp,area,
						isnew_flusso, coduc, pod, pivautente,data_misura,data_inizio,data_voltura, motivazione,trattamento_o,tensioneVista,perditatens,potcontrimpl,potimp,potdisp,cifreatt,
						cifrerea,cifrepot,cod_tariffa,serv_tutela,prestazioni,ka,kr,kp,matr_att,matr_rea,matr_pot, datainst_mis_att,datainst_mis_rea,datainst_mis_pot,gruppomis,
						forfait,raccolta,tipodatoE,tipodatoS,tipodatoA,validato,potmax,tipo_rettifica,data_rilevazione,data_prest,cod_prat_att,codprat_sii,motivazione_stima,
						DataInizioPeriodo,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp
					)

					val eaf1 = Try((sezConsumo \ "EaF1").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf2 = Try((sezConsumo \ "EaF2").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf3 = Try((sezConsumo \ "EaF3").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf4 = null
					val eaf5 = null
					val eaf6 = null

					val erf1 = Try((sezConsumo \ "ErF1").text.replace(",", ".").toDouble).getOrElse(null)
					val erf2 = Try((sezConsumo \ "ErF2").text.replace(",", ".").toDouble).getOrElse(null)
					val erf3 = Try((sezConsumo \ "ErF3").text.replace(",", ".").toDouble).getOrElse(null)
					val erf4 = null
					val erf5 = null
					val erf6 = null

					val potf1 = Try((sezConsumo \ "PotF1").text.replace(",", ".").toDouble).getOrElse(null)
					val potf2 = Try((sezConsumo \ "PotF2").text.replace(",", ".").toDouble).getOrElse(null)
					val potf3 = Try((sezConsumo \ "PotF3").text.replace(",", ".").toDouble).getOrElse(null)
					val potf4 = null
					val potf5 = null
					val potf6 = null

					val EaM = Try((sezConsumo \ "EaM").text.replace(",", ".").toDouble).getOrElse(null)
					val ErM = Try((sezConsumo \ "ErM").text.replace(",", ".").toDouble).getOrElse(null)
					val PotM = Try((sezConsumo \ "PotM").text.replace(",", ".").toDouble).getOrElse(null)

					val eaValues =List.fill(100)(null)
					val erValues =List.fill(100)(null)

					val row_non_oraria=(commonFields ++ List(null) ++ eaValues ++ erValues ++ List(eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM))

					val curva_non_oraria=Seq(Row.fromSeq(row_non_oraria))

					curva_non_oraria

				}

			}

			dpRes.toList



		} catch {
			case e: Exception => {
				e.printStackTrace()
        throw e
			}
		}
	}

	def letturaEValidazioni_NuovoFlusso(rdd1: (Boolean,(String,String),String,Boolean), dataelaborazione:java.sql.Timestamp ,xml:scala.xml.Elem): List[(Row)] = {
		try {

			/*val noBom = if(rdd1._2._2.startsWith(UTF8_BOM)){
        rdd1._2._2.substring(1)
      }else{
        rdd1._2._2
      }
        val xml = XML.loadString(noBom)*/
			//val xml = rdd1._5
			//val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())
			val sezflusso=getParentSezXML(xml)
			val datiPod = (sezflusso \\ "DatiPod")

			val cod_flusso = (sezflusso \ "@CodFlusso").text
			val pivautente = (xml \\ "IdentificativiFlusso" \ "PIvaUtente") text
			val pivadistributore = (xml \\ "IdentificativiFlusso" \ "PIvaDistributore") text
			val codContrDisp = (xml \\ "IdentificativiFlusso" \ "CodContrDisp") text


			val dpIt = datiPod.theSeq
			var tmp_index=0

			val dpRes = dpIt.flatMap { dp =>

				tmp_index =tmp_index+1
				//SEZIONE DatiPod
				val pod = (dp \\ "Pod").text
				val nomefile = rdd1._2._1
				val nomefileT =  new File( nomefile ).getName().toUpperCase()
				val sp = nomefile.split("/")
				val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt

				val timeStamp:Long = nomefile.substring(nomefile.lastIndexOf("/")).split("_")(4).toLong

				val annoMese_F=nomefile.substring(nomefile.lastIndexOf("/")).split("_")(2)

        val anno_F = annoMese_F.take(4)
        val mese_F = annoMese_F.takeRight(2)

        val meseAnno_N = ((dp \ "MeseAnno").text).split("/")


        val meseanno_tmp = if (!(dp \ "MeseAnno").text.trim().equals("")) "00/"+(dp \ "MeseAnno").text else "00/"+mese_F +"/"+anno_F
				val area = "NEW_F_" +  cod_flusso


        val data_misura = if (!(dp \ "DataMisura").text.trim().equals("")) (dp \ "DataMisura").text else null
        val data_inizio = if (!(dp \ "DataInizio").text.trim().equals("")) (dp \ "DataInizio").text else null
        val data_prest = if (!(dp \ "DataPrest").text.trim().equals(""))(dp \ "DataPrest").text else null


        val tmp = if (data_misura != null) data_misura.split("/")else if (data_prest != null) data_prest.split("/")else meseanno_tmp.split("/")
        val anno_N = if(meseAnno_N.length>=2) meseAnno_N(1).toInt else tmp(2).toInt
        val mese_N = if(meseAnno_N.length>=2) meseAnno_N(0).toInt else tmp(1).toInt

        val data_rilevazione=if((dp \ "DataRilevazione").text.trim().equals("")) null else (dp \ "DataRilevazione").text
        var data_voltura = if ((dp \ "DataVoltura").text.trim().equals("")) null else (dp \ "DataVoltura").text


        if((cod_flusso.substring(0,3)=="VNO" || cod_flusso.substring(0,3)=="RNV") && data_voltura == null && (data_prest!=null || data_misura != null)) {
          if (data_prest != null)
            data_voltura = data_prest
          else
            data_voltura = data_misura
        }

				//nuovo campo solo per rfo
				val tipo_rettifica = if (!(dp \ "TipoRettifica").text.trim().equals(""))(dp \ "TipoRettifica").text else  null


				//nuovo campo solo per rfo
				val motivazione_rett =if((dp \ "Motivazione").text.trim().equals("")) null else (dp \ "Motivazione").text

				//nuovo campo
				//val data_prest = if(raccolta.trim().equals("S") || raccolta.trim().equals("V") || raccolta.trim().equals("T")) {


				//nuovo campo
				//val codprat_sii=if(raccolta.trim().equals("S") || raccolta.trim().equals("V")) {
				val codprat_sii=if (!(dp \ "CodPrat_SII").text.trim().equals(""))(dp \\ "CodPrat_SII").text else null

        val cod_prat_att = if (!(dp \ "CodPratAtt").text.trim().equals("")) (dp \ "CodPratAtt").text else null

				val coduc = "UC_" + codContrDisp + "_" + area

				//SEZIONE DatiPod->DatiPdp

				val sezDatiPdp =(dp \\ "DatiPdp")

				val trattamento_o = (sezDatiPdp \ "Trattamento").text
				val tensioneVista = Try((sezDatiPdp \ "Tensione").text.replace(",", ".").toDouble).getOrElse(0D)
				val tensione = tensioneVista / 1000.0

				// <Tensione>132000</Tensione> / 1000 = V
				val perditatens = if (tensione == 0.0){
					0
				}else if(tensione.toInt < 1){
					perdita_1 // 0.104 // 10.4%
				}else if(tensione.toInt >= 1 && tensione.toInt <= 35){
					perdita_1_35 // 0.04 // 4%
				}else if(tensione.toInt <= 150){
					perdita_150 // 0.018 // 1.8%
				}else if(tensione.toInt <= 220){
					perdita_220 // 0.011 // 1.1
				}else if(tensione.toInt > 220){
					perdita_380 // 0.007
				}else {
					tensione
				}

				//nuovi campi(ka,kr,kp,gruppomis,forfait)
				val ka = Try(((sezDatiPdp \ "Ka").text.replace(",", ".")).toDouble).getOrElse(1D)
				val kr = Try(((sezDatiPdp \ "Kr").text.replace(",", ".")).toDouble).getOrElse(1D)
				val kp = Try(((sezDatiPdp \ "Kp").text.replace(",", ".")).toDouble).getOrElse(1D)

				var gruppomis = (sezDatiPdp \ "GruppoMis").text
				if(gruppomis.trim().equals("")) gruppomis="SI"

				var forfait = (sezDatiPdp \ "Forfait").text
				if(forfait.trim().equals("")) forfait="NO"


        val cifrepot = Try(((sezDatiPdp \ "CifrePot")text).toInt).getOrElse(0)
        val cifreatt = Try(((sezDatiPdp \ "CifreAtt")text).toInt).getOrElse(0)
        val cod_tariffa = if (!(sezDatiPdp \ "CodiceTariffa").text.trim().equals(""))(sezDatiPdp \ "CodiceTariffa").text else null
        val serv_tutela = if (!(sezDatiPdp \ "ServizioTutela").text.trim().equals(""))(sezDatiPdp \ "ServizioTutela").text else null
        val prestazioni = if (!(sezDatiPdp \ "Prestazioni").text.trim().equals(""))(sezDatiPdp \ "Prestazioni").text else null

        val matr_att = if (!(sezDatiPdp \ "MatrAtt").text.trim().equals(""))(sezDatiPdp \ "MatrAtt").text else null
        val matr_rea = if (!(sezDatiPdp \ "MatrRea").text.trim().equals(""))(sezDatiPdp \ "MatrRea").text else null
        val matr_pot = if (!(sezDatiPdp \ "MatrPot").text.trim().equals(""))(sezDatiPdp \ "MatrPot").text else null
        val datainst_mis_att = if (!(sezDatiPdp \ "DataInstMisAtt").text.trim().equals(""))(sezDatiPdp \ "DataInstMisAtt").text else null
        val datainst_mis_rea = if (!(sezDatiPdp \ "DataInstMisRea").text.trim().equals(""))(sezDatiPdp \ "DataInstMisRea").text else null
        val datainst_mis_pot = if (!(sezDatiPdp \ "DataInstMisPot").text.trim().equals(""))(sezDatiPdp \ "DataInstMisPot").text else null

        val potcontrimpl = Try((sezDatiPdp \ "PotContrImp").text.replace(",", ".").toDouble).getOrElse(0D)
        val potimp = Try((sezDatiPdp \ "PotImp").text.replace(",", ".").toDouble).getOrElse(0D)
        val potdisp = Try((sezDatiPdp \ "PotDisp").text.replace(",", ".").toDouble).getOrElse(0D)

				//SEZIONE DatiPod->Misura
				val sezMisura =(dp \\ "Misura")

				//val tmp_key=System.currentTimeMillis().toString()

				if( sezMisura.length > 0){

					val raccolta = if (!(sezMisura\ "Raccolta").text.trim().equals("")) (sezMisura\ "Raccolta").text else null
					val tipodato = (sezMisura\ "TipoDato").text

					val tipodatoE = if(tipodato == "E"){ 1 } else { 0 }
					val tipodatoS = if(tipodato == "S"){ 1 } else { 0 }
					//nuovo campo
					val tipodatoA = if(tipodato == "A"){ 1 } else { 0 }
					//nuovo campo
					val motivazione_stima =if (!(sezMisura \ "MotivazioneStima").text.trim().equals("")) (sezMisura\ "MotivazioneStima").text else null

					val validatoTmp = (sezMisura \ "Validato").text//).getOrElse("S")
					val potmax = if (!(sezMisura \ "PotMax").text.trim().equals("")) (sezMisura \ "PotMax").text.replace(",", ".").toDouble else null

					val validato = if(validatoTmp.trim().equals("")){
						"S"
					}else {
						validatoTmp
					}


					//nuovi campi
					val eaf1 = Try((sezMisura \ "EaF1").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf2 = Try((sezMisura \ "EaF2").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf3 = Try((sezMisura \ "EaF3").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf4 = Try((sezMisura \ "EaF4").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf5 = Try((sezMisura \ "EaF5").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf6 = Try((sezMisura \ "EaF6").text.replace(",", ".").toDouble).getOrElse(null)

					val erf1 = Try((sezMisura \ "ErF1").text.replace(",", ".").toDouble).getOrElse(null)
					val erf2 = Try((sezMisura \ "ErF2").text.replace(",", ".").toDouble).getOrElse(null)
					val erf3 = Try((sezMisura \ "ErF3").text.replace(",", ".").toDouble).getOrElse(null)
					val erf4 = Try((sezMisura \ "ErF4").text.replace(",", ".").toDouble).getOrElse(null)
					val erf5 = Try((sezMisura \ "ErF5").text.replace(",", ".").toDouble).getOrElse(null)
					val erf6 = Try((sezMisura \ "ErF6").text.replace(",", ".").toDouble).getOrElse(null)

					val potf1 = Try((sezMisura \ "PotF1").text.replace(",", ".").toDouble).getOrElse(null)
					val potf2 = Try((sezMisura \ "PotF2").text.replace(",", ".").toDouble).getOrElse(null)
					val potf3 = Try((sezMisura \ "PotF3").text.replace(",", ".").toDouble).getOrElse(null)
					val potf4 = Try((sezMisura \ "PotF4").text.replace(",", ".").toDouble).getOrElse(null)
					val potf5 = Try((sezMisura \ "PotF5").text.replace(",", ".").toDouble).getOrElse(null)
					val potf6 = Try((sezMisura \ "PotF6").text.replace(",", ".").toDouble).getOrElse(null)

					val EaM = Try((sezMisura \ "EaM").text.replace(",", ".").toDouble).getOrElse(null)
					val ErM = Try((sezMisura \ "ErM").text.replace(",", ".").toDouble).getOrElse(null)
					val PotM = Try((sezMisura \ "PotM").text.replace(",", ".").toDouble).getOrElse(null)
					//fine campi nuovi



					val cifrerea=tmp_index
					val DataInizioPeriodo =null

					val curva = if(cod_flusso.trim().contains("PDO") || cod_flusso.trim().contains("RFO")) {
						//(schema schemaQuarti)


						val commonFields = List(codContrDisp, coduc, pod, pivautente, pivadistributore, area, anno_N, mese_N, //dataTime,
							tipodatoE, tipodatoS, tensioneVista, trattamento_o, potcontrimpl, potdisp, cifreatt, cifrerea, raccolta, validato,
							potmax, perditatens, nomefileT, annoMeseGiornoDir, dataelaborazione, timeStamp,motivazione_rett,tipo_rettifica
						)

						val key_estensione_quarti=anno_N.toString+mese_N.toString+cifrerea.toString+dataelaborazione.toString+pod+nomefileT

						val ea = (sezMisura \ "Ea")
						val er = (sezMisura \ "Er")

						val itRes = getRowsEaEr(ea, commonFields, mese_N, ka, true,cod_flusso,key_estensione_quarti)

						val irRes = getRowsEaEr(er, List(), mese_N, kr, false)


						for ((a, r) <- (itRes zip irRes)) yield Row.fromSeq(a ++ r)
					}else //tracciato per flussi no PDO ed RFO (schema schemaflusso_noaggr)
					{
						val isnew_flusso=1


						/*val cifrepot=null
						val potimp=null
						val cod_tariffa =null
						val serv_tutela =null
						val prestazioni=null
						val codprat_att=null
						val matr_att=null
						val matr_rea=null
						val matr_pot=null
						val data_inst_misatt=null
						val data_inst_misrea=null
						val data_inst_mispot=null*/



						val commonFields = List(anno_N,mese_N,cod_flusso,pivadistributore,codContrDisp,area,
							isnew_flusso, coduc, pod, pivautente,data_misura,data_inizio,data_voltura, motivazione_rett,trattamento_o,tensioneVista,perditatens,potcontrimpl,potimp,potdisp,cifreatt,
							cifrerea,cifrepot,cod_tariffa,serv_tutela,prestazioni,ka,kr,kp,matr_att,matr_rea,matr_pot,datainst_mis_att,datainst_mis_rea,datainst_mis_pot,gruppomis,
							forfait,raccolta,tipodatoE,tipodatoS,tipodatoA,validato,potmax,tipo_rettifica,data_rilevazione,data_prest,cod_prat_att,codprat_sii,motivazione_stima,
							DataInizioPeriodo,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp
						)

						val ea = (sezMisura \ "Ea")
						val er = (sezMisura \ "Er")
						val itRes = getRowsEaEr(ea, commonFields, mese_N, ka,true)
						val irRes = getRowsEaEr(er, List(eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM), mese_N, kr,false)

						if(itRes != Nil && irRes !=Nil) {
							for ((a, r) <- (itRes zip irRes)) yield Row.fromSeq(a ++ r)
						}else {
							//caso per i flussi senza curva
							val eaValues =List.fill(100)(null)
							val erValues =List.fill(100)(null)

							val row_non_oraria=(commonFields ++ List(null) ++ eaValues ++ erValues ++ List(eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM))
							Seq(Row.fromSeq(row_non_oraria))
						}
					}

					val new_data= if(cod_flusso.trim().contains("PDO") || cod_flusso.trim().contains("RFO")) {

						val key_estensione_quarti=anno_N.toString+mese_N.toString+tmp_index.toString+dataelaborazione.toString+pod+nomefileT

						val newTableFields = List(Row(codContrDisp, pod, pivautente, pivadistributore, area, anno_N, mese_N, nomefileT, annoMeseGiornoDir, dataelaborazione, timeStamp,
							cod_flusso, tipodatoA, ka, kr, kp, data_misura, tipo_rettifica, data_rilevazione, motivazione_rett, data_prest, codprat_sii, gruppomis, forfait, motivazione_stima,
							DataInizioPeriodo, eaf1, eaf2, eaf3, eaf4, eaf5, eaf6, erf1, erf2, erf3, erf4, erf5, erf6, potf1, potf2, potf3, potf4, potf5, potf6, EaM, ErM, PotM,tmp_index,key_estensione_quarti))

						newTableFields
					}
					else {
						Nil
					}

					curva ++ new_data

				}else{ 	//SEZIONE DatiPod->Consumo(Se Forfait = SI)
					val sezConsumo=(dp \\ "Consumo")
					//nuovo schema


					val validatoTmp = (sezConsumo \ "Validato").text//).getOrElse("S")
					val validato = if(validatoTmp.trim().equals("")){
						"S"
					}else {
						validatoTmp
					}

					val tipodatoE = 0
					val tipodatoS =0
					val tipodatoA =0
					/*val potcontrimpl=null
					val potdisp =null*/
					val potmax =null
					//val cifreatt=null
					val cifrerea=tmp_index
					val raccolta = if (!(sezConsumo\ "Raccolta").text.trim().equals("")) (sezConsumo\ "Raccolta").text else null
					val motivazione_stima =if (!(sezConsumo \ "MotivazioneStima").text.trim().equals("")) (sezConsumo\ "MotivazioneStima").text else null

					val DataInizioPeriodo = (sezConsumo \ "DataInizioPeriodo").text
					val eaf1 = Try((sezConsumo \ "EaF1").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf2 = Try((sezConsumo \ "EaF2").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf3 = Try((sezConsumo \ "EaF3").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf4 = Try((sezConsumo \ "EaF4").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf5 = Try((sezConsumo \ "EaF5").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf6 = Try((sezConsumo \ "EaF6").text.replace(",", ".").toDouble).getOrElse(null)

					val erf1 = Try((sezConsumo \ "ErF1").text.replace(",", ".").toDouble).getOrElse(null)
					val erf2 = Try((sezConsumo \ "ErF2").text.replace(",", ".").toDouble).getOrElse(null)
					val erf3 = Try((sezConsumo \ "ErF3").text.replace(",", ".").toDouble).getOrElse(null)
					val erf4 = Try((sezConsumo \ "ErF4").text.replace(",", ".").toDouble).getOrElse(null)
					val erf5 = Try((sezConsumo \ "ErF5").text.replace(",", ".").toDouble).getOrElse(null)
					val erf6 = Try((sezConsumo \ "ErF6").text.replace(",", ".").toDouble).getOrElse(null)

					val potf1 = Try((sezConsumo \ "PotF1").text.replace(",", ".").toDouble).getOrElse(null)
					val potf2 = Try((sezConsumo \ "PotF2").text.replace(",", ".").toDouble).getOrElse(null)
					val potf3 = Try((sezConsumo \ "PotF3").text.replace(",", ".").toDouble).getOrElse(null)
					val potf4 = Try((sezConsumo \ "PotF4").text.replace(",", ".").toDouble).getOrElse(null)
					val potf5 = Try((sezConsumo \ "PotF5").text.replace(",", ".").toDouble).getOrElse(null)
					val potf6 = Try((sezConsumo \ "PotF6").text.replace(",", ".").toDouble).getOrElse(null)

					val EaM = Try((sezConsumo \ "EaM").text.replace(",", ".").toDouble).getOrElse(null)
					val ErM = Try((sezConsumo \ "ErM").text.replace(",", ".").toDouble).getOrElse(null)
					val PotM = Try((sezConsumo \ "PotM").text.replace(",", ".").toDouble).getOrElse(null)

					val curva_a_consumo = if(cod_flusso.trim().contains("PDO") || cod_flusso.trim().contains("RFO")) {
						//(schema schemaQuarti)

						val commonFields = List(codContrDisp, coduc, pod, pivautente, pivadistributore, area, anno_N, mese_N, //dataTime,
							tipodatoE, tipodatoS, tensioneVista, trattamento_o, potcontrimpl, potdisp, cifreatt, cifrerea, raccolta,
							validato, potmax, perditatens, nomefileT, annoMeseGiornoDir, dataelaborazione, timeStamp,motivazione_rett,tipo_rettifica
						)
						val key_estensione_quarti=anno_N.toString+mese_N.toString+cifrerea.toString+dataelaborazione.toString+pod+nomefileT

						val eaValues =List.fill(100)(null)
						val erValues =List.fill(100)(null)

						val row_non_oraria=(commonFields ++ List(null,cod_flusso,key_estensione_quarti) ++ eaValues ++ erValues ++ List())
						Seq(Row.fromSeq(row_non_oraria))

					}else //tracciato per flussi no PDO ed RFO (schema schemaflusso_noaggr)
					{
						val isnew_flusso=1


						/*val cifrepot=null
						val potimp=null
						val cod_tariffa =null
						val serv_tutela =null
						val prestazioni=null
						val codprat_att=null
						val matr_att=null
						val matr_rea=null
						val matr_pot=null
						val data_inst_misatt=null
						val data_inst_misrea=null
						val data_inst_mispot=null*/

						val commonFields = List(anno_N,mese_N,cod_flusso,pivadistributore,codContrDisp,area,
							isnew_flusso, coduc, pod, pivautente,data_misura,data_inizio,data_voltura, motivazione_rett,trattamento_o,tensioneVista,perditatens,potcontrimpl,potimp,potdisp,cifreatt,
							cifrerea,cifrepot,cod_tariffa,serv_tutela,prestazioni,ka,kr,kp,matr_att,matr_rea,matr_pot,datainst_mis_att,datainst_mis_rea,datainst_mis_pot,gruppomis,
							forfait,raccolta,tipodatoE,tipodatoS,tipodatoA,validato,potmax,tipo_rettifica,data_rilevazione,data_prest,cod_prat_att,codprat_sii,motivazione_stima,
							DataInizioPeriodo,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp
						)

						val ea = (sezMisura \ "Ea")
						val er = (sezMisura \ "Er")
						val itRes = getRowsEaEr(ea, commonFields, mese_N, ka,true)
						val irRes = getRowsEaEr(er, List(eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM), mese_N, kr,false)

						if(itRes != Nil && irRes !=Nil) {
							for ((a, r) <- (itRes zip irRes)) yield Row.fromSeq(a ++ r)
						}else {
							//caso per i flussi senza curva
							val eaValues =List.fill(100)(null)
							val erValues =List.fill(100)(null)

							val row_non_oraria=(commonFields ++ List(null) ++ eaValues ++ erValues ++ List(eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6,EaM,ErM,PotM))
							Seq(Row.fromSeq(row_non_oraria))
						}
					}


					val new_data= if(cod_flusso.trim().contains("PDO") || cod_flusso.trim().contains("RFO")) {

						val key_estensione_quarti=anno_N.toString+mese_N.toString+tmp_index.toString+dataelaborazione.toString+pod+nomefileT


						val newTableFields = List(Row(codContrDisp, pod, pivautente, pivadistributore, area, anno_N, mese_N, nomefileT, annoMeseGiornoDir, dataelaborazione, timeStamp,
							cod_flusso, tipodatoA, ka, kr, kp, data_misura, tipo_rettifica, data_rilevazione, motivazione_rett, data_prest, codprat_sii, gruppomis, forfait, motivazione_stima,
							DataInizioPeriodo, eaf1, eaf2, eaf3, eaf4, eaf5, eaf6, erf1, erf2, erf3, erf4, erf5, erf6, potf1, potf2, potf3, potf4, potf5, potf6, EaM, ErM, PotM,tmp_index,key_estensione_quarti))


						newTableFields
					}
					else {
						Nil
					}


					//	curva_non_oraria

					curva_a_consumo ++ new_data


				}
				//						 itRes
			}
			dpRes.toList



		} catch {
			case e: Exception => {
				e.printStackTrace()
        throw e
			}
		}
	}

	def letturaEValidazioni_FlussoSMIS(rdd1: (Boolean,(String,String),String,Boolean), dataelaborazione:java.sql.Timestamp,xml:scala.xml.Elem ): List[(Row)] = {


		try {

			//val xml = rdd1._5
			val sezflusso=getParentSezXML(xml)
			val datiPod = (sezflusso \\ "DatiPod")


			val pivadistributore = (xml \\ "IdentificativiFlusso" \ "PIvaDistributore") text
			val pivautente = (xml \\ "IdentificativiFlusso" \ "PIvaUtente") text
			val codContrDisp = (xml \\ "IdentificativiFlusso" \ "CodContrDisp") text

			val dpIt = datiPod.theSeq



			val dpRes = dpIt.flatMap { dp =>

				val pod = (dp \ "Pod").text
				val motivazione = (dp \ "Motivazione").text

				val nomefile = rdd1._2._1
				val nomefileT =  new File( nomefile ).getName().toUpperCase()
				val sp = nomefile.split("/")
				val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt

				val timeStamp:Long = nomefile.substring(nomefile.lastIndexOf("/")).split("_")(4).toLong

				val sezDatiSmontaggio=(dp \\ "Smontaggio")
				val sezDatiMontaggio=(dp \\ "Montaggio")

				var anno=0
				var mese=0

				// SEZIONE DatiPod -> Smontaggio
				val fieldsSmontaggio=if(sezDatiSmontaggio.length >0){

					val tipo_misuratore_sm=(sezDatiSmontaggio \ "TipoMisuratore").text
					val data_misura_sm=(sezDatiSmontaggio \ "DataMisura").text
					val tmp =data_misura_sm.split("/")
					anno = tmp(2).toInt
					mese = tmp(1).toInt

					val tipo_dato=(sezDatiSmontaggio \ "TipoDato").text

					val eaf1 = Try((sezDatiSmontaggio \ "EaF1").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf2 = Try((sezDatiSmontaggio \ "EaF2").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf3 = Try((sezDatiSmontaggio \ "EaF3").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf4 = Try((sezDatiSmontaggio \ "EaF4").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf5 = Try((sezDatiSmontaggio \ "EaF5").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf6 = Try((sezDatiSmontaggio \ "EaF6").text.replace(",", ".").toDouble).getOrElse(null)

					val erf1 = Try((sezDatiSmontaggio \ "ErF1").text.replace(",", ".").toDouble).getOrElse(null)
					val erf2 = Try((sezDatiSmontaggio \ "ErF2").text.replace(",", ".").toDouble).getOrElse(null)
					val erf3 = Try((sezDatiSmontaggio \ "ErF3").text.replace(",", ".").toDouble).getOrElse(null)
					val erf4 = Try((sezDatiSmontaggio \ "ErF4").text.replace(",", ".").toDouble).getOrElse(null)
					val erf5 = Try((sezDatiSmontaggio \ "ErF5").text.replace(",", ".").toDouble).getOrElse(null)
					val erf6 = Try((sezDatiSmontaggio \ "ErF6").text.replace(",", ".").toDouble).getOrElse(null)

					val potf1 = Try((sezDatiSmontaggio \ "PotF1").text.replace(",", ".").toDouble).getOrElse(null)
					val potf2 = Try((sezDatiSmontaggio \ "PotF2").text.replace(",", ".").toDouble).getOrElse(null)
					val potf3 = Try((sezDatiSmontaggio \ "PotF3").text.replace(",", ".").toDouble).getOrElse(null)
					val potf4 = Try((sezDatiSmontaggio \ "PotF4").text.replace(",", ".").toDouble).getOrElse(null)
					val potf5 = Try((sezDatiSmontaggio \ "PotF5").text.replace(",", ".").toDouble).getOrElse(null)
					val potf6 = Try((sezDatiSmontaggio \ "PotF6").text.replace(",", ".").toDouble).getOrElse(null)

					val EaM = Try((sezDatiSmontaggio \ "EaM").text.replace(",", ".").toDouble).getOrElse(null)
					val ErM = Try((sezDatiSmontaggio \ "ErM").text.replace(",", ".").toDouble).getOrElse(null)
					val PotM = Try((sezDatiSmontaggio \ "PotM").text.replace(",", ".").toDouble).getOrElse(null)

					List(tipo_misuratore_sm,data_misura_sm,tipo_dato,
						eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,
						erf1,erf2,erf3,erf4,erf5,erf6,
						potf1,potf2,potf3,potf4,potf5,potf6,
						EaM,ErM,PotM)

				}else{
					List(null,null,null,
						null,null,null,null,null,null,
						null,null,null,null,null,null,
						null,null,null,null,null,null,
						null,null,null)
				}


				val fieldsMontaggio=if(sezDatiMontaggio.length >0){

					val tipo_misuratore_mn=(sezDatiMontaggio \ "TipoMisuratore").text
					val data_misura_mn=(sezDatiMontaggio \ "DataMisura").text
					val tmp =data_misura_mn.split("/")
					anno = tmp(2).toInt
					mese = tmp(1).toInt

					val data_messa_regime_mn=(sezDatiMontaggio \ "DataMessaRegime2G").text
					val tensione_vista=Try((sezDatiMontaggio \ "Tensione").text.replace(",",".").toDouble).getOrElse(0D)
					val tensione_mn = tensione_vista / 1000.0

					// <Tensione>132000</Tensione> / 1000 = V
					val perditatens_mn = if (tensione_mn == 0.0){
						0
					}else if(tensione_mn.toInt < 1){
						perdita_1 // 0.104 // 10.4%
					}else if(tensione_mn .toInt>= 1 && tensione_mn.toInt <= 35){
						perdita_1_35 // 0.04 // 4%
					}else if(tensione_mn.toInt <= 150){
						perdita_150 // 0.018 // 1.8%
					}else if(tensione_mn.toInt == 220){
						perdita_220 // 0.011 // 1.1
					}else if(tensione_mn.toInt > 220){
						perdita_380 // 0.007
					}else {
						tensione_mn
					}

					val ka_mn = Try(((sezDatiMontaggio \ "Ka").text.replace(",", ".")).toDouble).getOrElse(1D)
					val kr_mn = Try(((sezDatiMontaggio \ "Kr").text.replace(",", ".")).toDouble).getOrElse(1D)
					val kp_mn = Try(((sezDatiMontaggio \ "Kp").text.replace(",", ".")).toDouble).getOrElse(1D)

					val matr_att_mn=(sezDatiMontaggio \ "MatrAtt").text
					val matr_rea_mn=(sezDatiMontaggio \ "MatrRea").text
					val matr_pot_mn=(sezDatiMontaggio \ "MatrPot").text
					val cifre_att_mn= Try(((sezDatiMontaggio \ "CifreAtt")text).toDouble).getOrElse(0D)
					val cifre_rea_mn=Try(((sezDatiMontaggio \ "CifreRea")text).toDouble).getOrElse(0D)
					val cifre_pot_mn=Try(((sezDatiMontaggio \ "CifrePot")text).toDouble).getOrElse(0D)

					val eaf1_mn = Try((sezDatiMontaggio \ "EaF1").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf2_mn = Try((sezDatiMontaggio \ "EaF2").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf3_mn = Try((sezDatiMontaggio \ "EaF3").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf4_mn = Try((sezDatiMontaggio \ "EaF4").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf5_mn = Try((sezDatiMontaggio \ "EaF5").text.replace(",", ".").toDouble).getOrElse(null)
					val eaf6_mn = Try((sezDatiMontaggio \ "EaF6").text.replace(",", ".").toDouble).getOrElse(null)

					val erf1_mn = Try((sezDatiMontaggio \ "ErF1").text.replace(",", ".").toDouble).getOrElse(null)
					val erf2_mn = Try((sezDatiMontaggio \ "ErF2").text.replace(",", ".").toDouble).getOrElse(null)
					val erf3_mn = Try((sezDatiMontaggio \ "ErF3").text.replace(",", ".").toDouble).getOrElse(null)
					val erf4_mn = Try((sezDatiMontaggio \ "ErF4").text.replace(",", ".").toDouble).getOrElse(null)
					val erf5_mn = Try((sezDatiMontaggio \ "ErF5").text.replace(",", ".").toDouble).getOrElse(null)
					val erf6_mn = Try((sezDatiMontaggio \ "ErF6").text.replace(",", ".").toDouble).getOrElse(null)

					val potf1_mn = Try((sezDatiMontaggio \ "PotF1").text.replace(",", ".").toDouble).getOrElse(null)
					val potf2_mn = Try((sezDatiMontaggio \ "PotF2").text.replace(",", ".").toDouble).getOrElse(null)
					val potf3_mn = Try((sezDatiMontaggio \ "PotF3").text.replace(",", ".").toDouble).getOrElse(null)
					val potf4_mn = Try((sezDatiMontaggio \ "PotF4").text.replace(",", ".").toDouble).getOrElse(null)
					val potf5_mn = Try((sezDatiMontaggio \ "PotF5").text.replace(",", ".").toDouble).getOrElse(null)
					val potf6_mn = Try((sezDatiMontaggio \ "PotF6").text.replace(",", ".").toDouble).getOrElse(null)

					val EaM_mn = Try((sezDatiMontaggio \ "EaM").text.replace(",", ".").toDouble).getOrElse(null)
					val ErM_mn = Try((sezDatiMontaggio \ "ErM").text.replace(",", ".").toDouble).getOrElse(null)
					val PotM_mn = Try((sezDatiMontaggio \ "PotM").text.replace(",", ".").toDouble).getOrElse(null)

					List(tipo_misuratore_mn,data_misura_mn,data_messa_regime_mn,
						tensione_vista,perditatens_mn,ka_mn,kr_mn,kp_mn,
						matr_att_mn,matr_rea_mn,matr_pot_mn,cifre_att_mn,cifre_rea_mn,cifre_pot_mn,
						eaf1_mn,eaf2_mn,eaf3_mn,eaf4_mn,eaf5_mn,eaf6_mn,
						erf1_mn,erf2_mn,erf3_mn,erf4_mn,erf5_mn,erf6_mn,
						potf1_mn,potf2_mn,potf3_mn,potf4_mn,potf5_mn,potf6_mn,
						EaM_mn,ErM_mn,PotM_mn)

				}else{
					List(null,null,null,
						null,null,null,null,null,
						null,null,null,null,null,null,
						null,null,null,null,null,null,
						null,null,null,null,null,null,
						null,null,null,null,null,null,
						null,null,null)
				}



				//if(verbose=="true")
				//log.info("SMIS KEYROW : " + keyrow)

				val fields=List(codContrDisp,pod,pivautente,pivadistributore,anno,mese,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp,motivazione)

				var field_seq= Seq(Row.fromSeq(fields ++ fieldsSmontaggio ++ fieldsMontaggio))

				field_seq
			}


			dpRes.toList


		} catch {
			case e: Exception => {
				e.printStackTrace()
        throw e
			}
		}
	}

	/**
		* vengono letti e validati tramite xsd i file xml in ingresso
		* in base al contenuto del file elabora il vecchio tracciato(solo PDO,RFO) oppure il nuovo tracciato(tutte le tipologie di flusso)
		* viene restiutita una tripla dove il primo parametro contiene una sequenza di righe per tutti i flussi tranne SMIS
		*  il secondo parametro contiene una sequenza di righe con i dati di estensione del nuovo tracciato
		*  il terzo parametro contiene solamente una sequenza di righe per il flusso SMIS
		*/
	def letturaEValidazioni(rdd1: (Boolean,(String,String),String,Boolean), dataelaborazione:java.sql.Timestamp ): List[(Row)] = {
		val nomeFile = rdd1._2._1
		val noBom = if(rdd1._2._2.startsWith(UTF8_BOM)){
			rdd1._2._2.substring(1)
		}else{
			rdd1._2._2
		}
		val xml = XML.loadString(noBom)

		val isnewtrack=rdd1._4
		//val xml =rdd1._5
		val sezflusso=getParentSezXML(xml)

		val cod_flusso = (sezflusso \ "@CodFlusso").text

		val ret =if(cod_flusso.trim().contains("SMIS") ){
			letturaEValidazioni_FlussoSMIS(rdd1, dataelaborazione,xml)
		}else if(isnewtrack){
			letturaEValidazioni_NuovoFlusso(rdd1,dataelaborazione,xml)
		}else {
			if (cod_flusso.trim().contains("PDO") || cod_flusso.trim().contains("RFO")) {
				letturaEValidazioni_vecchioFormato(rdd1, dataelaborazione,xml)
			} else {
				//lettura altri flussi vecchio formato
				letturaEValidazioni_vecchioFormatoAltriFlussi(rdd1, dataelaborazione,xml)
			}
		}
		ret
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

/*
	val rdd2_report = rdd2.map(f => {
					if (f._1)
						Row("000", f._2._1, "OK", dataelaborazione, (argsObjMaster.anno + argsObjMaster.mese).toInt)
					else
						Row(f._3, f._2._1, f._2._2, dataelaborazione, (argsObjMaster.anno + argsObjMaster.mese).toInt)
				}
 */


	def tryToValidation(rdd_input: (String,String),tipo_flusso:String) : (Boolean,(String,String),String,Boolean) = {

		if(verbose=="true")
			log.info("***** Avvio Validazione XSD ")


		val noBom = if(rdd_input._2.startsWith(UTF8_BOM)){
			rdd_input._2.substring(1)
		}else{
			rdd_input._2
		}

		if(verbose=="true")
			log.info("***** Avvio Validazione nome file " + rdd_input._1)

		try {
			val xml = XML.loadString(noBom)
      val sezflusso=getParentSezXML(xml)
      val cod_flusso = (sezflusso \ "@CodFlusso").text
      val cod_flusso1G=cod_flusso.substring(0,3)
      val isNewFlusso=isNuovo2G(xml,cod_flusso)

			//val rdd = validationXSD(rdd_input, xml,tipo_flusso,cod_flusso)
      val rdd = validationXSD(rdd_input,tipo_flusso,cod_flusso,isNewFlusso)


			if (rdd._1) {
				try {
					val nome_file = if (rdd._2._1.lastIndexOf("/") > -1) rdd._2._1.substring(rdd._2._1.lastIndexOf("/") + 1).split("_")
					else if (rdd._2._1.lastIndexOf("\\") > -1) rdd._2._1.substring(rdd._2._1.lastIndexOf("\\") + 1).split("_")
					else rdd._2._1.split("_")



					//informazioni nome file versione prototipo
					val isNewTrack = rdd._4
					val PIvaDistributoreNF = nome_file(0)
					val PIvaUtenteNF = nome_file(1)
					val MeseAnnoNF = nome_file(2)
					var CodContrDispNF_ = ("000000" + nome_file(5).split("\\.")(0)) takeRight 6

					val sp = rdd._2._1.split("/")
					val annoMeseGiornoDir = ((s"${sp(sp.length - 3)}-${sp(sp.length - 2)}") take 7)+"-01"

					//informazioni tag xml
					val PIvaDistributoreTX = ((xml \\ "IdentificativiFlusso" \\ "PIvaDistributore") text)
					val PIvaUtenteTX = (xml \\ "IdentificativiFlusso" \\ "PIvaUtente") text
					val CodContrDispTX = (xml \\ "IdentificativiFlusso" \\ "CodContrDisp") text

					//istruzione (momentanea?) per altri flusso (no PDO,RFO) vecchio tracciato
					//if (CodContrDispNF_.trim().equals("000001"))
					//	CodContrDispNF_ = CodContrDispTX

					val dataNf = MeseAnnoNF.substring(0, 6)
          val tmpAnnomeseInt=dataNf.toInt

					val sezflusso = getParentSezXML(xml)
					val datiPod = (sezflusso \ "DatiPod")

					/*val hasSezMeseAnno : Boolean=((datiPod \ "MeseAnno").length > 0)
					val dpRes2 = if (hasSezMeseAnno) {
						datiPod.theSeq.map(y => ((y \ "MeseAnno").text).split("/")).map(x => if(x.length>=2) (x(1) + x(0)) else dataNf ).filter(!_.equals(dataNf))
					} else {
						Seq()
					}*/

					//CONTROLLI NUOVI
					val timeZone = prop.getProperty("spark.app.time_zone")
					val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
					val annomese_cur =(cal.get(Calendar.YEAR) + ("0" + (cal.get(Calendar.MONTH) + 1) takeRight 2 )).toInt
          //CONTROLLO SULLA MISURA FUTURA AL MOMENTO DISATTIVATO
					//val misura_futura=if(cod_flusso.toUpperCase =="SMIS" || cod_flusso.toUpperCase =="SNS" || cod_flusso.toUpperCase =="SOS" || cod_flusso.toUpperCase()== "SNF" || cod_flusso.toUpperCase() =="SOF") false else { if(annomese_cur < dataNf.toInt ) true else false}
          val misura_futura=false

					val checkMisure=if(cod_flusso1G != "")
					{

						val dpIt = datiPod.theSeq

						var res=dpIt.flatMap { dp =>

							val anno_F = dataNf.take(4)
							val mese_F = dataNf.takeRight(2)

							//DataMisure ,DataPrest ,DataVoltura sono nel formato dd/mm/yyyyy
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

          //CONTROLLO SULLA MISURA UNDER 60 MESI AL MOMENTO DISATTIVATO
          //val misura_under_60mesi=checkMisure._2
          val misura_under_60mesi=false

          val AM_NOTOK=checkMisure._1


					//mev21-030
					var isValid2= (true, "OK")
					if (isCheckAmmiOn.equals("true")) {
						isValid2 = if (PIvaDistributoreNF != PIvaDistributoreTX) {
							(false, "PIvaDistributoreNF")
						} else if (PIvaUtenteNF != PIvaUtenteTX) {
							(false, "PIvaUtenteNF")
						} else if (CodContrDispNF_ != CodContrDispTX) {
							(false, "CodContrDispNF")
						} else if (AM_NOTOK) {
							//mev 21-030
							// if (cod_flusso1G == "SNM" || cod_flusso1G == "RSN" || cod_flusso1G == "SOS" || cod_flusso1G == "SNS") {
							//(true, "OK")
							//}
							//else {
								(false, "MeseAnnoNF")

						} else if (misura_futura) { //NUOVO CONTROLLO
							(false, "ANNOMESE_FUTURO")
						} else if (misura_under_60mesi) { //NUOVO CONTROLLO
							(false, "MISURA_UNDER60MESI")
						} else {
							(true, "OK")
						}
					}


					if (isValid2._1) {
						(isValid2._1, rdd._2, "OK", isNewTrack)
					} else {
						(false, (rdd._2._1, "Dati non congrui causa : " + isValid2._2), "002", isNewTrack)
					}


				} catch {
					case e: Exception =>
						e.printStackTrace()
						(false, (rdd._2._1, e.getMessage), "003", false)
				}
			} else {
				(rdd._1, (rdd._2._1, rdd._2._2), rdd._3, rdd._4)
			}
		}
		catch {
			case e: Exception =>
				//e.printStackTrace()
				var tmp= XML.loadString("<FlussoMisure></FlussoMisure>")
				(false, (rdd_input._1, e.getMessage), "003", false)
		}
	}

	/**
		* Valida la consistenza file xml tramite dei XSD in termini di vincoli: quali elementi e attributi possono apparire,
		* in quale relazione reciproca, quale tipo di dati può contenere; al fine di accertare se i tipi di dati appartengono al documento xml.
		* @param rdd rappresenta il file xml.
		* @return un valore booleano corrispondente alla validazione e un messaggio d'errore.
		*/
	def validationXSD(rdd: (String,String) ,tipo_flusso:String,cod_flusso:String,isNewFlusso:Boolean) : (Boolean,(String,String),String,Boolean) =
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
			//val sezflusso=getParentSezXML(xmldata)
			//val cod_flusso = (sezflusso \ "@CodFlusso").text
			val cod_flusso1G=cod_flusso.substring(0,3)


			if((cod_flusso1G=="PDO" && tipo_flusso!=TypeDataToElab.Pdo.toString))
				return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)
			if((cod_flusso1G=="RFO" && tipo_flusso!=TypeDataToElab.Rfo.toString))
				return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)
			if((cod_flusso=="SMIS" && tipo_flusso!=TypeDataToElab.Smis.toString))
				return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)
			if((cod_flusso!="SMIS" && cod_flusso1G!="PDO" && cod_flusso1G!="RFO" && tipo_flusso!=TypeDataToElab.Other_Data.toString))
				return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)

			//val isNewFlusso=isNuovo2G(xmldata)


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


			//istruzione temporanea per i vecchi flussi non PDO e RFO e SMIS
			/*if(!isNewFlusso && !cod_flusso.contains("PDO") && !cod_flusso.contains("RFO") && !cod_flusso.contains("SMIS") && hashMap1.keySet.exists(_ == cod_flusso1G)) {
				return (true,rdd,"OK",isNewFlusso)
			}*/

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



						val newFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
						val newSourceList = if(f3_ok)Array(f33,f11,f22)else Array(f11,f22)
						val newSchema = newFactory.newSchema( newSourceList )
						val newValidator = newSchema.newValidator()
						val newSs = new StreamSource(new File( rdd._1.replaceFirst("file:///", "/").replaceFirst("file:/", "/").replaceFirst("file://", "/") ))
						newValidator.validate(newSs)
					}else{
						//	throw new Exception(e)
						if(verbose=="true")
							e.printStackTrace()
            if(!isNewFlusso && !(xsdProp_old == null) && (cod_flusso.contains("PDO") || cod_flusso.contains("RFO")))
              return validationXSD_old(rdd,tipo_flusso,cod_flusso,isNewFlusso)
            else
						return (false, (rdd._1, e.getMessage()),"001",isNewFlusso)
					}
			}
			//Row(f._3, f._2._1, f._2._2, dataelaborazione, (argsObjMaster.anno + argsObjMaster.mese).toInt))
			(true,rdd,"OK",isNewFlusso)
		}catch{
			case e: Exception =>
				if(verbose=="true")
					e.printStackTrace()
        if(!isNewFlusso && !(xsdProp_old == null) && (cod_flusso.contains("PDO") || cod_flusso.contains("RFO")))
          return validationXSD_old(rdd,tipo_flusso,cod_flusso,isNewFlusso)
				else
          return (false, (rdd._1, e.getMessage() + " - err1 " ),"001",isNewFlusso)
		}

	}

  def validationXSD_old(rdd: (String,String) ,tipo_flusso:String,cod_flusso:String,isNewFlusso:Boolean) : (Boolean,(String,String),String,Boolean) =
  {
    val defdomplextypes_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defdomplextypes")
    val defsimpletypes_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defsimpletypes")
    val flusso1_pdo_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_pdo")
    val flusso1_rfo_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_rfo")
    val flusso1_pdo_v2_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_pdo_v2")
    val flusso1_rfo_v2_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_rfo_v2")

    val defdomplextypes_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defdomplextypes.straniere")
    val defsimpletypes_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defsimpletypes.straniere")
    val flusso1_pdo_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_pdo.straniere")
    val flusso1_rfo_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_rfo.straniere")
    val flusso1_pdo_v2_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_pdo_v2.straniere")
    val flusso1_rfo_v2_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso1_rfo_v2.straniere")

    val flusso1_vno_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_vno")
    val flusso1_rnv_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_rnv")
    val flusso1_pno_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_pno")
    val flusso1_rno_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_rno")
    val flusso1_snm_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_snm")
    val flusso1_sof_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_sof")
    val flusso1_sos_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_sos")
    val flusso1_snf_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_snf")
    val flusso1_sns_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_sns")
    val flusso1_rsn_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_rsn")

    val defdomplextypes_2G_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defdomplextypes_2G")
    val defsimpletypes_2G_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defsimpletypes_2G")
    val flusso_new_pdo_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_new_pdo")
    val flusso_new_rfo_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_new_rfo")


    val defdomplextypes_2G_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defdomplextypes_2G.straniere")
    val defsimpletypes_2G_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.defsimpletypes_2G.straniere")
    val flusso_new_pdo_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_new_pdo.straniere")
    val flusso_new_rfo_straniere_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_new_rfo.straniere")
    val flusso_smis_old:String = xsdProp_old.getProperty("spark.validazione.xsd.flusso_smis")


    val keys1 = List("PDO","PNO","VNO","SNM")
    val keys2 = List("RFO","RNO","RNV" ,"RSN")

    //MANCA RNM
    val hashMap1: HashMap[String, String] =
      HashMap(("VNO",flusso1_vno_old), ("RNV",flusso1_rnv_old),
        ("PNO",flusso1_pno_old), ("RNO",flusso1_rno_old),
        ("SNM",flusso1_snm_old), ("SOF",flusso1_sof_old),
        ("SOS",flusso1_sos_old), ("SNF",flusso1_snf_old),
        ("SNS",flusso1_sns_old), ("RSN",flusso1_rsn_old), ("SMI",flusso_smis_old))

    try{
      val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)//CRLF e LF

      val nomeFile = rdd._1
      //val sezflusso=getParentSezXML(xmldata)
      //val cod_flusso = (sezflusso \ "@CodFlusso").text
      val cod_flusso1G=cod_flusso.substring(0,3)


      if((cod_flusso1G=="PDO" && tipo_flusso!=TypeDataToElab.Pdo.toString))
        return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)
      if((cod_flusso1G=="RFO" && tipo_flusso!=TypeDataToElab.Rfo.toString))
        return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)
      if((cod_flusso=="SMIS" && tipo_flusso!=TypeDataToElab.Smis.toString))
        return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)
      if((cod_flusso!="SMIS" && cod_flusso1G!="PDO" && cod_flusso1G!="RFO" && tipo_flusso!=TypeDataToElab.Other_Data.toString))
        return (false, (rdd._1, "file da non elaborare tipo flusso da elaborare :" + tipo_flusso +" trovato : " + cod_flusso ),"004",isNewFlusso)

      //val isNewFlusso=isNuovo2G(xmldata)


      if(verbose=="true") {
        if(cod_flusso.trim().contains("SMIS"))
          log.info("***** Tracciato flusso SMIS - file :" + nomeFile)
        else if (isNewFlusso)
          log.info("***** Versione Tracciato flusso : nuovo ("+ cod_flusso +") - file :" + nomeFile)
        else
          log.info("***** Versione Tracciato flusso : vecchio ("+ cod_flusso +") - file :" + nomeFile)
      }


      val f1:Source = if(isNewFlusso && !cod_flusso.trim().contains("SMIS")){
        new StreamSource(defdomplextypes_2G_old)
      }else{
        if(((!isNewFlusso && !cod_flusso.contains("PDO") && !cod_flusso.contains("RFO")) || cod_flusso.contains("SMIS"))  && hashMap1.keySet.exists(_ == cod_flusso1G)){
          val tmp=hashMap1(cod_flusso1G)
          val path = new File(tmp).getParent();
          val fx= path +"/FlussiDatiMisuraPrelievoEE-DefComplexTypes.xsd" ;

          new StreamSource(fx)
        }else{
          new StreamSource(defdomplextypes_old)
        }
      }

      val f2:Source = if(isNewFlusso && !cod_flusso.trim().contains("SMIS")){
        new StreamSource(defsimpletypes_2G_old)
      }else{
        if(((!isNewFlusso && !cod_flusso.contains("PDO") && !cod_flusso.contains("RFO")) || cod_flusso.contains("SMIS")) && hashMap1.keySet.exists(_ == cod_flusso1G)){
          val tmp=hashMap1(cod_flusso1G)
          val path = new File(tmp).getParent();
          val fx=path+"/FlussiDatiMisuraPrelievoEE-DefSimpleTypes.xsd" ;

          new StreamSource(fx)
        }else {
          new StreamSource(defsimpletypes_old)
        }
      }


      //istruzione temporanea per i vecchi flussi non PDO e RFO e SMIS
      /*if(!isNewFlusso && !cod_flusso.contains("PDO") && !cod_flusso.contains("RFO") && !cod_flusso.contains("SMIS") && hashMap1.keySet.exists(_ == cod_flusso1G)) {
        return (true,rdd,"OK",isNewFlusso)
      }*/

      var f3_ok=true
      val f3 =if(isNewFlusso && keys2.exists(cod_flusso.contains(_)) ) {
        new StreamSource(flusso_new_rfo_old)
      }else if(isNewFlusso && keys1.exists(cod_flusso.contains(_))) {
        new StreamSource(flusso_new_pdo_old)
      }else	if(cod_flusso.contains("SMIS")) {
        new StreamSource(flusso_smis_old)
      }else	if(cod_flusso.contains("RFO") && nomeFile.contains( v2_2G )){
        new StreamSource(flusso1_rfo_v2_old)
      }else if(cod_flusso.contains("RFO")){
        new StreamSource(flusso1_rfo_old)
      }else if(cod_flusso.contains("PDO") && nomeFile.contains( v2_2G )) {
        new StreamSource(flusso1_pdo_v2_old)
      }else if(cod_flusso.contains("PDO")){
        new StreamSource(flusso1_pdo_old)
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
              new StreamSource(defdomplextypes_2G_straniere_old)
            }else{
              new StreamSource(defdomplextypes_straniere_old)
            }

            val f22:Source = if(isNewFlusso){
              new StreamSource(defsimpletypes_2G_straniere_old)
            }else{
              new StreamSource(defsimpletypes_straniere_old)
            }


            f3_ok=true
            val f33 =if(isNewFlusso && keys2.exists(cod_flusso.contains(_)) ) {
              new StreamSource(flusso_new_rfo_straniere_old)
            }else if(isNewFlusso && keys1.exists(cod_flusso.contains(_))) {
              new StreamSource(flusso_new_pdo_straniere_old)
            }else	if(cod_flusso.contains("SMIS")) {
              new StreamSource(flusso_smis_old)
            }else	if(cod_flusso.contains("RFO") && nomeFile.contains( v2_2G )){
              new StreamSource(flusso1_rfo_v2_straniere_old)
            }else if(cod_flusso.contains("RFO")){
              new StreamSource(flusso1_rfo_straniere_old)
            }else if(cod_flusso.contains("PDO") && nomeFile.contains( v2_2G )) {
              new StreamSource(flusso1_pdo_v2_straniere_old)
            }else if(cod_flusso.contains("PDO")){
              new StreamSource(flusso1_pdo_straniere_old)
            }else{
              f3_ok=false
              new StreamSource("")
            }



            val newFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
            val newSourceList = if(f3_ok)Array(f33,f11,f22)else Array(f11,f22)
            val newSchema = newFactory.newSchema( newSourceList )
            val newValidator = newSchema.newValidator()
            val newSs = new StreamSource(new File( rdd._1.replaceFirst("file:///", "/").replaceFirst("file:/", "/").replaceFirst("file://", "/") ))
            newValidator.validate(newSs)
          }else{
            //	throw new Exception(e)
            if(verbose=="true")
              e.printStackTrace()
              return (false, (rdd._1, e.getMessage()),"001",isNewFlusso)
          }
      }
      //Row(f._3, f._2._1, f._2._2, dataelaborazione, (argsObjMaster.anno + argsObjMaster.mese).toInt))
      (true,rdd,"OK",isNewFlusso)
    }catch{
      case e: Exception =>
        if(verbose=="true")
          e.printStackTrace()
          return (false, (rdd._1, e.getMessage() + " - err1 " ),"001",isNewFlusso)
    }

  }

	def isEmpty[T](rdd : RDD[T]) = {
		try {
			rdd.take(1).length == 0
		}catch {
			case e: Exception =>  true
		}
	}

	def splitRdd[T:ClassTag](rdd: RDD[T], p: T => Boolean): (RDD[T], RDD[T]) = {

		val splits = rdd.mapPartitions { iter =>
			val (left, right) = iter.partition(p)
			val iterSeq = Seq(left, right)
			iterSeq.iterator
		}

		val left = splits.mapPartitions { iter => iter.next().toIterator}

		val right = splits.mapPartitions { iter =>
			iter.next()
			iter.next().toIterator
		}
		(left, right)
	}

	def walkTree(file:File):Iterable[File]={
		val children = new Iterable[File]{
			def iterator = if(file.isDirectory) file.listFiles.iterator else Iterator.empty
		}
		Seq(file)++: children.flatMap(walkTree(_))
	}

	def getCountFiles(dir:File): Long= {
		var count:Long =0
		for (f <- walkTree(dir)) if(f.getName.toLowerCase.matches(".*\\.xml")) count =count + 1

		return count
	}

	def can_go_ingestione(): Boolean ={

		val vmName = ManagementFactory.getRuntimeMXBean.getName
		val p = vmName.indexOf("@")
		val cur_pid = vmName.substring(0, p)

		val parAmm:String=""
		//verifica che non ci sia decompressione o ingestione in corso
		val tmpDec = Try("ps aux" #| "grep it.au.misure.cli.FlussoMisureTool" #| s"grep -v ${cur_pid}" #| s"grep D${parAmm}" !!) getOrElse("")
		val tmpInj = Try("ps aux" #| "grep it.au.misure.cli.FlussoMisureTool" #| s"grep -v ${cur_pid}" #| s"grep i${parAmm}" !!) getOrElse("")


		var pidfound=""

		if(tmpDec!="" && tmpDec.contains(s".jar -D${parAmm}") && !tmpDec.contains(s".jar -DGAS"))
		{
			val vals=tmpDec.split(" ")
			val utente=vals(0)
			for(i<-1 to vals.length-1) {
				if (vals(i).trim() != "" && pidfound ==""){
					pidfound=vals(i)
					if(cur_pid!=pidfound){
						val descr =  "decompressione"

						log.info(s"Attenzione è stato trovato un processo di ${descr} in corso , attendere la fine del processo in corso e riprovare")
						log.info(s"Utente processo ${descr} in corso : " + utente)
						log.info(s"PID processo ${descr} in corso : " + pidfound)
						return false
					}

				}
			}

		}

		pidfound=""
		if(tmpInj!="" && tmpInj.contains(s".jar -i${parAmm}"))
		{
			val vals=tmpInj.split(" ")
			val utente=vals(0)
			for(i<-1 to vals.length-1) {
				if (vals(i).trim() != "" && pidfound ==""){
					val pid=vals(i)
					if(cur_pid!=pid) {
						val descr = "ingestione"

						log.info(s"Attenzione è stato trovato un processo di ${descr} in corso , attendere la fine del processo in corso e riprovare")
						log.info(s"Utente processo ${descr} in corso : " + utente)
						log.info(s"PID processo ${descr} in corso : " + pidfound)
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

		if(!can_go_ingestione()) return


		log.info("***** Inizio processo " + argsObjMaster.appName + " *****")
		val nameApp= if(argsObjMaster.appName.contains("Decompressione ed ingestione")){
			log.info("***** Fase di ingestione NEW*****")
		  argsObjMaster.appName +" - Fase di ingestione - NEW"
		}else{
			argsObjMaster.appName + " "
		}

		log.info("***** current user " + System.getProperty("user.name") + "****")
		log.info(propertiesC.printEnvVar)


		val tipo_flusso = argsObjMaster.PdoRfo
		val isAggiorna: Boolean = commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)
		val injectionTmp: String = argsObjMaster.injectionTmp



		//val rootDir: String = argsObjMaster.rootDir.replace("isilonshare1G","Test_clouderaShare")
    val rootDir: String = argsObjMaster.rootDir

    log.info("*** injectionTmp: " + injectionTmp)
    log.info("*** rootDir: " + rootDir)

		//val anno_in: String = argsObjMaster.anno
		//val mese_im: String = argsObjMaster.mese
		//val giorno_in: String = argsObjMaster.giorno

		val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())

		val dataelaborazione_decompressione = readDataElabEE_Decompressione_HDFS()

		val tmpRootDir = s"${injectionTmp}${File.separator}${rootDir}"

		log.info("**** Avvio controllo numero file per calibrazione partizioni ****")

		val totF=getCountFiles(new File(tmpRootDir))
		log.info("**** Tot files :" + totF.toString +" ****")

		val conf = new SparkConf()
			.setAppName(nameApp)
			.set("spark.shuffle.service.enabled", "false")
			.set("spark.dynamicAllocation.enabled", "false")
			.set("spark.io.compression.codec", "snappy")
			.set("spark.rdd.compress", "true")
			//.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")



			.setMaster(argsObjMaster.master)


		val sc = new SparkContext(conf)
		sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

		//nuove
		sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
		sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
		//sc.hadoopConfiguration.set("parquet.enable.dictionary", "false")
		//sc.hadoopConfiguration.setInt("parquet.metadata.read.parallelism", 1)
		//fine nuove

		sc.setLogLevel(argsObjMaster.logLevel)

		val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions")

		val sqlCtx = new HiveContext(sc)

		sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

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

			val quartiPDO_RFO = "/user/silvia/au/misure_ee_au/flusso_misure_quarti"//sc.getConf.get("spark.flusso.misure.quarti")
			val flusso_EXT = "/user/silvia/au/misure_ee_au/flusso_misure_estensione_quarti_new" //sc.getConf.get("spark.flusso.misure.ext")
			val flusso_NoAggr = sc.getConf.get("spark.flusso.misure.noaggr")
			val flusso_SMIS = sc.getConf.get("spark.flusso.misure.smis")

			val report = "/user/silvia/au/misure_ee_au/report_ingestione_new" //sc.getConf.get("spark.flusso.misure.report")
			val slash = sc.getConf.get("spark.flusso.misure.slash")


			log.info("*** sc.master: " + sc.master)

			log.info("*** slash: " + slash)
			//hdfs
			log.info("*** quartiPDO_RFO: " + quartiPDO_RFO)
			log.info("*** tabelle hive quarti: " + tbl_misurequarti)
			log.info("*** estensione flusso dati: " + flusso_EXT)
			log.info("*** flusso dati da non aggregare: " + flusso_NoAggr)
			log.info("*** flusso dati SMIS: " + flusso_SMIS)

			log.info("*** report: " + report)
			log.info("*** tipo flusso: " + tipo_flusso)
			log.info("*** isAggiorna: " + isAggiorna)

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

			log.info("*** dataelaborazione: " + dataelaborazione)
			if(dataelaborazione_decompressione !=null)
				log.info("*** dataelaborazione di decompressione di riferimento: " + dataelaborazione_decompressione)

			//perdita di tensione
			log.info("*** perdita_380: " + perdita_380)
			log.info("*** perdita_220: " + perdita_220)
			log.info("*** perdita_150: " + perdita_150)
			log.info("*** perdita_1_35: " + perdita_1_35)
			log.info("*** perdita_1: " + perdita_1)

			log.info("*** verbose: "+ verbose)
			log.info("*** testlevel: "+ testlevel)


			val db = _dbDest

			log.info("*** database di destinazione : "+ db)



			val elencoUDDTmp: List[String] = scansionaUDD(tmpRootDir)
			if(elencoUDDTmp.length==0)
				{
					log.info("*** Nessun file da ingerire")
					return
				}

			//valore medio di un file 1,52 mb
			val numpartitions = (((totF *1.52)/(40*10))).toInt


			val maxparts=((sc.defaultParallelism - (sc.defaultParallelism*0.08)).toInt * 2).toInt
			val partitions=if(numpartitions <48) 48
			else if(numpartitions >maxparts) maxparts
			else numpartitions

			log.info("partizioni massime :" + maxparts.toString)

			val rddUDDTmp = sc.parallelize(elencoUDDTmp,partitions.toInt).setName("Scansiona alberatura temporanea")
			//rddUDDTmp.persist(StorageLevels.MEMORY_ONLY_SER)

			val parts :Int = rddUDDTmp.partitions.size

			log.info("*** numero di partizioni  :" + parts.toString)

			val lists = rddUDDTmp.mapPartitions { partition =>
				val ret = partition
					.map(rddUDDTmpDir => leggiPathXml(new File(rddUDDTmpDir)))
					.flatMap(f => f)
				ret
			}.collect().map(xml => "file://" + slash + xml.getPath).toList

			val lenf=lists.length

			log.info("*** numero di file in processamento :" + lenf.toString)

			val rddValidAll = sc.wholeTextFiles(lists.mkString(","),parts.toInt).setName("Acquisizione files di misurazione")
			log.info("*** num partizioni WholeTextFiles : " + rddValidAll.partitions.size.toString)
			log.info(s"*** WholeTextFiles OK")

			log.info("*** utilizzo metodo  tryToValidation " )
			log.info("*** controlli ammissibilita tipo 9xx abilitati:  " + isCheckAmmiOn )
			val rdd2 = rddValidAll.map(tryToValidation(_,tipo_flusso)).setName("Validazione xsd")
			rdd2.persist(StorageLevels.MEMORY_ONLY_SER)

			log.info("***** Validazione File + XSD OK")

			/*
      * INSERT REPORT IN THREAD
      */
			val threadReports = if( (!(rdd2.partitions.isEmpty)) &&  (testlevel=="1" || testlevel=="0" || testlevel==null)) {

        val numf_invalidi: org.apache.spark.Accumulator[Int] = sc.accumulator(0, "Files_invalidi")
        val numf_validi: org.apache.spark.Accumulator[Int] = sc.accumulator(0, "Files_validi")

				val rdd2_report = rdd2.map(f => {
					if (f._1) {
            Row("000", f._2._1, "OK", dataelaborazione, dataelaborazione_decompressione, (argsObjMaster.anno + argsObjMaster.mese).toInt)
          }
					else {
            Row(f._3, f._2._1, f._2._2, dataelaborazione, dataelaborazione_decompressione, (argsObjMaster.anno + argsObjMaster.mese).toInt)
          }
				}
				)
				log.info("***** filtro Report file OK")
        rdd2_report.foreach(f => if (f.getString(0) == "000") numf_validi += 1 else numf_invalidi += 1)

        log.info(s"***** NUMERO DI FILES VALIDI : ${numf_validi.value.toString} ")
        log.info(s"***** NUMERO DI FILES INVALIDI : ${numf_invalidi.value.toString} ")

				new Thread {

					override def run {

            sqlCtx.setConf("spark.sql.parquet.compression.codec", "snappy")
            val dfQS3 = sqlCtx.createDataFrame(rdd2_report, schemaReport_new)
            //dfQS3.persist(StorageLevels.MEMORY_ONLY_SER)

            log.info("***** creazione DataFrame Report misure OK")


            if (testlevel == "0" || testlevel == null) {
              log.info(s"***** Avvio scrittura su : ${report}")
              dfQS3
                .write.option("parquet.block.size", blocksize.toString)
                .format("parquet")
                .mode(SaveMode.Append)
                .partitionBy("annomese")
                .save(report)
              log.info("***** insert Report misure OK")

            }



          }
				}

			}else {
				new Thread(){

					override def run {}
					}
			}



			val validData = rdd2.filter(_._1).flatMap(letturaEValidazioni(_, dataelaborazione)).setName("Lettura e validazioni")
			log.info("***** lettura e validazioni OK")


			if (printdtframe == "true")
				validData.foreach(println)


		//	rddUDDTmp.unpersist()
			rdd2.unpersist()
			validData.persist(StorageLevels.MEMORY_ONLY_SER)



			var flusso_quarti=false
			var flusso_noaggr=false
			var flusso_quarti_ext=false
			var is_flusso_smis = false


			val cc_data=(!(validData.partitions.isEmpty))

			if (cc_data  && (tipo_flusso==TypeDataToElab.Pdo.toString || tipo_flusso==TypeDataToElab.Rfo.toString) && (testlevel=="1" || testlevel=="0" || testlevel==null)) {

				val vflusso_quarti = validData.filter(f => f.length == schemaQuarti_new.length)

				val thread1 = new Thread {
					override def run {

						val vflusso_ext = validData.filter(f => f.length == schemaflussoExt_new.length)

            val numPartPiene: org.apache.spark.Accumulator[Int] = sqlCtx.sparkContext.accumulator(0, "Partitioni_piene")
            vflusso_ext.foreachPartition(f=> if(f.length > 0)numPartPiene.add(1))

						if ((numPartPiene.value>0) && (testlevel == "1" || testlevel == "0" || testlevel == null)) {

              sqlCtx.setConf("spark.sql.parquet.compression.codec", "snappy")
							val dfQS11 = sqlCtx.createDataFrame(vflusso_ext, schemaflussoExt_new)

              //bilanciamento partizioni di scrittura
             /* val dftmp11 =dfQS11.select(col("*"), substring(col("podquarti"), 6, 2).as("codpod")).
                repartition(col("annoquarti"),col("mesequarti"),col("codpod"))
              val dfQS11x=dftmp11.drop("codpod")
              */
							log.info("***** creazione DataFrame schema estensione quarti OK")
							log.info(s"***** Avvio scrittura su : ${flusso_EXT}")

							if (testlevel == "0" || testlevel == null) {

                //dfQS11x
								dfQS11
									.write.option("parquet.block.size", blocksize.toString)
									.format("parquet")
									.mode(SaveMode.Append)
									.partitionBy("annoquarti", "mesequarti")
									.save(flusso_EXT)
								flusso_quarti_ext = true

							}


						}

					}

				}

				thread1.start


				if ((testlevel == "0" || testlevel == null)) {

          if(!vflusso_quarti.partitions.isEmpty) {
            sqlCtx.setConf("spark.sql.parquet.compression.codec", "snappy")
            val dfQS1 = sqlCtx.createDataFrame(vflusso_quarti, schemaQuarti_new)

            //bilanciamento partizioni di scrittura
            /*val dftmp1 =dfQS1.select(col("*"), substring(col("podquarti"), 6, 2).as("codpod")).
              repartition(col("annoquarti"),col("mesequarti"),col("codpod"))
            val dfQS1x=dftmp1.drop("codpod")
             */
            log.info("***** creazione DataFrame schema quarti OK")
						log.info(s"***** Avvio scrittura su : ${quartiPDO_RFO}")

            //dfQS1x
						dfQS1
              .write.option("parquet.block.size", blocksize.toString)
              .format("parquet")
              .mode(SaveMode.Append)
							.partitionBy("annoquarti", "mesequarti")
              .save(quartiPDO_RFO)

            flusso_quarti = true
            log.info("***** insert misure quarti OK")
          }



				}

				if(thread1.isAlive)
					thread1.join()

				if(flusso_quarti_ext)
          log.info("***** insert misure estensione quarti OK")
			}
			else {


				if (cc_data && (tipo_flusso == TypeDataToElab.Other_Data.toString) && (testlevel == "1" || testlevel == "0" || testlevel == null)) {

          val vflusso_noaggr = validData.filter(f => f.length == schemaflusso_noaggr.length)

					if ((testlevel == "0" || testlevel == null)) {

            if (!vflusso_noaggr.partitions.isEmpty) {
							sqlCtx.setConf("spark.sql.parquet.compression.codec", "snappy")
              //sqlCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
              val dfQS2 = sqlCtx.createDataFrame(vflusso_noaggr, schemaflusso_noaggr)

             /* val dftmp2 =dfQS2.select(col("*"), substring(col("pod"), 6, 2).as("codpod")).
                repartition(col("anno"),col("mese"),col("tipo_flusso"),col("codpod"))
              val dfQS2x=dftmp2.drop("codpod")
              */
              log.info("***** creazione DataFrame schema flusso da non aggregare OK")
							log.info(s"***** Avvio scrittura su : ${flusso_NoAggr}")

              //dfQS2x
							dfQS2
                .write.option("parquet.block.size", blocksize.toString)
                .format("parquet")
                .mode(SaveMode.Append)
                .partitionBy("anno", "mese", "tipo_flusso")
                .save(flusso_NoAggr)

              flusso_noaggr = true
            }
            log.info("***** insert flusso dati da non aggregare OK :" + flusso_NoAggr)


          }


				} else if (cc_data && (tipo_flusso == TypeDataToElab.Smis.toString) && (testlevel == "1" || testlevel == "0" || testlevel == null)) {


					if ((testlevel == "0" || testlevel == null)) {

            val vflusso_smis = validData.filter(f => f.length == schemaflussoSMIS.length)

            if (!vflusso_smis.partitions.isEmpty) {
              sqlCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
              val dfQS21 = sqlCtx.createDataFrame(vflusso_smis, schemaflussoSMIS)

              //bilanciamento partizioni di scrittura
              /*val dftmp21 =dfQS21.select(col("*"), substring(col("pod"), 6, 2).as("codpod")).
                repartition(col("anno_dtms"),col("mese_dtms"),col("codpod"))
              val dfQS21x=dftmp21.drop("codpod")
              */

              log.info("***** creazione DataFrame schema dati per SMIS OK")
							log.info(s"***** Avvio scrittura su : ${flusso_SMIS}")

              //dfQS21x
              dfQS21
                .write.option("parquet.block.size", blocksize.toString)
                .format("parquet")
                .mode(SaveMode.Append)
                .partitionBy("anno_dtms", "mese_dtms", "pivadistributore", "codcontrdisp")
                .save(flusso_SMIS)

              is_flusso_smis = true
            }

            log.info("***** insert dati per flusso SMIS OK")
          }

				}


			}

			validData.unpersist()
      if(flusso_quarti || flusso_noaggr || is_flusso_smis)
					threadReports.start()




			/*
        * aggiorno le partizioni
      */


			if(testlevel=="0" || testlevel==null) {


        log.info("***** Avvio aggiornamento partizioni ")

				if (flusso_quarti_ext) {
					sqlCtx.sql("MSCK REPAIR TABLE " + db + s".${tbl_misurequarti}")
					sqlCtx.sql("MSCK REPAIR TABLE " + db + ".flusso_misure_estensione_quarti_new")
				}
				else if(flusso_quarti)
					sqlCtx.sql("MSCK REPAIR TABLE " + db + s".${tbl_misurequarti}")

				if (flusso_noaggr)
					sqlCtx.sql("MSCK REPAIR TABLE " + db + ".flusso_misure_noaggr")
				if (is_flusso_smis)
					sqlCtx.sql("MSCK REPAIR TABLE " + db + ".flusso_misure_smis")



				if(threadReports.isAlive)
					threadReports.join()



        sqlCtx.sql("MSCK REPAIR TABLE " + db + ".report_ingestione")
        sqlCtx.sql("MSCK REPAIR TABLE " + db + ".report_ingestione_new")
        log.info("***** aggiornamento partizioni OK")

			}





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

		try {

			val pathudd = new File(injectionPath)
			if (!pathudd.exists())
				return abW2.toList
		}catch {
			case e: Exception =>  return abW2.toList
		}

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

	def readDataElabEE_Decompressione_HDFS():java.sql.Timestamp = {
		val hadoopConfig = new Configuration()
		val hdfs = FileSystem.get(hadoopConfig)
		try {
			val filehdfs = new Path("/tmp/decomprime_ee")

				if (hdfs.exists(filehdfs))
				{
				  val	is= hdfs.open(filehdfs)
					val reader = new BufferedReader(new InputStreamReader(is))
					val dt:String = reader.readLine()
					reader.close()
					is.close()
					java.sql.Timestamp.valueOf(dt)

				}
			  else null


			} catch {
			case e: Exception => { return null }


		}
	}

}
