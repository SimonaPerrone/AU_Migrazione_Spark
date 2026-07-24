import logging
import os
import constants

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import expr
from pyspark.sql import HiveContext
import xml.etree.ElementTree as ET
from pyspark.sql.types import *
from pyspark.sql.functions import lit
import datetime
import time

from Data import Data
from validator import Validator
from functions.util_file import UtilFiles
import threading
import re

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
import codecs
import hashlib

from flussi.flussoTAL import FlussoTAL
from flussi.flussoSW1 import FlussoSW1
from flussi.flussoTGL import FlussoTGL
from flussi.flussoRML import FlussoRML
from flussi.flussoTML import FlussoTML
from flussi.flussoRGL import FlussoRGL
from flussi.flussoRSL import FlussoRSL
from flussi.flussoTAV import FlussoTAV
from flussi.flussoTAS import FlussoTAS
from flussi.flussoRMV import FlussoRMV
from flussi.flussoFUI import FlussoFUI
from flussi.flussoDEF import FlussoDEF
from flussi.flussoTMV import FlussoTMV


class Ingestion:
    # Construttore
    def __init__(self, directoryName, mode, codFlusso,file, anno, mese, giorno, distributore):
        self.master = mode
        self.appName = "Ingestione " + constants.APPNAME
        self.workDirectory = directoryName
        self.codFlusso = codFlusso
        self.file = file
        self.anno = anno
        self.mese = mese
        self.giorno = giorno
        self.distributore = distributore
        self.debug = False

    # Stampa il contenuto della variabile "value"
    def stampa(self, value):
        """
            Funzione che permette di stampare il contenuto di un RDD
        """
        print("****************", value)

    def stampaPartition(self, iterator):
        """
            Funzione che permette di stampare il contenuto di un RDD 
            partizionato
        """
        for item in iterator:
            print(item)

    # Esegue il parsing xml dei dati
    def parse(self, root, dataElaborazione, file_xml):
        """
            Lettura e parsing dei file XML
        """
        data = Data(root, file_xml)
        result = data.toList(dataElaborazione=dataElaborazione)

        return result

    def get_filexsd(self, flusso):
        """
            Dato il flusso ritorna il nome del file XSD per la validazione
            Se il flusso non esiste viene ritornato un nome file non valido 
        """
        codice_servizio = flusso.get(constants.CODSERVIZIO_STR)
        codice_flusso   = flusso.get(constants.CODFLUSSO_STR)

        file_xsd = ""
        if codice_servizio == "TAL": file_xsd =  FlussoTAL('TAL').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "TML": file_xsd =  FlussoTML('TML').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "RML": file_xsd =  FlussoRML('RML').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "SW1": file_xsd =  FlussoSW1('SW1').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "TGL": file_xsd =  FlussoTGL('TGL').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "RGL": file_xsd =  FlussoRGL('RGL').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "RSL": file_xsd =  FlussoRSL('RSL').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "TAV": file_xsd =  FlussoTAV('TAV').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "TAS": file_xsd =  FlussoTAS('TAS').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "RMV": file_xsd =  FlussoRMV('RMV').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "FUI": file_xsd =  FlussoFUI('FUI').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "DEF": file_xsd =  FlussoDEF('DEF').getFileXSD(codice_servizio, codice_flusso)
        if codice_servizio == "TMV": file_xsd =  FlussoTMV('TMV').getFileXSD(codice_servizio, codice_flusso)
       
        if file_xsd == "" or file_xsd == None:
                file_xsd = constants.FILE_XSD_RMV_0400

        return file_xsd


    def validate_filename(self, s):
        """
            Validazione del nome del file
            Utilizza l'espressione regolare:
            ```
                ^\d{16,}_\d{16,}_\d{4,}_\w{3,}(.)(\d{4,})?_\d{14}_\d*.\w{3,}$
            ```
        """
        return bool(re.match("^\d{16,}_\d{16,}_\d{4,}_\w{3,}(.)(\d{4,})?_\d{14}_\d*.\w{3,}$", s))
        #return bool(re.match("^\d{11,}_\d{11,}_\d{4,}_\w{3,}(.)?\d{4,}_\d{14}_\d*.\w{3,}$", s))
    
    # Validazione del flusso dati
    # dataItem: [ <XML-filename>; <flusso-xml>, <file-xsd> ]
    # Ritorna: (resultValidation, dataItem[0], dataItem[1], file_xsd, errors )
    # resultValidation: risultato Validazione (Boolean)
    # dataItem[0]: file xml (String)
    # dataItem[1]: oggetto XML (ElementTree)
    # file_xsd: file xsd (String)
    # errors: errore (String)
    def validate(self, dataItem, cod_servizio):
        """
           Validazione del flusso dati
           In questa funzione viene verificato se il nome file rispetta la struttura prevista
           Ritorna:
                Risultato della validazione - boolean
                Nome file xml - string
                Nome file xsd - string
                codice causale - string 
                motivazione - string
        """
        resultValidation = True
        file_xsd = None

	errors = "Error not valid file name"
        cod_causale = "904"
        motivazione = "Il nome del file non rispetta la struttura prevista"

        fileName = os.path.basename(dataItem[0])
	#print ("full filename: ", dataItem[0])
        #print ("Validazione file: ", fileName)

        file_xsd = self.get_filexsd(dataItem[1])
        #print("File name:{} \nfile xsd:{}".format(fileName, file_xsd))

	#Validazione della cartella
        directory_name = os.path.dirname(dataItem[0])
        name_directory_name_prev = os.path.basename(directory_name)
        if (len(name_directory_name_prev) != 4):
            cod_causale, motivazione, resultValidation = ("001", "Il template (formato file e/o tracciato) utilizzato non e' congruo", False)
	    #print("err:", cod_causale, motivazione, resultValidation)
            return (resultValidation, dataItem[0], dataItem[1], file_xsd, cod_causale, motivazione)

        # Risultato della validazione:
        cod_causale, motivazione, resultValidation = Validator().validateFlusso(flussoXML=dataItem[1], file_xsd=file_xsd, file_xml=dataItem[0])

        if (resultValidation):
            
	    if cod_causale is not None:
 		    flusso = Data.getFlusso(cod_servizio, None)
		    if not flusso.isValidFile(fileName):
                	errors = "Error not valid file name"
	                cod_causale = "904"
        	        motivazione = "Il nome del file non rispetta la struttura prevista"
                	resultValidation = False

                        print("flusso:{}, cod_servizio: {}, filename:{}".format(flusso, cod_servizio, fileName))
		    else:
        	        errors, resultValidation = self.checkErrors(xml = dataItem[1], file_xsd = file_xsd, resultValidation = resultValidation)
            #print("errors", errors, resultValidation, fileName)
	else:
            errors = "Error not valid file name" 
            #cod_causale = "904"
            #motivazione = "Il nome del file non rispetta la struttura prevista"


	#print(resultValidation, cod_causale, motivazione, file_xsd, dataItem)
	# result, filename, xml, file_xsd, cod_causale, motivazione
        return (resultValidation, dataItem[0], dataItem[1], file_xsd, cod_causale, motivazione)



    def checkErrors(self, xml, file_xsd, resultValidation):
        """
            Verifica il file xml mediante il file xsd
            Ritorna 
                Messaggio di errore - string
                errore - boolean
        """
        if (xml.tag == "error"):
            return xml.text, False
             
        if (resultValidation == False):
            return "Error check file XSD: " + str(file_xsd), True

        return "", True

    def set_spark_context(self, app_name, master):
        """
        Crea il context spark
        """
        conf = SparkConf().setAppName(app_name) \
                              .setMaster(master) \
                              .set("spark.shuffle.service.enabled", "false") \
                              .set("spark.dynamicAllocation.enabled", "false") \
                              .set("spark.io.compression.codec", "snappy") \
                              .set("spark.rdd.compress", "true") \
                              .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
                              .set("spark.sql.execution.arrow.enabled", "true")
        sc = SparkContext(conf=conf)
        try:
            sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
            sqlCtx = sqlContext = HiveContext(sc)
    
        except py4j.protocol.Py4JError:
            sqlCtx = sqlContext = SQLContext(sc)
    
    
        sc._jsc.hadoopConfiguration().set( "mapreduce.input.fileinputformat.input.dir.recursive", "true")
        sqlContext.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        sqlContext.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        sqlContext.setConf("hive.exec.max.dynamic.partitions", "1000000000")

        sqlContext.setConf("spark.scheduler.mode", "FAIR")
        #sqlContext.setConf("spark.executor.instances", "5")
        
        return sc, sqlContext


    def decodeUTF8(self, item):
        """
        Decodifica il flusso dati nel formato utf-8
        """
        #return ET.fromstring(item.value)
        val = item.value
        #print("Decode UTF8: ", val)
        
        try:
            return ET.fromstring(val.encode('utf-8'))
        except Exception as e:
            try:
                return ET.fromstring(val.encode('utf-8')[:1])
            except Exception as e2:
                try:
                    return ET.fromstring(val.encode('utf-8')[:2])
                except Exception as e3:
                    return ET.fromstring("<error>Decoding UTF8: "+str(e3)+"</error>")

        return ET.fromstring("<error>No decode</error>")   

    def start(self):
        logging.debug("Start Ingestion")

        logging.debug("Configurazione spark configuration")
        appName=self.appName +  " " + self.codFlusso + ( " Anno:" + self.anno if self.anno is not None else "") + (" Mese:" + self.mese if self.mese is not None else "") + (" Giorno:" + self.giorno if self.giorno is not None else "")
        #appName="Ingestion"
        sc, sqlCtx = self.set_spark_context(appName, self.master)

        logging.debug("Configurazione spark configuration completata")

        # recupera dalla directory 'workDirectory' la lista dei file
        # questa lista e' filtrata recuperando solamente i file con estensione .xml
        # verra' formattato il percorso del file nella seguente forma: file://<path-filename>.xml
        print("Carica file XML dalla directory: " + self.workDirectory)
        
        # Il numero minimo di partizioni dipende dalla dimensione della lista dei file in input. 
        # Se la lista e' maggiore di 10 viene impostato il numero di partizione pari a 10 altrimenti alla dimensione 
        # della lista
        #minPartitions = 10 if len(listFiles) >= 10 else len(listFiles)
        minPartitions = 215 
        print ("Setting Partition: {}".format(minPartitions))

        # Se l'utente non ha impostato il file da elaborare verra' prelevato il contenuto delle directory
        if (self.file == ""):
            #if (self.anno and self.mese and self.distributore):
            #    print("Utilizzo ricerca anno, mese e distributore")
            #    listFiles = UtilFiles().getListOfFilesByAnnoMeseDistr(self.workDirectory, self.anno, self.mese, self.distributore, constants.EXT_XML)
            #    listFiles = sc.parallelize(listFiles, minPartitions).map(lambda f: "file://" + f)
            #elif (self.anno and self.mese):
            #    print("Utilizzo ricerca anno e mese")
            #    listFiles = UtilFiles().getListOfFilesByAnnoMese(self.workDirectory, self.anno, self.mese, constants.EXT_XML)
            #    listFiles = sc.parallelize(listFiles, minPartitions).map(lambda f: "file://" + f)
            #elif (self.anno):
            #    print("Utilizzo ricerca solo per anno")
            #    listFiles = UtilFiles().getListOfFilesByAnno(self.workDirectory, self.anno, constants.EXT_XML)
            #    listFiles = sc.parallelize(listFiles, minPartitions).map(lambda f: "file://" + f)
            #else:	
            #    print("Ricerca file")
            #    #listFiles = UtilFiles().getListOfFiles(self.workDirectory, constants.EXT_XML)
            #    listFiles = UtilFiles().getListOfFiles2V_parallel2(sc, self.workDirectory, self.anno, self.mese, None, self.codFlusso, self.distributore)

            # Versione 2
            listFiles = UtilFiles().getListOfFiles2V_parallel2(sc, self.workDirectory, self.anno, self.mese, self.giorno, self.codFlusso, self.distributore)

            # Versione 3
            #PATHHDFS_FILES = "/user/silvia/au/misure_gas_au/cmg_gas/gas_zip"
            #dataframe_files = sqlCtx.read.parquet(PATHHDFS_FILES)
            #listFiles = dataframe_files.map(lambda d: d["file"].replace("/mnt/isiloshare_gas","/mnt/isiloshare1/TEST_GAS_INJ"))
	else: # Utilizzo query
            print("Estrazione dati da query: {}".format(self.file))
	    with open(self.file, 'r') as content_file:
            	query_tmp = content_file.read()

	    print ("Query: {}".format(query_tmp))
            listFiles = sqlCtx.sql(query_tmp)
            #listFiles.show(truncate = False)
        #else: # altrimenti viene utilizzato il nome del file presente nella directory di lavoro impostata
        #    print("Elaborazione del file:", self.file)
        #    listFiles = sc.parallelize([ (self.workDirectory + self.file) ], 1)

	#query_tmp = " select concat('/mnt/isilonshare1/TEST_GAS_INJ/TMG_',t_piva_distributore,'/DISTRIBUTORE/TMG_',t_piva_distributore, '_', t_piva_udd, '/',t_anno_caricamento, '/', t_mese_caricamento, t_giorno_caricamento, '/',t_nome_file)  from cmg_gas.tmv_recupero"
	#query_tmp = "select recupero_tmv.t_nome_file from cmg_gas.recupero_tmv left join  cmg_gas.prt_cmg_file on recupero_tmv.t_nome_file = prt_cmg_file.t_nome_file where prt_cmg_file.n_id_file is null"
	#query_tmp ="select * from cmg_gas.recupero_tmv"
        #listFiles = sqlCtx.sql(query_tmp)

        #listFiles.map(lambda x: (x,)).toDF().show(truncate = False)
        rddTempUDD = listFiles.flatMap(lambda d: d).map(lambda f: "file://" + f)
        
        # NOTA: La funzione wholeTextFile vuole in ingresso la lista dei file separati dal carattere ","
        # La collection "Lista File" viene convertita in una stringa, nella quale sono contenuti i nomi
        # dei files separate dalla "," (virgola)
        # eseguo la funzione di apertura e lettura dei file
        print("Creazione lista file da caricare")
        array_files = rddTempUDD.collect()
        #print(type(array_files))
        #print("Array generato dalla ricerca nella struttura cloud\n{}".format(array_files))

        if (self.codFlusso != ""):
            array_files = [collection_ele for collection_ele in array_files if self.codFlusso in collection_ele]
        collection_files = ",".join(f for f in array_files)
	#print collection_files
	#return
        self.log("Collection da inviare al wholeTextFiles: ", collection_files)

        print("Lettura file XML: items: {}".format(len(array_files)))
	if (len(array_files) == 0):
	    return

        rddValidAll = sc.wholeTextFiles(collection_files, minPartitions=minPartitions, use_unicode=False).setName("Acquisizione files di misurazione")

        vflusso = sqlCtx.createDataFrame(rddValidAll, ['file', 'value'])
        

        # Calcolo la data Elaborazione.
        # Mediante la funzione datetime.now recupero la data di sistema nel formato ISO
        dataElaborazione = datetime.datetime.now().isoformat()
        #logging.debug("Set Data di elaborazione: " + dataElaborazione)

        # Parsing dei file xml
        # Per ogni file presente nella collection viene eseguito il parsing
        # i dati sono conservati all'interno della variabile "data" nel formato <filename>, <struct-xml>
        #logging.debug("Parsing file XML")
        data = vflusso.map(lambda d: (d.file,self.decodeUTF8(d))).setName("parsing files xml")

	#print("data parsing xml:",data.count())

        # Valida il flusso dati XML
        # la procedura di validazione utilizza la funzione "validate":
        # Il risultato della validazione viene generata una tabella nella quale e' contenuto il flag "valid"
        # se il valore del flag "valid" e' True il flusso XML e' valido altrimenti il valore e' False
        # valid; <filename>; <struct-xml>
        #logging.debug("Validazione file XML")
        dataValidate = data.map(lambda d: self.validate(d, self.codFlusso)).setName("Validazione")
        # Caching rdd
        #dataValidate.cache()
	
	#dataValidate.(truncate = False)
	#dataValidate.foreach(lambda d: self.stampa(d))
	#print ("validazione: ", dataValidate.count())

        #dataValidate.foreach(lambda d: self.stampa(d))
        # Per ogni elemento nella variabile data viene eseguita l'elaborazione che recupera tutti
        # i dati necessari per l'archivizione

        # f[0] = stato validazione
        # d[0] = codice_servizio
        # d[1] = (resultValidation, dataItem[0], dataItem[1], file_xsd)
        #logging.info("Lettura e imporazione dei dati dai file XML")
        #dataParsed = dataValidate.filter(lambda f: (f[0] == True and f[2] != None)) \
        dataParsed = dataValidate.filter(lambda f: (f[0] == True )) \
                                 .flatMap(lambda d: self.parse(d[2], dataElaborazione=dataElaborazione, file_xml = d[1])) \
                                 .map(lambda f: (f[0], f)).setName("lettura xml")
        
 	dataParsed.cache()
        #print("Parse dataValidate", dataParsed.count())
	# Recupero elementi non validi
	dataParsed_notValid = dataValidate.filter(lambda f: (f[0] == False)) 
	dataParsed_notValid.cache()

	# ATTIVARE
	#dataParsed_notValid_cnt=dataParsed_notValid.count()
	#dataParsed_valid_cnt=dataParsed.count()
	#print("dataParsed Valid count: {}".format(dataParsed_valid_cnt)) 
        #print("dataParsed not Valid count: {}".format(dataParsed_notValid_cnt))
	
        # Elaborazione/Ingestione nella tabella cmg_file_p
         
	self.elaborateFilesThead(dataValidate, 11, sc, sqlCtx)
	self.notValid(dataParsed_notValid, sqlCtx, 11, sc)

        #return
        """
        self.elaborateFilesThead(dataValidate, 11, sc, sqlCtx)
	self.notValid(dataParsed_notValid, sqlCtx, 11, sc)

        """
	# Caching rdd        
        #print("Caching dataParsed", dataParsed.count())
        dataParsed.cache()

        # Ogni elemento della tabella e' composto da (CodiceFlusso, array<dati>)
        # Vengono prelevati le varie collection filtrando per i vari flussi dati definite nell'attributo xml di ogni file

        if (self.codFlusso != ""):
            rdd = dataParsed.filter(lambda f: f[0] == self.codFlusso)	    
	    rdd = rdd.map( lambda d: d[1]).setName("Filtro flusso " + str(self.codFlusso))
	    print ("cod Flusso: {}".format(self.codFlusso))
	    #print ("flusso count: ", rdd.count())
            self.getWorker(codiceFlusso = self.codFlusso)(rdd, 1, sc, sqlCtx)

            #thRdd = threading.Thread(target=self.getWorker(codiceFlusso = self.codFlusso), args=(rdd, 1, sc, sqlCtx, ))
            #thRdd.start()
            dataParsed.unpersist()
            dataValidate.unpersist()
            return
        else:
            self.log("Avvio ingestione per tutti i flussi", "")
            rddTAL = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_TAL).map( lambda d: d[1]).setName("Filtro flusso TAL")
            rddSW1 = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_SW1).map( lambda d: d[1]).setName("Filtro flusso SW1")
            rddTGL = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_TGL).map( lambda d: d[1]).setName("Filtro flusso TGL")
            rddRML = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_RML).map( lambda d: d[1]).setName("Filtro flusso RML")
            rddTML = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_TML).map( lambda d: d[1]).setName("Filtro flusso TML")
            rddRSL = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_RSL).map( lambda d: d[1]).setName("Filtro flusso RSL")
            rddRGL = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_RGL).map( lambda d: d[1]).setName("Filtro flusso RGL")
            rddTAV = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_TAV).map( lambda d: d[1]).setName("Filtro flusso TAV")
            rddTAS = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_TAS).map( lambda d: d[1]).setName("Filtro flusso TAS")
            rddRMV = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_RMV).map( lambda d: d[1]).setName("Filtro flusso RMV")
            rddFUI = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_FUI).map( lambda d: d[1]).setName("Filtro flusso FUI")
            rddDEF = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_DEF).map( lambda d: d[1]).setName("Filtro flusso DEF")
            rddTMV = dataParsed.filter(lambda f: f[0] == constants.CODSERVIZIO_TMV).map( lambda d: d[1]).setName("Filtro flusso TMV")

            self.getWorker(codiceFlusso = self.codFlusso)(rddTAL, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddSW1, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddTGL, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddRML, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddTML, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddRSL, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddRGL, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddRGL, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddTAV, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddTAS, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddRMV, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddFUI, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddDEF, 1, sc, sqlCtx) 
            self.getWorker(codiceFlusso = self.codFlusso)(rddTMV, 1, sc, sqlCtx) 

            dataParsed.unpersist()
            dataValidate.unpersist()




    def log(self, text, message_obj):
        """
        Log
        """
        if self.debug == True:
            print(text, message_obj)


    def getWorker(self, codiceFlusso):
        """
        Dato il codiceFlusso ritorna la funzione associato al flusso 
        """
        if (codiceFlusso == constants.CODSERVIZIO_TAL): return FlussoTAL('TAL').write
        if (codiceFlusso == constants.CODSERVIZIO_SW1): return FlussoSW1('SW1').write
        if (codiceFlusso == constants.CODSERVIZIO_TGL): return FlussoTGL('TGL').write
        if (codiceFlusso == constants.CODSERVIZIO_RML): return FlussoRML('RML').write
        if (codiceFlusso == constants.CODSERVIZIO_TML): return FlussoTML('TML').write
        if (codiceFlusso == constants.CODSERVIZIO_RSL): return FlussoRSL('RSL').write
        if (codiceFlusso == constants.CODSERVIZIO_RGL): return FlussoRGL('RGL').write
        if (codiceFlusso == constants.CODSERVIZIO_TAV): return FlussoTAV('TAV').write
        if (codiceFlusso == constants.CODSERVIZIO_TAS): return FlussoTAS('TAS').write
        if (codiceFlusso == constants.CODSERVIZIO_RMV): return FlussoRMV('RMV').write
        if (codiceFlusso == constants.CODSERVIZIO_FUI): return FlussoFUI('FUI').write
        if (codiceFlusso == constants.CODSERVIZIO_DEF): return FlussoDEF('DEF').write
        if (codiceFlusso == constants.CODSERVIZIO_TMV): return FlussoTMV('TMV').write
   
    def notValid(self, rdd, sqlCtx, pool_id, sc):
	"""
	   Scrive nella tabella i record non validi
	   RDD contiene questi attributi: result, filename, xml, file_xsd, cod_causale, motivazione
	   sqlCtx: sql context
	"""	
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
	rdd_notValid=rdd.map(lambda d: (d[0],d[1],d[3],d[4],d[5]))
	
	# Dimensione del file
        dimensione_udf = udf(
            lambda namefile: os.path.getsize(namefile)
        )

        # Anno dal nome del file
        anno_udf = udf(
            lambda d:  (os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][2:4]) if len(os.path.basename(d).split("_")) >= 3 else ""
        )
    
        # Mese dal nome del file
        mese_udf = udf(
            lambda d:  (os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2]) if len(os.path.basename(d).split("_")) >= 3 else ""
        )

        # Tipo del file esempio XML
        tipo_file_udf = udf(
            lambda d: (os.path.splitext(d)[1][1:]) if len(os.path.basename(d).split("_")) >= 2 else ""
        )

        # Anno file nel cloud
        anno_caricamento_udf = udf(
            lambda d: d.split("/")[8]
            #lambda d: os.path.basename(d).split("_")[4][0:4]
            #lambda d: time.strftime('%Y',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        )

        # Mese file nel cloud
        mese_caricamento_udf = udf(
            lambda d: str(d.split("/")[9][0:2]).rjust(2, '0')
            #lambda d: os.path.basename(d).split("_")[4][4:6]
            #lambda d: time.strftime('%m',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        )

        # Giorno file nel cloud
        giorno_caricamento_udf = udf(
            lambda d: str(d.split("/")[9][2:4]).rjust(2, '0')
            #lambda d: os.path.basename(d).split("_")[4][6:8]
            #lambda d: time.strftime('%d',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        ) 

        # Dimensione file
        dimensione_udf = udf(
            lambda d: os.path.getsize(d) if os.path.isfile(d) else -1
        )

        # Recupero codice servizio esempio TMV
        tipo_servizio_udf = udf(
            lambda d: (os.path.basename(d).split("_")[3] if len(os.path.basename(d).split("_")[3]) <= 3 else os.path.basename(d).split("_")[3][0:3]) if len(os.path.basename(d).split("_")) >= 4 else "EE"
        )

        # Recupero codice flusso esempio 0350
        tipo_flusso_udf = udf(
            lambda d: (os.path.basename(d).split("_")[4] if (len(os.path.basename(d).split("_")) > 6 ) else (os.path.basename(d).replace(".xml","").replace(".",'') + ".xml").split("_")[3][3:7]) if len(os.path.basename(d).split("_")) >= 5 else "EE"
        )        

        # Genera id file
        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # Anno Mese di riferimento ( Anno + Mese ) dal nome file
        annomese_rif_udf = udf(
	        lambda d: (str(os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else "20" + os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2])) if len(os.path.basename(d).split("_")) >= 3 else ""        
	    )


        piva_distributore_udf = udf(
            lambda d: (d.split("/")[-4]).split("_")[1] if len(d.split("/")) == 10 else ""
        )

        tipoflusso_udf = udf(
            lambda x: "EE" if x is not None  else x
        )


	schemaElaborateFiles = StructType([
            StructField("b_ammissibile", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("filexsd", StringType(), True),
            StructField("t_cod_causale", StringType(), True),
            StructField("t_motivazione", StringType(), True)
        ])

        dfElaborateFiles = sqlCtx.createDataFrame(rdd_notValid, schema=schemaElaborateFiles)
	dataElaborazione = str(datetime.datetime.now().isoformat())
	dfElaborateFiles = dfElaborateFiles.withColumn("filename",  expr("regexp_replace(filename, 'file:','')")  )
	dfElaborateFiles = dfElaborateFiles.withColumn("n_id_file",  n_id_file_udf(col('filename')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_cmg",  lit(None).cast(StringType()))
	dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file",  lit(col('filename')))
	#dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file",  expr("regexp_replace(filename, '/isilonshare_gas', '')"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_stato_file",  lit("A"))
        dfElaborateFiles = dfElaborateFiles.withColumn("d_data_caricamento",  lit(dataElaborazione))

        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_file", tipo_file_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_anno_caricamento", anno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_mese_caricamento", mese_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_giorno_caricamento", giorno_caricamento_udf(col('t_nome_file')))


        dfElaborateFiles = dfElaborateFiles.withColumn("t_digest", lit(None).cast(StringType())) 
        dfElaborateFiles = dfElaborateFiles.withColumn("n_dimensione", dimensione_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_inviato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_distributore", piva_distributore_udf(col('t_nome_file'))) 
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_udd", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipo_servizio_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_flusso", tipo_flusso_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("presente_db", lit("N"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_annomese_rif", annomese_rif_udf( col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_padre", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_verificato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo_csv", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipoflusso_udf(col('t_tipo_servizio')))


	dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file",  expr("regexp_replace(filename, '/isilonshare_gas', '')"))

	#dfElaborateFiles.show(truncate = False)

	#constants.PATHHDFS_ELEBORATEFILES= "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_file_p"
        #constants.PATHHDFS_ELEBORATEFILES= "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_file_backeted_p_test"
        PARTITIONLIST = ["t_anno_caricamento", "t_mese_caricamento", "t_tipo_servizio"]
	print ("Not Valid records - Write: ", constants.PATHHDFS_ELEBORATEFILES)
        dfElaborateFiles.write.partitionBy(PARTITIONLIST).parquet(constants.PATHHDFS_ELEBORATEFILES, 'append')
        #dfElaborateFiles.coalesce(1).write.partitionBy(PARTITIONLIST).parquet(constants.PATHHDFS_ELEBORATEFILES, 'append')

        #constants.CMD_REFRESH_ELABORATEFILES="MSCK REPAIR TABLE cmg_gas.prt_cmg_file_backeted_p_v2"
        sqlCtx.sql(constants.CMD_REFRESH_ELABORATEFILES)

        sc.setLocalProperty("spark.scheduler.pool", None)

    def elaborateFilesThead(self, rdd, pool_id, sc, sqlCtx):
        """
        Funzione che permette di salvare i nomi dei file elaborati all'interno della tabella

        Il formato del flusso dati e' nel seguente formato
        <PIVA Distributore>_<PIVA Utente>_<AAAAMM>_<FlussoOperazione>_<Timestamp>_<Progressivo>.xml
        esempio:
            12883450152_06655971007_0618_TMV.0350_20180620183521_247.xml
            12883450152_06655971007_0618_TMV_0350_20180620183521_245.xml
            12883450152_06655971007_0618_TMV_20180620183521_248.xml
            12883450152_06655971007_0618_TMV0350_20180620183521_246.xml
        """
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        
        rddElaborateFiles = rdd.map(lambda d:  (d[1], d[0], d[4], d[5]))
        
        if (self.codFlusso != ""):
            rddElaborateFiles = rddElaborateFiles.filter(lambda r: self.codFlusso in r[0])	

        # Dimensione del file
        dimensione_udf = udf(
            lambda namefile: os.path.getsize(namefile)
        )

        # Anno dal nome del file
        anno_udf = udf(
            lambda d:  (os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][2:4]) if len(os.path.basename(d).split("_")) >= 3 else ""
        )
    
        # Mese dal nome del file
        mese_udf = udf(
            lambda d:  (os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2]) if len(os.path.basename(d).split("_")) >= 3 else ""
        )

        # Tipo del file esempio XML
        tipo_file_udf = udf(
            lambda d: (os.path.splitext(d)[1][1:]) if len(os.path.basename(d).split("_")) >= 2 else ""
        )

        # Anno file nel cloud
        anno_caricamento_udf = udf(
            lambda d: d.split("/")[8]
            #lambda d: os.path.basename(d).split("_")[4][0:4]
            #lambda d: time.strftime('%Y',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        )

        # Mese file nel cloud
        mese_caricamento_udf = udf(
	    lambda d: str(d.split("/")[9][0:2]).rjust(2, '0')
            #lambda d: d.split("/")[9][0:2]
            #lambda d: os.path.basename(d).split("_")[4][4:6]
            #lambda d: time.strftime('%m',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        )

        # Giorno file nel cloud
        giorno_caricamento_udf = udf(
	    lambda d: str(d.split("/")[9][2:4]).rjust(2, '0')
            #lambda d: d.split("/")[9][2:4]
            #lambda d: os.path.basename(d).split("_")[4][6:8]
            #lambda d: time.strftime('%d',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        ) 

        # Dimensione file
        dimensione_udf = udf(
            lambda d: os.path.getsize(d) if os.path.isfile(d) else -1
        )

        # Recupero codice servizio esempio TMV
        tipo_servizio_udf = udf(
            lambda d: (os.path.basename(d).split("_")[3] if len(os.path.basename(d).split("_")[3]) <= 3 else os.path.basename(d).split("_")[3][0:3]) if len(os.path.basename(d).split("_")) >= 4 else "EE"
        )

        # Recupero codice flusso esempio 0350
        tipo_flusso_udf = udf(
            lambda d: (os.path.basename(d).split("_")[4] if (len(os.path.basename(d).split("_")) > 6 ) else (os.path.basename(d).replace(".xml","").replace(".",'') + ".xml").split("_")[3][3:7]) if len(os.path.basename(d).split("_")) >= 5 else ""
        )        

        # Piva del distributore dal nome file
        #piva_distributore_udf = udf(
        #    lambda d: os.path.basename(d).split("_")[0] if len(os.path.basename(d).split("_")) >= 1 else ""
        #)

        # Piva dell udd dal nome file
        #piva_udd_udf = udf(
        #    lambda d: os.path.basename(d).split("_")[1] if len(os.path.basename(d).split("_")) >= 2 else ""
        #)

        # Genera id file
        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # Anno Mese di riferimento ( Anno + Mese ) dal nome file
        annomese_rif_udf = udf(
	        lambda d: (str(os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else "20" + os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2])) if len(os.path.basename(d).split("_")) >= 3 else ""        
	    )

	ammissibile_udf = udf(
            lambda d: True if d == "" else False
        )

	piva_distributore_udf = udf(
            lambda d: (d.split("/")[-4]).split("_")[1] if len(d.split("/")) >= 1 else ""
        )
        
        piva_udd_udf = udf(
            lambda d: (d.split("/")[-4]).split("_")[2] if len(d.split("/")) >= 1 else ""
        )

        tipoflusso_udf = udf(
            lambda x: "EE" if x is None  else x
        )
        
        schemaElaborateFiles = StructType([
            StructField("namefile", StringType(), True), 
            StructField("esito", StringType(), True), 
            StructField("t_cod_causale", StringType(), True), 
            StructField("t_motivazione", StringType(), True)
        ])

	rddElaborateFiles = rddElaborateFiles.filter(lambda d: d[1] == True)
        dfElaborateFiles = sqlCtx.createDataFrame(rddElaborateFiles, schema=schemaElaborateFiles)
	#dfElaborateFiles.show()

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfElaborateFiles = dfElaborateFiles.withColumn("namefile",  expr("regexp_replace(namefile, 'file:','')")  )
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_file",  n_id_file_udf(col('namefile')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_cmg",  lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file",  lit(col('namefile')))
        #dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file", expr("regexp_replace(namefile, '/isilonshare_gas','')")  ) 
        dfElaborateFiles = dfElaborateFiles.withColumn("t_stato_file",  lit("A"))
        dfElaborateFiles = dfElaborateFiles.withColumn("d_data_caricamento",  lit(dataElaborazione))

        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_file", tipo_file_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_anno_caricamento", anno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_mese_caricamento", mese_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_giorno_caricamento", giorno_caricamento_udf(col('t_nome_file')))

	dfElaborateFiles = dfElaborateFiles.withColumn("b_ammissibile", ammissibile_udf(col("t_cod_causale")))

        #dfElaborateFiles = dfElaborateFiles.withColumn("b_ammissibile", lit(col('esito')))
        #dfElaborateFiles = dfElaborateFiles.withColumn("t_cod_causale", lit("")) # Non inserire
        #dfElaborateFiles = dfElaborateFiles.withColumn("t_motivazione", lit("")) # Non inserire
        dfElaborateFiles = dfElaborateFiles.withColumn("t_digest", lit(None).cast(StringType())) 
        dfElaborateFiles = dfElaborateFiles.withColumn("n_dimensione", dimensione_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_inviato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_distributore", piva_distributore_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_udd", piva_udd_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipo_servizio_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_flusso", tipo_flusso_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("presente_db", lit("N"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_annomese_rif", annomese_rif_udf( col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_padre", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_verificato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo_csv", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipoflusso_udf(col('t_tipo_servizio')))


	dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file",  expr("regexp_replace(namefile, '/isilonshare_gas', '')"))
	#TEST
	#dfElaborateFiles.show(truncate = False)
	#dfElaborateFiles.foreach(self.stampa)

	#return
       
	#dfElaborateFiles.show(truncate = False)
        #constants.PATHHDFS_ELEBORATEFILES= "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_file_backeted_p_test"
	PARTITIONLIST = ["t_anno_caricamento", "t_mese_caricamento", "t_tipo_servizio"]
        print ("Valid records - Write: ", constants.PATHHDFS_ELEBORATEFILES)
        dfElaborateFiles.write.partitionBy(PARTITIONLIST).parquet(constants.PATHHDFS_ELEBORATEFILES, 'append')
        #dfElaborateFiles.coalesce(1).write.partitionBy(PARTITIONLIST).parquet(constants.PATHHDFS_ELEBORATEFILES, 'append')

        #constants.CMD_REFRESH_ELABORATEFILES="MSCK REPAIR TABLE cmg_gas.prt_cmg_file_backeted_p_v2"
	print ("Refresh tabella: ", constants.CMD_REFRESH_ELABORATEFILES)
        sqlCtx.sql(constants.CMD_REFRESH_ELABORATEFILES)

        #Stampa il numero di file elaborati
        #print ("Number files Elaborated: ", rddElaborateFiles.count())
        #print ("Number files Failed: ", rddElaborateFiles.filter(lambda d: d[1] == False).count())
        sc.setLocalProperty("spark.scheduler.pool", None)
