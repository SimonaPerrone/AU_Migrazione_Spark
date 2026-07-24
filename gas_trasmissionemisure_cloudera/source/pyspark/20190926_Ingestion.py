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
    def __init__(self, directoryName, mode, codFlusso,file, anno, mese, distributore):
        self.master = mode
        self.appName = "Ingestione " + constants.APPNAME
        self.workDirectory = directoryName
        self.codFlusso = codFlusso
        self.file = file
        self.anno = anno
        self.mese = mese
        self.distributore = distributore
        self.debug = False

    # Debug: stampa il contenuto della variabile "value"
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
                ^\d{11,}_\d{11,}_\d{4,}_\w{3,}(.)(\d{4,})?_\d{14}_\d*.\w{3,}$
            ```
        """
        return bool(re.match("^\d{11,}_\d{11,}_\d{4,}_\w{3,}(.)(\d{4,})?_\d{14}_\d*.\w{3,}$", s))
        #return bool(re.match("^\d{11,}_\d{11,}_\d{4,}_\w{3,}(.)?\d{4,}_\d{14}_\d*.\w{3,}$", s))
        

    # Validazione del flusso dati
    # dataItem: [ <XML-filename>; <flusso-xml>, <file-xsd> ]
    # Ritorna: (resultValidation, dataItem[0], dataItem[1], file_xsd, errors )
    # resultValidation: risultato Validazione (Boolean)
    # dataItem[0]: file xml (String)
    # dataItem[1]: oggetto XML (ElementTree)
    # file_xsd: file xsd (String)
    # errors: errore (String)
    def validate(self, dataItem):
        """
        Validazione del flusso dati
        """
        resultValidation = False
        file_xsd = None
        errors = ""

        fileName = os.path.basename(dataItem[0])
	#print ("full filename: ", dataItem[0])
        print ("Validazione file: ", fileName)

        resultValidation = self.validate_filename(fileName)

        if (resultValidation):
            # Verifica il codice Flusso
            file_xsd = self.get_filexsd(dataItem[1])
            #print("file_xsd", file_xsd)

            # Risultato della validazione:
            #resultValidation = Validator().validateFlusso(flussoXML=dataItem[1], file_xsd=file_xsd, file_xml=dataItem[0])
            cod_causale, motivazione, resultValidation = Validator().validateFlusso(flussoXML=dataItem[1], file_xsd=file_xsd, file_xml=dataItem[0])
            #print("result Validation:", cod_causale, motivazione, resultValidation)

            errors, resultValidation = self.checkErrors(xml = dataItem[1], file_xsd = file_xsd, resultValidation = resultValidation)
            #print("errors", errors)
        else: 
            errors = "Error not valid file name" 
            cod_causale = "904"
            motivazione = "Il nome del file non rispetta la struttura prevista"

        return (resultValidation, dataItem[0], dataItem[1], file_xsd, cod_causale, motivazione)



    def checkErrors(self, xml, file_xsd, resultValidation):
        if (xml.tag == "error"):
            return xml.text, False
             
        if (resultValidation == False):
            return "Error check file XSD: " + str(file_xsd), True

        return "", True

    def set_spark_context(self, app_name, master):
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
        sqlContext.setConf("spark.scheduler.mode", "FAIR")
        #sqlContext.setConf("spark.executor.instances", "5")
        
        return sc, sqlContext


    def decodeUTF8(self, item):
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
        sc, sqlCtx = self.set_spark_context(self.appName, self.master)

        logging.debug("Configurazione spark configuration completata")

        # recupera dalla directory 'workDirectory' la lista dei file
        # questa lista e' filtrata recuperando solamente i file con estensione .xml
        # verra' formattato il percorso del file nella seguente forma: file://<path-filename>.xml
        print("Carica file XML dalla directory: " + self.workDirectory)
        
        # Il numero minimo di partizioni dipende dalla dimensione della lista dei file in input. 
        # Se la lista e' maggiore di 10 viene impostato il numero di partizione pari a 10 altrimenti alla dimensione 
        # della lista
        #minPartitions = 10 if len(listFiles) >= 10 else len(listFiles)
        minPartitions = 30
        print ("Setting Partition: ", minPartitions)

        # Se l'utente non ha impostato il file da elaborare verra' prelevato il contenuto delle directory
        if (self.file == ""):
            if (self.anno and self.mese and self.distributore):
                print("Utilizzo ricerca anno, mese e distributore")
                listFiles = UtilFiles().getListOfFilesByAnnoMeseDistr(self.workDirectory, self.anno, self.mese, self.distributore, constants.EXT_XML)
                listFiles = sc.parallelize(listFiles, minPartitions).map(lambda f: "file://" + f)
            elif (self.anno and self.mese):
                print("Utilizzo ricerca anno e mese")
                listFiles = UtilFiles().getListOfFilesByAnnoMese(self.workDirectory, self.anno, self.mese, constants.EXT_XML)
                listFiles = sc.parallelize(listFiles, minPartitions).map(lambda f: "file://" + f)
            elif (self.anno):
                print("Utilizzo ricerca solo per anno")
                listFiles = UtilFiles().getListOfFilesByAnno(self.workDirectory, self.anno, constants.EXT_XML)
                listFiles = sc.parallelize(listFiles, minPartitions).map(lambda f: "file://" + f)
            else:	
                print("Ricerca file")
                #listFiles = UtilFiles().getListOfFiles(self.workDirectory, constants.EXT_XML)
                listFiles = UtilFiles().getListOfFiles2V_parallel2(sc, self.workDirectory, self.anno, self.mese, None, self.codFlusso)

        else: # altrimenti viene utilizzato il nome del file presente nella directory di lavoro impostata
            print("Elaborazione del file:", self.file)
            listFiles = sc.parallelize([ (self.workDirectory + self.file) ], 1)

        #listFiles.map(lambda x: (x,)).toDF().show(truncate = False)
        rddTempUDD = listFiles.flatMap(lambda d: d).map(lambda f: "file://" + f)
        
        # NOTA: La funzione wholeTextFile vuole in ingresso la lista dei file separati dal carattere ","
        # La collection "Lista File" viene convertita in una stringa, nella quale sono contenuti i nomi
        # dei files separate dalla "," (virgola)
        # eseguo la funzione di apertura e lettura dei file
        print("Creazione lista file da caricare")
        array_files = rddTempUDD.collect()
        self.log("Array generato dalla ricerca nella struttura cloud", array_files)

        if (self.codFlusso != ""):
            array_files = [collection_ele for collection_ele in array_files if self.codFlusso in collection_ele]
        collection_files = ",".join(f for f in array_files)
        self.log("Collection da inviare al wholeTextFiles: ", collection_files)

        print("Lettura file XML: items:", len(array_files))
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

        # Valida il flusso dati XML
        # la procedura di validazione utilizza la funzione "validate":
        # Il risultato della validazione viene generata una tabella nella quale e' contenuto il flag "valid"
        # se il valore del flag "valid" e' True il flusso XML e' valido altrimenti il valore e' False
        # valid; <filename>; <struct-xml>
        #logging.debug("Validazione file XML")
        dataValidate = data.map(self.validate).setName("Validazione")

        
        # Per ogni elemento nella variabile data viene eseguita l'elaborazione che recupera tutti
        # i dati necessari per l'archivizione

        # f[0] = stato validazione
        # d[0] = codice_servizio
        # d[1] = (resultValidation, dataItem[0], dataItem[1], file_xsd)
        #logging.info("Lettura e imporazione dei dati dai file XML")
        dataParsed = dataValidate.filter(lambda f: (f[0] == True and f[2] != None)) \
                                 .repartition(minPartitions) \
                                 .flatMap(lambda d: self.parse(d[2], dataElaborazione=dataElaborazione, file_xml = d[1])) \
                                 .map(lambda f: (f[0], f)).setName("lettura xml")
        
        # Caching rdd        
        self.log("Caching dataValidate", "")
        dataValidate.cache()
        
        self.elaborateFilesThead(dataValidate, 11, sc, sqlCtx)

        # Caching rdd        
        self.log("Caching dataParsed", "")
        dataParsed.cache()

        # Ogni elemento della tabella e' composto da (CodiceFlusso, array<dati>)
        # Vengono prelevati le varie collection filtrando per i vari flussi dati definite nell'attributo xml di ogni file

        if (self.codFlusso != ""):
            rdd = dataParsed.filter(lambda f: f[0] == self.codFlusso).map( lambda d: d[1]).setName("Filtro flusso " + str(self.codFlusso))
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


        dimensione_udf = udf(
            lambda namefile: os.path.getsize(namefile)
        )

        anno_udf = udf(
            lambda d:  os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][2:4]
        )

        mese_udf = udf(
            lambda d:  os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2]
        )

        tipo_file_udf = udf(
            lambda d: os.path.splitext(d)[1][1:]
        )

        anno_caricamento_udf = udf(
            lambda d: time.strftime('%Y',time.localtime(os.path.getmtime(d)))
        )

        mese_caricamento_udf = udf(
            lambda d: time.strftime('%m',time.localtime(os.path.getmtime(d)))
        )

        giorno_caricamento_udf = udf(
            lambda d: time.strftime('%d',time.localtime(os.path.getmtime(d)))
        )

        dimensione_udf = udf(
            lambda d: os.path.getsize(d)
        )

        # Recupero codice servizio esempio TMV
        tipo_servizio_udf = udf(
            lambda d: os.path.basename(d).split("_")[3] if len(os.path.basename(d).split("_")[3]) <= 3 else os.path.basename(d).split("_")[3][0:3]
        )

        tipo_flusso_udf = udf(
            lambda d: os.path.basename(d).split("_")[4] if (len(os.path.basename(d).split("_")) > 6 ) else (os.path.basename(d).replace(".xml","").replace(".",'') + ".xml").split("_")[3][3:7]
        )        

        piva_distributore_udf = udf(
            lambda d: os.path.basename(d).split("_")[0]
        )

        piva_udd_udf = udf(
            lambda d: os.path.basename(d).split("_")[1]
        )

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )

        annomese_rif_udf = udf(
	    lambda d: str(os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else "20" + os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2])        
	)


        
        schemaElaborateFiles = StructType([
            StructField("namefile", StringType(), True), 
            StructField("esito", StringType(), True), 
            StructField("t_cod_causale", StringType(), True), 
            StructField("t_motivazione", StringType(), True)
        ])

        dfElaborateFiles = sqlCtx.createDataFrame(rddElaborateFiles, schema=schemaElaborateFiles)

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfElaborateFiles = dfElaborateFiles.withColumn("namefile",  expr("regexp_replace(namefile, 'file:','')")  )
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_file",  n_id_file_udf(col('namefile')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_cmg",  lit(""))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file",  lit(col('namefile')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_stato_file",  lit("A"))
        dfElaborateFiles = dfElaborateFiles.withColumn("d_data_caricamento",  lit(dataElaborazione))

        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_file", tipo_file_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_anno_caricamento", anno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_mese_caricamento", mese_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_giorno_caricamento", giorno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_ammissibile", lit(col('esito')))
        #dfElaborateFiles = dfElaborateFiles.withColumn("t_cod_causale", lit("")) # Non inserire
        #dfElaborateFiles = dfElaborateFiles.withColumn("t_motivazione", lit("")) # Non inserire
        dfElaborateFiles = dfElaborateFiles.withColumn("t_digest", lit(""))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_dimensione", dimensione_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_inviato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_distributore", piva_distributore_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_udd", piva_udd_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipo_servizio_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_flusso", tipo_flusso_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("presente_db", lit("N"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_annomese_rif", annomese_rif_udf( col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_padre", lit(""))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_verificato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo", lit(""))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo_csv", lit(""))


       
        print ("Write: ", constants.PATHHDFS_ELEBORATEFILES)
        dfElaborateFiles.write.parquet(constants.PATHHDFS_ELEBORATEFILES, 'append')
        #dfElaborateFiles.write.partitionBy(constants.PARTITIONLIST_TMV).parquet(constants.PATHDFS_TMV, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_ELABORATEFILES)

        #Stampa il numero di file elaborati
        #print ("Number files Elaborated: ", rddElaborateFiles.count())
        #print ("Number files Failed: ", rddElaborateFiles.filter(lambda d: d[1] == False).count())
        sc.setLocalProperty("spark.scheduler.pool", None)
