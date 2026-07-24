import logging
import os
import constants

import xml.etree.ElementTree as ET
import datetime

from Data import Data
from flussi.flussoIM1 import FlussoIM1
from validator import Validator
from functions.util_file import UtilFiles
import re

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import expr, lit, col
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import hashlib


class Ingestion:
    # Construttore
    def __init__(self, directory_name, mode, cod_flusso, file, anno, mese, giorno, distributore):
        self.master = mode
        self.appName = "Ingestione " + constants.APPNAME
        self.workDirectory = directory_name
        self.codFlusso = cod_flusso
        self.file = file
        self.anno = anno
        self.mese = mese
        self.giorno = giorno
        self.distributore = distributore
        self.debug = False

    # Esegue il parsing xml dei dati
    def parse(self, root, data_elaborazione, file_xml):
        """
            Lettura e parsing dei file XML
        """
        #print("parse:{}".format(file_xml))
        data = Data(root, file_xml)
        result = data.toList(dataElaborazione=data_elaborazione)

        return result

    def get_filexsd(self, flusso):
        """
            Dato il flusso ritorna il nome del file XSD per la validazione
            Se il flusso non esiste viene ritornato un nome file non valido
        """
        codice_servizio = flusso.get(constants.CODSERVIZIO_STR)
        codice_flusso = flusso.get(constants.CODFLUSSO_STR)

        file_xsd = ""
        if codice_servizio == "IM1":
            file_xsd = FlussoIM1('IM1').getFileXSD(codice_servizio, codice_flusso)

        if file_xsd == "" or file_xsd is None:
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
        return bool(re.match("^\\d{11,16}_\\d{11,16}_\\d{4,}_\\w{3,}(.)(\\d{4,})?_\\d{14}_\\d*.\\w{3,}$", s))
        # return bool(re.match("^\d{11,}_\d{11,}_\d{4,}_\w{3,}(.)?\d{4,}_\d{14}_\d*.\w{3,}$", s))

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
        result_validation = True
        file_xsd = None

        errors = "Error not valid file name"
        cod_causale = "904"
        motivazione = "Il nome del file non rispetta la struttura prevista"

        file_name = os.path.basename(dataItem[0])
        file_xsd = self.get_filexsd(dataItem[1])

        # Validazione della cartella
        directory_name = os.path.dirname(dataItem[0])
        name_directory_name_prev = os.path.basename(directory_name)
        #if len(name_directory_name_prev) != 4:
        #    cod_causale, motivazione, result_validation = (
        #        "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo", False)
        #    return result_validation, dataItem[0], dataItem[1], file_xsd, cod_causale, motivazione

        # Risultato della validazione:
        cod_causale, motivazione, result_validation = Validator().validateFlusso(
            flussoXML=dataItem[1],
            file_xsd=file_xsd,
            file_xml=dataItem[0]
        )

        if result_validation:
            if cod_causale is not None:
                flusso = Data.getFlusso(cod_servizio, None)
                if not flusso.isValidFile(file_name):
                    errors = "Error not valid file name"
                    cod_causale = "904"
                    motivazione = "Il nome del file non rispetta la struttura prevista"
                    result_validation = False
                else:
                    errors, result_validation = self.checkErrors(
                        xml=dataItem[1],
                        file_xsd=file_xsd,
                        resultValidation=result_validation
                    )
        else:
            errors = "Error not valid file name"
            # cod_causale = "904"
            # motivazione = "Il nome del file non rispetta la struttura prevista"

        #print(result_validation, cod_causale, motivazione, file_xsd, dataItem)
        return result_validation, dataItem[0], dataItem[1], file_xsd, cod_causale, motivazione

    def checkErrors(self, xml, file_xsd, resultValidation):
        """
            Verifica il file xml mediante il file xsd
            Ritorna
                Messaggio di errore - string
                errore - boolean
        """
        if xml.tag == "error":
            return xml.text, False

        if not resultValidation:
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

        sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
        sqlContext.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        sqlContext.setConf("spark.sql.parquet.output.committer.class",
                           "org.apache.spark.sql.parquet.ParquetOutputCommitter")
        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        sqlContext.setConf("hive.exec.max.dynamic.partitions", "1000000000")
        sqlContext.setConf("spark.scheduler.mode", "FAIR")

        return sc, sqlContext

    def decodeUTF8(self, item):
        """
        Decodifica il flusso dati nel formato utf-8
        """
        val = item.value

        try:
            return ET.fromstring(val.encode('utf-8'))
        except Exception as e:
            try:
                return ET.fromstring(val.encode('utf-8')[:1])
            except Exception as e2:
                try:
                    return ET.fromstring(val.encode('utf-8')[:2])
                except Exception as e3:
                    return ET.fromstring("<error>Decoding UTF8: " + str(e3) + "</error>")

        return ET.fromstring("<error>No decode</error>")

    def start(self):
        logging.debug("Start Ingestion IM1")

        logging.debug("Configurazione spark configuration")
        # sc, sqlCtx = self.set_spark_context(self.appName +  " " + self.codFlusso, self.master)
        sc, sqlCtx = self.set_spark_context(
            self.appName + " " + self.codFlusso +
            (" Anno:" + self.anno if self.anno is not None else "") +
            (" Mese:" + self.mese if self.mese is not None else "") +
            (" Giorno:" + self.giorno if self.giorno is not None else ""),
            self.master
        )

        logging.debug("Configurazione spark configuration completata")

        # recupera dalla directory 'workDirectory' la lista dei file
        # questa lista e' filtrata recuperando solamente i file con estensione .xml
        # verra' formattato il percorso del file nella seguente forma: file://<path-filename>.xml
        print("Carica file XML dalla directory (IM1): " + self.workDirectory)

        # Il numero minimo di partizioni dipende dalla dimensione della lista dei file in input.
        # Se la lista e' maggiore di 10 viene impostato il numero di partizione pari a 10 altrimenti alla dimensione
        # della lista
        # minPartitions = 10 if len(listFiles) >= 10 else len(listFiles)
        minPartitions = 215
        print("Setting Partition: ", minPartitions)

        # Se l'utente non ha impostato il file da elaborare verra' prelevato il contenuto delle directory
        if self.file == "":
            # Versione 2
            listFiles = UtilFiles().getListOfFiles2V_parallel2(
                sc,
                self.workDirectory,
                self.anno, self.mese, self.giorno,
                self.codFlusso,
                self.distributore
            )
          
        else:  # Utilizzo query
            print("Estrazione dati da query:", self.file)
            with open(self.file, 'r') as content_file:
                query_tmp = content_file.read()

            print("Query: ", query_tmp)
            listFiles = sqlCtx.sql(query_tmp)

        rddTempUDD = listFiles.flatMap(lambda d: d).map(lambda f: "file://" + f)

        # NOTA: La funzione wholeTextFile vuole in ingresso la lista dei file separati dal carattere ","
        # La collection "Lista File" viene convertita in una stringa, nella quale sono contenuti i nomi
        # dei files separate dalla "," (virgola)
        # eseguo la funzione di apertura e lettura dei file
        print("Creazione lista file da caricare")
        array_files = rddTempUDD.collect()
        self.log("Array generato dalla ricerca nella struttura cloud", array_files)

        if self.codFlusso != "":
            array_files = [collection_ele for collection_ele in array_files if self.codFlusso in collection_ele]
        collection_files = ",".join(f for f in array_files)
        self.log("Collection da inviare al wholeTextFiles: ", collection_files)

        print("Lettura file XML: items:", len(array_files))
        if len(array_files) == 0:
            return

        rddValidAll = sc.wholeTextFiles(collection_files, minPartitions=minPartitions, use_unicode=False) \
            .setName("Acquisizione files di misurazione")

        vflusso = sqlCtx.createDataFrame(rddValidAll, ['file', 'value'])

        # Calcolo la data Elaborazione.
        # Mediante la funzione datetime.now recupero la data di sistema nel formato ISO
        dataElaborazione = datetime.datetime.now().isoformat()
        # logging.debug("Set Data di elaborazione: " + dataElaborazione)

        # Parsing dei file xml
        # Per ogni file presente nella collection viene eseguito il parsing
        # i dati sono conservati all'interno della variabile "data" nel formato <filename>, <struct-xml>
        # logging.debug("Parsing file XML")
        data = vflusso.map(lambda d: (d.file, self.decodeUTF8(d))).setName("parsing files xml")

        # print("data parsing xml:",data.count())

        # Valida il flusso dati XML
        # la procedura di validazione utilizza la funzione "validate":
        # Il risultato della validazione viene generata una tabella nella quale e' contenuto il flag "valid"
        # se il valore del flag "valid" e' True il flusso XML e' valido altrimenti il valore e' False
        # valid; <filename>; <struct-xml>
        # logging.debug("Validazione file XML")
        dataValidate = data.map(lambda d: self.validate(d, self.codFlusso)).setName("Validazione")
        # Caching rdd
        # dataValidate.cache()

        # dataValidate.foreach(lambda d: self.stampa(d))
        # Per ogni elemento nella variabile data viene eseguita l'elaborazione che recupera tutti
        # i dati necessari per l'archivizione

        # f[0] = stato validazione
        # d[0] = codice_servizio
        # d[1] = (resultValidation, dataItem[0], dataItem[1], file_xsd)
        # logging.info("Lettura e imporazione dei dati dai file XML")
        # dataParsed = dataValidate.filter(lambda f: (f[0] == True and f[2] != None)) \
        dataParsed = dataValidate.filter(lambda f: (f[0] == True)) \
            .flatMap(lambda d: self.parse(d[2], data_elaborazione=dataElaborazione, file_xml=d[1])) \
            .map(lambda f: (f[0], f)).setName("lettura xml")

        dataParsed.cache()
        # print("Parse dataValidate", dataParsed.count())
        # Recupero elementi non validi
        dataParsed_notValid = dataValidate.filter(lambda f: (f[0] == False))

        #dataParsed_valid_cnt=dataParsed.count()
        #dataParsed_notValid_cnt = dataParsed_notValid.count()

        dataParsed_notValid.cache()
        dataParsed.cache()


        self.elaborateFilesThead(dataValidate, 11, sc, sqlCtx)
        self.notValid(dataParsed_notValid, sqlCtx, 11, sc)

	"""
	rdd = dataParsed.filter(lambda f: f[0] == self.codFlusso)
        rdd = rdd.map(lambda d: d[1]).setName("Filtro flusso " + str(self.codFlusso))
	th_dett = threading.Thread(target=FlussoIM1('IM1').write, args=(rdd, 1, sc, sqlCtx,))
	th_dett.start()

        self.elaborateFilesThead(dataValidate, 11, sc, sqlCtx)
        self.notValid(dataParsed_notValid, sqlCtx, 11, sc)

        th_dett.join()

	return
	"""

        # Vengono prelevati le varie collection filtrando per i vari flussi dati definite nell'attributo xml di ogni file
        rdd = dataParsed.filter(lambda f: f[0] == self.codFlusso)
        rdd = rdd.map(lambda d: d[1]).setName("Filtro flusso " + str(self.codFlusso))
        #print("flusso count: ", rdd.count())
        self.getWorker(codiceFlusso=self.codFlusso)(rdd, 1, sc, sqlCtx)

        dataParsed.unpersist()
        dataValidate.unpersist()
        return

    def log(self, text, message_obj):
        """
        Log
        """
        if self.debug:
            print(text, message_obj)

    def getWorker(self, codiceFlusso):
        """
        Dato il codiceFlusso ritorna la funzione associato al flusso
        """
        if codiceFlusso == constants.CODSERVIZIO_IM1:
            return FlussoIM1('IM1').write

    def notValid(self, rdd, sqlCtx, pool_id, sc):
        """
           Scrive nella tabella i record non validi
           RDD contiene questi attributi: result, filename, xml, file_xsd, cod_causale, motivazione
           sqlCtx: sql context
        """
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        rdd_notValid = rdd.map(lambda d: (d[0], d[1], d[3], d[4], d[5]))

        # Dimensione del file
        dimensione_udf = udf(
            lambda namefile: os.path.getsize(namefile)
        )

        # Anno dal nome del file
        anno_udf = udf(
            lambda d: (
                os.path.basename(d).split("_")[2][0:4]
                if (len(os.path.basename(d).split("_")[2]) > 4)
                else os.path.basename(d).split("_")[2][2:4])
            if len(os.path.basename(d).split("_")) >= 3
            else ""
        )

        # Mese dal nome del file
        mese_udf = udf(
            lambda d: (
                os.path.basename(d).split("_")[2][4:6]
                if (len(os.path.basename(d).split("_")[2]) > 4)
                else os.path.basename(d).split("_")[2][0:2])
            if len(os.path.basename(d).split("_")) >= 3
            else ""
        )

        # Tipo del file esempio XML
        tipo_file_udf = udf(
            lambda d: (os.path.splitext(d)[1][1:]) if len(os.path.basename(d).split("_")) >= 2 else ""
        )

        # Anno file nel cloud
        anno_caricamento_udf = udf(
            lambda d: d.split("/")[8]
        )

        # Mese file nel cloud
        mese_caricamento_udf = udf(
	    lambda d: str(d.split("/")[9][0:2]).rjust(2, '0')
            #lambda d: d.split("/")[9][0:2]
        )

        # Giorno file nel cloud
        giorno_caricamento_udf = udf(
	    lambda d: str(d.split("/")[9][2:4]).rjust(2, '0')
            #lambda d: d.split("/")[9][2:4]
        )

        # Dimensione file
        dimensione_udf = udf(
            lambda d: os.path.getsize(d) if os.path.isfile(d) else -1
        )

        # Recupero codice servizio esempio TMV
        tipo_servizio_udf = udf(
            lambda d: (
                os.path.basename(d).split("_")[3]
                if len(os.path.basename(d).split("_")[3]) <= 3
                else os.path.basename(d).split("_")[3][0:3])
            if len(os.path.basename(d).split("_")) >= 4 else ""
        )

        # Recupero codice flusso esempio 0350
        tipo_flusso_udf = udf(
            lambda d: (
                os.path.basename(d).split("_")[4]
                if (len(os.path.basename(d).split("_")) > 6)
                else (os.path.basename(d).replace(".xml", "").replace(".", '') + ".xml").split("_")[3][3:7])
            if len(os.path.basename(d).split("_")) >= 5 else ""
        )

        # Genera id file
        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # Anno Mese di riferimento ( Anno + Mese ) dal nome file
        annomese_rif_udf = udf(
            lambda d: (str(
                os.path.basename(d).split("_")[2][0:4]
                if (len(os.path.basename(d).split("_")[2]) > 4)
                else "20" + os.path.basename(d).split("_")[2][2:4]) + str(
                os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else
                os.path.basename(d).split("_")[2][0:2])) if len(os.path.basename(d).split("_")) >= 3 else ""
        )

        piva_distributore_udf = udf(
            lambda d: (d.split("/")[-4]).split("_")[1] if len(d.split("/")) >= 1 else ""
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

        dfElaborateFiles = dfElaborateFiles.withColumn("filename", expr("regexp_replace(filename, 'file:','')"))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_file", n_id_file_udf(col('filename')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file", lit(col('filename')))
        dfElaborateFiles = dfElaborateFiles.withColumn("d_data_caricamento", lit(dataElaborazione))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_anno_caricamento", anno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_mese_caricamento", mese_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_giorno_caricamento", giorno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_dimensione", dimensione_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_distributore", piva_distributore_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_flusso", tipo_flusso_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipo_servizio_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_file", tipo_file_udf(col('t_nome_file')))

        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file", expr("regexp_replace(filename, '/isilonshare_gas', '')"))
        #dfElaborateFiles.show(truncate = False)

        #constants.PATHHDFS_IM_FILES = "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_file_im1_p"
        PARTITIONLIST = ["t_anno_caricamento","t_mese_caricamento"]
        print("Write to path: {} \t partition list: {}: ".format(constants.PATHHDFS_IM_FILES, PARTITIONLIST))
        dfElaborateFiles.write.partitionBy(PARTITIONLIST).parquet(constants.PATHHDFS_IM_FILES, 'append')

        print("Refresh tabella: {}".format(constants.CMD_REFRESH_ELABORATEFILES_IM1))
        sqlCtx.sql(constants.CMD_REFRESH_ELABORATEFILES_IM1)

        sc.setLocalProperty("spark.scheduler.pool", None)

    # TODO
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

        rddElaborateFiles = rdd.map(lambda d: (d[1], d[0], d[4], d[5]))

        if self.codFlusso != "":
            rddElaborateFiles = rddElaborateFiles.filter(lambda r: self.codFlusso in r[0])

        # Dimensione del file
        dimensione_udf = udf(
            lambda namefile: os.path.getsize(namefile)
        )

        # Anno dal nome del file
        anno_udf = udf(
            lambda d: (os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else
                       os.path.basename(d).split("_")[2][2:4]) if len(os.path.basename(d).split("_")) >= 3 else ""
        )

        # Mese dal nome del file
        mese_udf = udf(
            lambda d: (os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else
                       os.path.basename(d).split("_")[2][0:2]) if len(os.path.basename(d).split("_")) >= 3 else ""
        )

        # Tipo del file esempio XML
        tipo_file_udf = udf(
            lambda d: (os.path.splitext(d)[1][1:]) if len(os.path.basename(d).split("_")) >= 2 else ""
        )

        # Anno file nel cloud
        anno_caricamento_udf = udf(
            lambda d: d.split("/")[8]
            # lambda d: os.path.basename(d).split("_")[4][0:4]
            # lambda d: time.strftime('%Y',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        )

        # Mese file nel cloud
        mese_caricamento_udf = udf(
	    lambda d: str(d.split("/")[9][0:2]).rjust(2, '0')
            #lambda d: d.split("/")[9][0:2]
            # lambda d: os.path.basename(d).split("_")[4][4:6]
            # lambda d: time.strftime('%m',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        )

        # Giorno file nel cloud
        giorno_caricamento_udf = udf(
	    lambda d: str(d.split("/")[9][2:4]).rjust(2, '0')
            #lambda d: d.split("/")[9][2:4]
            # lambda d: os.path.basename(d).split("_")[4][6:8]
            # lambda d: time.strftime('%d',time.localtime(os.path.getmtime(d))) if os.path.isfile(d) else "EE"
        )

        # Dimensione file
        dimensione_udf = udf(
            lambda d: os.path.getsize(d) if os.path.isfile(d) else -1
        )

        # Recupero codice servizio esempio TMV
        tipo_servizio_udf = udf(
            lambda d: (os.path.basename(d).split("_")[3] if len(os.path.basename(d).split("_")[3]) <= 3 else
                       os.path.basename(d).split("_")[3][0:3]) if len(os.path.basename(d).split("_")) >= 4 else ""
        )

        # Recupero codice flusso esempio 0350
        tipo_flusso_udf = udf(
            lambda d: (os.path.basename(d).split("_")[4] if (len(os.path.basename(d).split("_")) > 6) else
                       (os.path.basename(d).replace(".xml", "").replace(".", '') + ".xml").split("_")[3][3:7]) if len(
                os.path.basename(d).split("_")) >= 5 else ""
        )

        # Genera id file
        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # Anno Mese di riferimento ( Anno + Mese ) dal nome file
        annomese_rif_udf = udf(
            lambda d: (str(
                os.path.basename(d).split("_")[2][0:4]
                if (len(os.path.basename(d).split("_")[2]) > 4)
                else "20" + os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6]
                                                                          if (
                    len(os.path.basename(d).split("_")[2]) > 4)
                                                                          else os.path.basename(d).split("_")[2][
                                                                               0:2])) if len(
                os.path.basename(d).split("_")) >= 3 else ""
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

        schemaElaborateFiles = StructType([
            StructField("filename", StringType(), True),
            StructField("esito", StringType(), True),
            StructField("t_cod_causale", StringType(), True),
            StructField("t_motivazione", StringType(), True)
        ])

        rddElaborateFiles = rddElaborateFiles.filter(lambda d: d[1] == True)
        dfElaborateFiles = sqlCtx.createDataFrame(rddElaborateFiles, schema=schemaElaborateFiles)

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfElaborateFiles = dfElaborateFiles.withColumn("filename", expr("regexp_replace(filename, 'file:','')"))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_file", n_id_file_udf(col('filename')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file", lit(col('filename')))
        dfElaborateFiles = dfElaborateFiles.withColumn("d_data_caricamento", lit(dataElaborazione))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_anno_caricamento", anno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_mese_caricamento", mese_caricamento_udf(col('t_nome_file')))
	dfElaborateFiles = dfElaborateFiles.withColumn("t_giorno_caricamento", giorno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_dimensione", dimensione_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_distributore", piva_distributore_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_flusso", tipo_flusso_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipo_servizio_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_file", tipo_file_udf(col('t_nome_file')))
	dfElaborateFiles = dfElaborateFiles.withColumn("b_ammissibile", lit("True")) 

        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file", expr("regexp_replace(filename, '/isilonshare_gas', '')"))
        #dfElaborateFiles.show(truncate=False)
        #constants.PATHHDFS_IM_FILES = "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_file_im1_p"
        PARTITIONLIST = ["t_anno_caricamento", "t_mese_caricamento"]
        print("Write to path: {} \t partition list: {}: ".format(constants.PATHHDFS_IM_FILES, PARTITIONLIST))
        dfElaborateFiles.write.partitionBy(PARTITIONLIST).parquet(constants.PATHHDFS_IM_FILES, 'append')

        print("Refresh tabella: {}".format(constants.CMD_REFRESH_ELABORATEFILES_IM1))
        sqlCtx.sql(constants.CMD_REFRESH_ELABORATEFILES_IM1)

        # Stampa il numero di file elaborati
        sc.setLocalProperty("spark.scheduler.pool", None)


