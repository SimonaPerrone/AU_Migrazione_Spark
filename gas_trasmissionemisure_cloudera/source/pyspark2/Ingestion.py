import datetime
import os
import xmlschema

import codecs
import hashlib
import threading
import re

import xml.etree.ElementTree as ET

import py4j
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import expr, lit, udf, col
from pyspark.sql.types import *

from selectorFlusso import select_flusso


class Ingestion:
    def __init__(self, params):
        self.params = params
        self.minPartitions = 215
        self.dataElaborazione = datetime.datetime.now().isoformat()

        #self.flusso_instance = select_flusso(cod_servizio=self.params.flusso, cod_flusso=None)
        #self.file_xsd = self.flusso_instance.get_xsd_file(self.params.flusso)

    def print_log(self, message):
        if self.params.verbose:
            print("{}".format(message))

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
        # sqlContext.setConf("spark.executor.instances", "5")

        return sc, sqlContext

    def decodeUTF8(self, item):
        """
        Decodifica il flusso dati nel formato utf-8
        """
        # return ET.fromstring(item.value)
        val = item.value
        filename=item.file
        # print("Decode UTF8: ", val)

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

    def valida_nomenclatura(self, file, flusso):
        result_validation = True
        cod_causale = ""
        motivazione = ""
        filename = os.path.basename(file)

        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_"+flusso+"((\\d{4})|\\.\\d{4}|\\_\\d{4})_\\d*_\\d{1,}.\\w{3,}$"
        #pattern = "^\\w*"+flusso+"\\w*.\\w{3}$"
        #self.print_log("pattern validazione: {}".format(pattern))
        result_validation = bool(re.match(pattern, filename))
        #self.print_log("Validazione nomenclatura file: {}".format(filename))

	result_validation = flusso in filename

        if not result_validation:
            errors = "Error not valid file name: {}".format(filename)
            cod_causale = "903"
            motivazione = "Il codice del flusso non e' previsto/coerente"
            self.print_log(errors)

        return result_validation, cod_causale, motivazione

    def valida_flussoXSD(self, file, content_xml, flusso):
        result_validation = True
        cod_causale = ""
        motivazione = ""
        file_xsd = select_flusso(file).get_xsd_file(flusso)

        #self.print_log("Validazione xsd file: {}\nxsd:".format(file, file_xsd))

        my_schema = xmlschema.XMLSchema(file_xsd)
        result = my_schema.is_valid(content_xml)
        filename = os.path.basename(file.replace("file:/", ""))

        #self.print_log("Validazione xsd result: {} -- file: {}".format(result, file))

        if not result:
            return False, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati"

        codice_servizio = content_xml.get("cod_servizio")
        cod_flusso = content_xml.get("cod_flusso")

        if not cod_flusso or not codice_servizio:
            self.print_log("EE: Validazione xsd Codice Flusso o Codice Servizio non valorizzati -- file: {}".format(file))
            return False, "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo"

        isValidFile002 = cod_flusso in filename
        if not isValidFile002:
            self.print_log("EE: Validazione xsd Codice Servizio non presente nel nome file  -- file: {}".format(file))
            return False, "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo (2)"

        isValidFile001 = os.path.splitext(filename)[1].upper() == ".XML"
        if not isValidFile001:
            self.print_log("EE: Validazione xsd Nome file estensione non valida (XML mancante) -- file: {}".format(file))
            return False, "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo (3)"

        return result_validation, cod_causale, motivazione

    def parse(self, filename, content_xml):
        flusso_instance = select_flusso(filename)
        return (flusso_instance.getItems(filename, content_xml), flusso_instance.getCodFlusso())

    def start(self):
        print("Start Ingestion")
        app_name = "Ingestion flusso {} {}".format(
            self.params.flusso,
            "- Test" if self.params.enableTestMode else ""
        )

        self.print_log("Configurazione Context Spark")
        sc, sqlCtx = self.set_spark_context(app_name=app_name, master=self.params.mode)
        self.print_log("Context Spark Creato")

        self.print_log("Recupero query")
        query_tmp = self.params.query
        if self.params.query_file is not None:
            self.print_log("Estrazione dati da query: {}".format(self.params.query_file))
            with open(self.params.query_file, 'r') as content_file:
                query_tmp = content_file.read()

        self.print_log("Query: {}".format(query_tmp))
        listFiles = sqlCtx.sql(query_tmp)
        rdd_list_files = listFiles.flatMap(lambda d: d).map(lambda f: "file://" + f)

        array_files = rdd_list_files.collect()
        if self.params.flusso:
            array_files = [collection_ele for collection_ele in array_files if self.params.flusso in collection_ele]

        if len(array_files) == 0:
            self.print_log("Attenzione lista degli elementi e' vuota (exit): {}".format(len(array_files)))
            return

        collection_files = ",".join(f for f in array_files)
        self.print_log("Numero di file da caricare usando wholeTextFiles: {}".format(len(array_files)))

        self.print_log("Lettura File")
        rddValidAll = sc.wholeTextFiles(collection_files, minPartitions=self.minPartitions, use_unicode=False) \
            .setName("Lettura file")
        self.print_log("Lettura File Completata")

        self.print_log("Creazione Dataframe")
        vflusso = sqlCtx.createDataFrame(rddValidAll, ['file', 'value'])

        self.print_log("Decode in UTF8")
	data = vflusso.map(lambda d: (d.file, self.decodeUTF8(d))).setName("parsing files xml")
 	
        self.print_log("Validazione nomenclatura file")
        # Validazione nomenclatura file
        # f[0] = {
        #   stato della validazione (true valido, false non valido)
        #   codice errore
        #   motivazione errore
        # }
        # f[1] = nome file
        # f[2] = contenuto file
        data_nom = data.map(lambda d: (self.valida_nomenclatura(d[0], self.params.flusso), d[0], d[1]))

        self.print_log("Validazione XSD")
        # Validazione XSD
        # f[0] = {
        #   stato della validazione (true valido, false non valido)
        #   codice errore
        #   motivazione errore
        # }
        # f[1] = nome file
        # f[2] = contenuto file
        data_xsd = data_nom.filter(lambda f: f[0][0])\
            .map(lambda d: (self.valida_flussoXSD(d[1], d[2], self.params.flusso), d[1], d[2]))\
            .setName("Validazione XSD")
	data_xsd.cache()

	self.print_log("Filtra Rdd validi")
        rddValid = data_xsd.filter(lambda f: f[0][0]).setName("Creazione RDD valido")

	self.print_log("Filtra Rdd non validi")
        rddNotValid = data_nom.filter(lambda f: not f[0][0]).union(data_xsd.filter(lambda f: not f[0][0]))\
            .setName("Creazione rdd da scarto")

        # Parse file XML
        #rdd_dettaglio = rddValid.flatMap(lambda d: self.parse(d[1], d[2])).setName("Get Items")
        rdd_dettaglio = rddValid.map(lambda d: self.parse(d[1], d[2])).setName("Get Items")
        arr_cod = rdd_dettaglio.groupBy(lambda d: d[1]).map(lambda d: d[0]).distinct().collect()

        for cod_flusso in arr_cod:
            flusso_instance = select_flusso(cod_flusso)
            #rdd_to_write = rdd_dettaglio.filter(lambda d: d[1] == cod_flusso).flatMap(lambda d: d[0]).collect()
            rdd_to_write = rdd_dettaglio.filter(lambda d: d[1] == cod_flusso).flatMap(lambda d: d[0])
            # Scrittura sulla tabella Dettaglio
            flusso_instance.write(rdd_to_write, sc, sqlCtx, self.params.enableTestMode)


        # Scrittura sulla tabella Principale
        rddMain = rddValid.union(rddNotValid).setName("Creazione RDD Main")
        self.write(rddMain, sc, sqlCtx, self.params.enableTestMode)

        return

    def estrai_cod_servizio(self, filename, cod_servizio):
        d = os.path.basename(filename)
        print("filename: {}".format(d))
        cod_servizio_return = "EE"
        cod_flusso_return = "EE"
        annomese_rif_return = "Nan"
        str_servizio = ""
        splited = os.path.basename(d).split("_")
        index = 0
        is_present = False
        for item in splited:
            if cod_servizio in item:
                str_servizio = item
                is_present = True
                break
            else:
                index=index+1
        if is_present:
            if str_servizio == cod_servizio:
                cod_servizio_return = str_servizio
                cod_flusso_return = splited[index+1]
            else:
                cod_servizio_return = str_servizio[0:3]
                cod_flusso_return = str_servizio[3:len(str_servizio)]

            if index > 2:
                annomese_rif_return = splited[index-1]

        cod_flusso_return = cod_flusso_return.replace(".", "")
        return cod_servizio_return, cod_flusso_return, annomese_rif_return

    def write(self, rdd, sc, sqlCtx, test_mode_enabled):
        """
            # f[0] = {
            #   stato della validazione (true valido, false non valido)
            #   codice errore
            #   motivazione errore
            # }
            # f[1] = nome file
            # f[2] = contenuto file

        :param rdd:
        :param sc:
        :param sqlCtx:
        :return:
        """

        PATH_HDFS = "/user/hive/warehouse/au.db/misure_gas_au/cmg_gas/prt_cmg_file_backeted_p{}".format("_test" if test_mode_enabled else "") 
        CMD_REFRESH = "MSCK REPAIR TABLE cmg_gas.prt_cmg_file_backeted_p{}".format("_test" if test_mode_enabled else "")
        PARTITIONS = ["t_anno_caricamento", "t_mese_caricamento", "t_tipo_servizio"]

        rdd = rdd.map(lambda d: (d[1], d[0][0], d[0][1], d[0][2]))

        schema = StructType([
            StructField("namefile", StringType(), True),
            StructField("esito", StringType(), True),
            StructField("t_cod_causale", StringType(), True),
            StructField("t_motivazione", StringType(), True)
        ])

        # Genera id file
        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
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
        )

        # Giorno file nel cloud
        giorno_caricamento_udf = udf(
            lambda d: str(d.split("/")[9][2:4]).rjust(2, '0')
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
            lambda x: "EE" if x is None else x
        )

        # Dimensione file
        dimensione_udf = udf(
            lambda d: os.path.getsize(d) if os.path.isfile(d) else -1
        )

        # Recupero codice servizio esempio TMV
        tipo_servizio_udf = udf(
            lambda d:  self.estrai_cod_servizio(d, self.params.flusso)[0]
	)

        # Recupero codice flusso esempio 0350
        tipo_flusso_udf = udf(
            lambda d: self.estrai_cod_servizio(d, self.params.flusso)[1] 
        )

        # Anno Mese di riferimento ( Anno + Mese ) dal nome file
        annomese_rif_udf = udf(
            lambda d: self.estrai_cod_servizio(d, self.params.flusso)[2]
        )

        dfElaborateFiles = sqlCtx.createDataFrame(rdd, schema=schema)
        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfElaborateFiles = dfElaborateFiles.withColumn("namefile",  expr("regexp_replace(namefile, 'file:','')"))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_file", n_id_file_udf(col('namefile')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_cmg", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file", lit(col('namefile')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_stato_file", lit("A"))
        dfElaborateFiles = dfElaborateFiles.withColumn("d_data_caricamento", lit(dataElaborazione))

        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_file", tipo_file_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_anno_caricamento", anno_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_mese_caricamento", mese_caricamento_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_giorno_caricamento", giorno_caricamento_udf(col('t_nome_file')))

        dfElaborateFiles = dfElaborateFiles.withColumn("b_ammissibile", ammissibile_udf(col("t_cod_causale")))

        dfElaborateFiles = dfElaborateFiles.withColumn("t_digest", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_dimensione", dimensione_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_inviato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_distributore", piva_distributore_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_piva_udd", piva_udd_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipo_servizio_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_flusso", tipo_flusso_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("presente_db", lit("N"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_annomese_rif", annomese_rif_udf(col('t_nome_file')))
        dfElaborateFiles = dfElaborateFiles.withColumn("n_id_padre", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("b_verificato", lit("Y"))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_corpo_csv", lit(None).cast(StringType()))
        dfElaborateFiles = dfElaborateFiles.withColumn("t_tipo_servizio", tipoflusso_udf(col('t_tipo_servizio')))

        # Per eliminare il path isilonshare_gas
        dfElaborateFiles = dfElaborateFiles.withColumn("t_nome_file", expr("regexp_replace(namefile, '/isilonshare_gas', '')"))

        self.print_log("Write: {}".format(PATH_HDFS))
        dfElaborateFiles.write.partitionBy(PARTITIONS).parquet(PATH_HDFS, 'append')
        self.print_log("Refresh tabella: {}".format(CMD_REFRESH))
        sqlCtx.sql(CMD_REFRESH)


