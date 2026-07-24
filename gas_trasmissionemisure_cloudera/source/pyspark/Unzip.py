import io
import os
import constants
import shutil

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext

import zipfile
import logging
from functions.util_file import UtilFiles 
from shutil import copyfile

class Unzip:
    def __init__(self, file, mode, giorno, mese, anno, codiceFlusso, distr):
        self.master = mode
        self.appName = "Decompressione " + constants.APPNAME
        self.giorno = giorno
        self.anno = anno
        self.mese = mese
        self.codiceFlusso = codiceFlusso
        self.distr = distr

        self.workDirectory = file

    # Estrae dal file zip i files nella directory corrente
    def zip_extract(self, x, workdir, ext_filename):
        """
        Unzip dai file zip i file xml
        """
        err_str = ""
        
        baseDir = os.path.dirname(x) + "/"
        baseDir = baseDir.replace(workdir,"")

        home = constants.DIRECTORY_HOME
        directoryXML = constants.TEMPDIRECTORY_XML
        
        directory = directoryXML + baseDir
        print("Elaborazione file:", x)	

        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError as exception:  # Python >2.5
            err_str = "{0} {1}".format(
                type(exception), 
                " ".join([arg for arg in exception.args]))

            return (False, constants.EXT_ZIP, x, err_str, ext_filename)	

        # Unzip file
        try:    
            file_obj = zipfile.ZipFile(x, "r")
            file_obj.extractall(directory)
        except Exception as exception:
            err_str = "{0} {1}".format(
                type(exception), 
                " ".join([arg for arg in exception.args]))
            print ("Error:" , err_str)
            #print("********* Error zip file:", x)
            return (False, constants.EXT_ZIP, x, err_str, ext_filename)	

        
        return (True,constants.EXT_ZIP, x, err_str, ext_filename)	
        #os.remove(destFile)

    def xml_extract(self, x, workdir, ext_filename):
        """
        Copia i file xml dalla cartella sorgente nella cartella di lavoro
        """
        err_str = ""

        baseDir = os.path.dirname(x) + "/"
        baseDir = baseDir.replace(workdir,"")

        home = constants.DIRECTORY_HOME
        directoryXML = constants.TEMPDIRECTORY_XML
        
 
        directory = directoryXML + baseDir 
        files_result = directory + os.path.basename(x)

        print("Elaborazione file:", x)	
        #print("directoryXML", directoryXML)	
        #print("baseDir", baseDir)	
        #print("workdir", workdir)	
        #print("File xml", files_result)	
        #print("File xml", files_result)	
        #print("os.path.exists(directory)", os.path.exists(directory))	
        #print("directory", directory)	
        #print("x", x)	
        
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)

            shutil.copy2(x, directory)
        except Exception as exception:
            err_str = "{0} {1}".format(
                type(exception), 
                " ".join([arg for arg in exception.args]))
            print ("Error:" , err_str)
            return (False, constants.EXT_XML,files_result, err_str, ext_filename)	
    
        #print("files_result", files_result)
                    
        return (True,constants.EXT_XML, x, err_str, ext_filename)	

    def filter(self, nameFile):
        result = True	

        if (self.anno == None and self.anno == '' and \
            self.mese == None and self.mese == '' and \
            self.giorno == None and self.giorno == '' and \
            self.codiceFlusso == None and self.codiceFlusso == ''): 
                return True

        items = nameFile.split("_")
        piva_distr = items[0]
        piva_utente = items[1]
        flusso = items[3]
        timestamp = items[4]

        if (self.anno != None and self.anno != ''):
            anno = timestamp[:4]
            result = result and anno == self.anno

        if (self.mese != None and self.mese != ''):
            mese = timestamp[4:6]
            result = result and mese == self.mese

        if (self.giorno != None and self.giorno != ''):
            giorno = timestamp[6:8]
            result = result and giorno == self.giorno
        
        if (self.codiceFlusso != None and self.codiceFlusso != ''):
            codiceFlusso = flusso[:3]
            result = result and codiceFlusso == self.codiceFlusso

        return result

    def stampa(self, value):
        print("****************", value)


    def set_spark_context(self, app_name, master):
        conf = SparkConf().setAppName(app_name) \
            .setMaster(master) \
            .set("spark.shuffle.service.enabled", "false") \
            .set("spark.dynamicAllocation.enabled", "false") \
            .set("spark.io.compression.codec", "snappy") \
            .set("spark.rdd.compress", "true") \
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
        
        return sc, sqlContext

    def RemoveFolder(self, directory_rm):
        directory = os.path.join(constants.TEMPDIRECTORY_XML, directory_rm)
        print("Delete directory:", directory)

        shutil.rmtree(directory)
        print("Directory eliminata:", directory)
        return True
        

    def unzip_subfolder(self, file_name, workdir):
        print ("file_name:", file_name)
        ext_filename = os.path.splitext(file_name)[1].lower()

        result = (False, constants.EXT_XML,file_name, "Nessuna elaborazione", ext_filename)

        if ext_filename == constants.EXT_ZIP:
           result = self.zip_extract(file_name, workdir, ext_filename)

        if ext_filename == constants.EXT_XML:
            result = self.xml_extract(file_name, workdir, ext_filename)

        #print ("Elaborazione file completata: ", file_name)
        return result

        

    def start(self):
        logging.debug("Start Unzip")

        logging.debug("Configurazione spark configuration")
        sc, sqlCtx = self.set_spark_context(self.appName, self.master)

        logging.debug("Configurazione spark configuration completata")

        print("Delete folder:", constants.TEMPDIRECTORY_XML)
        byPass = len(os.listdir(constants.TEMPDIRECTORY_XML)) > 0

        # Verifica se ci sono delle cartelle da cancellare
        if (byPass):
            # Cancella tabella TMP
            uddRemoveDir = sc.parallelize(os.listdir(constants.TEMPDIRECTORY_XML)).repartition(30)
            #result_remove = uddRemoveDir.map(self.RemoveFolder).collect()

            #if not len(os.listdir(constants.TEMPDIRECTORY_XML) ) == 0:
            #    print("Directory is not empty: ", constants.TEMPDIRECTORY_XML)
            #    return
      
        #Versione 2
        print("Unzip subfolder")
        
        # Recupera la lista dei file da elaborare
        rddFolder = UtilFiles().getListOfFiles2V_parallel2(
            sc, 
            dirName = self.workDirectory, 
            anno = self.anno, 
            mese = self.mese, 
            giorno = self.giorno, 
            codiceFlusso = self.codiceFlusso, 
            distr = self.distr
        )

	

        # Decomprime e copia i file XML nella cartella di lavoro (workDirectory)
        result = rddFolder.repartition(30)\
                        .flatMap(lambda x: x)\
                        .map(lambda d: self.unzip_subfolder(d, self.workDirectory))


        # Crea il dataframe per la scrittura sulla tabella 
        dfResult = sqlCtx.createDataFrame(result, schema=constants.schemaZip)

	count_items = dfResult.count()	
	print ("count: ", count_items) 

	if (count_items > 0):
   	     dfResult.write.parquet(constants.PATHHDFS_ZIP,'append')
        
        print("End unzip")

