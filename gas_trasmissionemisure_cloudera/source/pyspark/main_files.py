
import sys
import os
import json
import shutil
import datetime
import time
import glob
import getopt
import zipfile

from functions.spark_utils import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.utils import IllegalArgumentException

HOME = "/mnt/isilonshare_gas/"
DIRECTORY_XML = "/mnt/isilonshare1/TEST_GAS_INJ/"
PATHHDFS_FILES = "/user/silvia/au/misure_gas_au/cmg_gas/gas_files"
PATHHDFS_ZIP = "/user/silvia/au/misure_gas_au/cmg_gas/gas_zip"


def set_spark_context(app_name, master):
    conf = SparkConf().setAppName(app_name) \
        .setMaster(master) \
        .set("spark.shuffle.service.enabled", "true") \
        .set("spark.dynamicAllocation.enabled", "false") \
        .set("spark.io.compression.codec", "snappy") \
        .set("spark.rdd.compress", "true") \
        .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
        .set("spark.sql.execution.arrow.enabled", "true")
    sc = SparkContext(conf=conf)
    try:
        sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        sqlCtx = sqlContext = HiveContext(sc)

    except:
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


def zip_extract(x, workdir, ext_filename):
    """
    Unzip dai file zip i file xml
    """
    err_str = ""
    baseDir = os.path.dirname(x) + "/"
    home = HOME
    baseDir = baseDir.replace(home,"")
    directoryXML = DIRECTORY_XML
    directory = directoryXML + baseDir

    list_files = [] 

    print ("file: ", x)
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError as exception:  # Python >2.5
        err_str="Error: makedir"
        #err_str = "{0} {1}".format(
        #    type(exception), 
        #    " ".join([arg for arg in exception.args]))
        print ("Error: ", exception)
        return (False, "zip", x, err_str, ext_filename)	
    # Unzip file
    try:    
        file_obj = zipfile.ZipFile(x, "r")
        file_obj.extractall(directory)
        list_files = file_obj.namelist()
    except Exception as exception:
        err_str="Error: extract zip"
        #err_str = "{0} {1}".format(
        #    type(exception), 
        #    " ".join([arg for arg in exception.args]))
        #print ("Error:" , err_str)
        print ("Error: ", exception)
        return (False, "zip", x, err_str, ext_filename)	
    return (True,"zip", x, err_str, ext_filename)	

def xml_extract(x, workdir, ext_filename):
    """
    Copia i file xml dalla cartella sorgente nella cartella di lavoro
    """
    err_str = ""
    baseDir = os.path.dirname(x) + "/"
    home = HOME
    baseDir = baseDir.replace(home,"")
    directoryXML = DIRECTORY_XML
    directory = directoryXML + baseDir 
    files_result = directory + os.path.basename(x)

    print ("file: ", x)
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        shutil.copy2(x, directory)
    except Exception as exception:
        err_str="Error: copy file"
        print ("Error: ", exception)
        return (False, "xml", files_result, err_str, ext_filename)	
    return (True,"xml", x, err_str, ext_filename)	

def unzip_subfolder(file_name, workdir):
    ext_filename = os.path.splitext(file_name)[1].lower()
    result = (False, "xml",file_name, "Nessuna elaborazione", ext_filename)
    if ext_filename == ".zip":
        result = zip_extract(file_name, workdir, ext_filename)
    if ext_filename == ".xml":
        result = xml_extract(file_name, workdir, ext_filename)
    return result

#/mnt/isilonshare_gas/TMG_04080690656/DISTRIBUTORE/TMG_04080690656_03916040656/2019/1016/04080690656_03916040656_201910_TMV0350_20191016074226_10.xml.zip
def getFiles(distr):
    src = HOME + "TMG_" + distr[0] + "/DISTRIBUTORE/*/*/*/*"
    print ("SRC:", src)
    list_file = glob.glob(src)
    print ("SRC:", src, len(list_file))
    return list_file


def main(argv):
    version = '1.0'
    mode = 'yarn-client'
    #mode = 'local'

    # Creare spark context
    sc, sqlCtx = set_spark_context("GAS - Unzip All V." + version, mode)

    schema = StructType([
        StructField("file", StringType(), True)
    ])

    #select distinct split(nomefile,"_")[0] as distr from gas_file_mancanti
    query = "select distinct split(nomefile,'_')[0] as distr from cmg_gas.gas_file_mancanti"
    rdd = sqlCtx.sql(query).map(lambda d: d)
    min_partition = rdd.count()
    print ("Partition:", min_partition)
    #rdd_files = df.rdd.repartition(100).flatMap(getFiles)
    print(rdd.repartition(min_partition).glom().collect())
    rdd_files = rdd.repartition(min_partition).map(lambda d: getFiles(d))#.flatMap(lambda d: d)
    #print(rdd_files.collect())

    rdd_files = sc.parallelize(rdd_files.collect())

    dataframe = rdd_files.flatMap(lambda d: d).filter(lambda d: 'XML' in d or 'xml' in d or 'zip' in d or 'ZIP' in d).map(lambda d: (d,)).toDF()
    dfResult = sqlCtx.createDataFrame(dataframe.rdd, schema=schema)
    dfResult.write.parquet(PATHHDFS_FILES,'append')
   
    #HOME=/mnt/isilonshare_gas/
    #list_distr = os.listdir(HOME)
    #for distr in list_distr:
    #    try:
    #        input_file = DIRECTORY_XML + "/files" + distr + ".txt"
    #        print ("file Input:", input_file)
    #        if os.path.isfile(input_file):
    #            rdd = sc.textFile( "file:" + input_file)
    #            dataframe = rdd.filter(lambda d: 'XML' in d or 'xml' in d or 'zip' in d or 'ZIP' in d).map(lambda d: (d,)).toDF()
    #            dfResult = sqlCtx.createDataFrame(dataframe.rdd, schema=schema)
    #            dfResult.write.parquet(PATHHDFS_FILES,'append')
    #    except Exception as e:
    #        print ("Error:", e)

    dataframe_files = sqlCtx.read.parquet(PATHHDFS_FILES)
    result = dataframe_files.map(lambda d: unzip_subfolder(d["file"], ""))

    schemaZip = StructType([
            StructField("result", StringType(), True),
            StructField("type", StringType(), True),
            StructField("file", StringType(), True),
            StructField("error", StringType(), True),
            StructField("ext", StringType(), True)
    ])
    dfResult = sqlCtx.createDataFrame(result, schema=schemaZip)
    dfResult.write.parquet(PATHHDFS_ZIP,'append')


if __name__ == "__main__":
    main(sys.argv[1:])
