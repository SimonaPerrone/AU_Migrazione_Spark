from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonicallyIncreasingId
from pyspark.sql.functions import expr
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from itertools import chain
from __builtin__ import min as minx
import constants
import getopt
import sys


APPNAME = "PyAcquirenteUnico"                                                   # Nome dell'applicazione


def usage():
    print('Usage')
    print('-i   Input file (esempio: file:///opt/data/GAS/XML/profStd.CSV)')
    print('-h   Visualizza questa guida')

class ImportTable:
	def init(self, mode):
    		master = mode
                conf = SparkConf().setAppName(APPNAME) \
                                .setMaster(master) \
                                .set("spark.shuffle.service.enabled", "false") \
                                .set("spark.dynamicAllocation.enabled", "false") \
                                .set("spark.io.compression.codec", "snappy") \
                                .set("spark.rdd.compress", "true") \
                                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
                                .set("spark.sql.execution.arrow.enabled", "true")

                #Recupero il context di spark
                sc = SparkContext(conf=conf)
                sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

                sqlCtx = SQLContext(sc)
                sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                sqlCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
                sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
                sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
                sqlCtx.setConf("hive.exec.dynamic.partition", "true")
                sqlCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

		return (sc, sqlCtx)

        def start(self, mode):
    		sc, sqlCtx = self.init(mode)

                ########################### Profili di prelievo standard e parametri caratteristici ####################
                #PATHPREVSTAND = "file:///opt/data/GAS/tempP/prelstand"
                #PATHPREVSTAND = "/user/acu/au/misure_gas_au/param_caratt"

                schemaPrevStand = StructType([
                StructField("PROF",         StringType(),   True), 
                StructField("B1",           DoubleType(),   True), 
                StructField("B2",           DoubleType(),   True),
                StructField("B3",           DoubleType(),   True), 
                StructField("B4",           DoubleType(),   True), 
                StructField("Cat",          StringType(),   True), 
                StructField("Zona",         StringType(),   True), 
                StructField("classe",       IntegerType(),  True)
                ])

                dfPrevStand = sc.parallelize([
                ('C1A1',        1.0,    0.0,    0.0,    0.0,    'C1',   'A',    1),
                ('C1B1',        1.0,    0.0,    0.0,    0.0,    'C1',   'B',    1),
                ('C1C1',        1.0,    0.0,    0.0,    0.0,    'C1',   'C',    1),
                ('C1D1',        1.0,    0.0,    0.0,    0.0,    'C1',   'D',    1),
                ('C1E1',        1.0,    0.0,    0.0,    0.0,    'C1',   'E',    1),
                ('C1F1',        1.0,    0.0,    0.0,    0.0,    'C1',   'F',    1),
                ('C2X1',        0.0,    1.0,    0.0,    0.0,    'C2',   'X',    1),
                ('C3A1',        0.57,   0.43,   0.0,    0.0,    'C3',   'A',    1),
                ('C3B1',        0.57,   0.43,   0.0,    0.0,    'C3',   'B',    1),
                ('C3C1',        0.76,   0.24,   0.0,    0.0,    'C3',   'C',    1),
                ('C3D1',        0.71,   0.29,   0.0,    0.0,    'C3',   'D',    1),
                ('C3E1',        0.76,   0.24,   0.0,    0.0,    'C3',   'E',    1),
                ('C3F1',        0.72,   0.28,   0.0,    0.0,    'C3',   'F',    1),
                ('C4X1',        0.0,    0.0,    0.0,    1.0,    'C4',   'X',    1),
                ('C5A1',        0.50,   0.0,    0.0,    0.50,   'C5',   'A',    1),
                ('C5B1',        0.50,   0.0,    0.0,    0.50,   'C5',   'B',    1),
                ('C5C1',        0.60,   0.0,    0.0,    0.40,   'C5',   'C',    1),
                ('C5D1',        0.67,   0.0,    0.0,    0.33,   'C5',   'D',    1),
                ('C5E1',        0.72,   0.0,    0.0,    0.28,   'C5',   'E',    1),
                ('C5F1',        0.80,   0.0,    0.0,    0.20,   'C5',   'F',    1),
                ('T1X1',        0.0,    0.0,    1.0,    0.0,    'T1',   'X',    1),
                ('T1X2',        0.0,    0.0,    1.0,    0.0,    'T1',   'X',    2),
                ('T1X3',        0.0,    0.0,    1.0,    0.0,    'T1',   'X',    3),
                ('T2A1',        0.23,   0.0,    0.77,   0.0,    'T2',   'A',    1),
                ('T2B1',        0.23,   0.0,    0.77,   0.0,    'T2',   'B',    1),
                ('T2C1',        0.23,   0.0,    0.77,   0.0,    'T2',   'C',    1),
                ('T2D1',        0.23,   0.0,    0.77,   0.0,    'T2',   'D',    1),
                ('T2E1',        0.23,   0.0,    0.77,   0.0,    'T2',   'E',    1),
                ('T2F1',        0.23,   0.0,    0.77,   0.0,    'T2',   'F',    1),
                ('T2A2',        0.23,   0.0,    0.77,   0.0,    'T2',   'A',    2),
                ('T2B2',        0.23,   0.0,    0.77,   0.0,    'T2',   'B',    2),
                ('T2C2',        0.23,   0.0,    0.77,   0.0,    'T2',   'C',    2),
                ('T2D2',        0.23,   0.0,    0.77,   0.0,    'T2',   'D',    2),
                ('T2E2',        0.23,   0.0,    0.77,   0.0,    'T2',   'E',    2),
                ('T2F2',        0.23,   0.0,    0.77,   0.0,    'T2',   'F',    2),
                ('T2A3',        0.23,   0.0,    0.77,   0.0,    'T2',   'A',    3),
                ('T2B3',        0.23,   0.0,    0.77,   0.0,    'T2',   'B',    3),
                ('T2C3',        0.23,   0.0,    0.77,   0.0,    'T2',   'C',    3),
                ('T2D3',        0.23,   0.0,    0.77,   0.0,    'T2',   'D',    3),
                ('T2E3',        0.23,   0.0,    0.77,   0.0,    'T2',   'E',    3),
                ('T2F3',        0.23,   0.0,    0.77,   0.0,    'T2',   'F',    3)
                ])

                dfPrevStand = sqlCtx.createDataFrame(dfPrevStand, schemaPrevStand)
                dfPrevStand.write.parquet(constants.PATHPREVSTAND, 'overwrite')
		
                #################################### Categorie d uso del gas ########################################
                #PATHCATEGORIATERMICA = "file:///opt/data/GAS/tempP/categtermica"

                schemaCategTermica = StructType([
                StructField("Codice",       StringType(),   True), 
                StructField("Descrizione",  StringType(),   True), 
                StructField("CompTermica",  BooleanType(),  True)
                ])

                dfCategTermica = sc.parallelize([ 
                ('C1',  'Riscaldamento',                                                                True), 
                ('C2',  'Uso cottura cibi e/o produzione di acqua calda sanitaria',                     False),
                ('C3',  'Riscaldamento + uso cottura cibi e/o produzione di acqua calda sanitaria',     True),
                ('C4',  'Uso condizionamento',                                                          False),
                ('C5',  'Uso condizionamento + riscaldamento',                                          True),
                ('T1',  'Uso tecnologico (artigianale-industriale)',                                    False),
                ('T2',  'Uso tecnologico + riscaldamento',                                              True)
                ])

                dfCategTermicaDF = sqlCtx.createDataFrame(dfCategTermica, schemaCategTermica)
                dfCategTermicaDF.write.parquet(constants.PATHCATEGORIATERMICA, 'overwrite')

                #################################### CLASSI PRELIEVO ########################################
                #PATHCLASSIPRELIEVO = "file:///opt/data/GAS/tempP/classiprelievo"

                schemaClassiPrelievo = StructType([
                StructField("Codice",       StringType(),   True), 
                StructField("Descrizione",  StringType(),   True), 
                StructField("Giorni",       IntegerType(),  True)
                ])

                dfClassiPrelievo = sc.parallelize([ 
                ('1', '7 giorni',                                                       7), 
                ('2', '6 giorni (escluse domeniche e festivita nazionali)',             6),
                ('3', '5 giorni (esclusi sabati, domeniche e festivita nazionali)',     5)
                ])

                dfClassiPrelievoDF = sqlCtx.createDataFrame(dfClassiPrelievo, schemaClassiPrelievo)
                dfClassiPrelievoDF.write.parquet(constants.PATHCLASSIPRELIEVO, 'overwrite')      


# Load 
def main(argv):
        try:
            opts, args = getopt.getopt(argv, "h", []) 
        except getopt.GetoptError as err:
            print(err)
            usage()
            sys.exit(2)

        for opt, arg in opts:
            if opt == '-h':
                usage()
                sys.exit()
                

	ImportTable().start('local')        

if __name__ == "__main__":
    main(sys.argv[1:])
