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

# Percorso del file parquet dove sono presenti profili standard 
# Valori percentuali necessari alla definizione dei profili di prelievo standard per l'anno termico
#PATHPROFSTD = "file:///opt/data/GAS/tempP/profstd_parquet"

def usage():
    print('Usage')
    print('-i   Input file: Valori percentuali (esempio: file:///opt/data/GAS/XML/profStd.CSV)')
    print('-h   Visualizza questa guida')


class ProfStandard:
	def init(self, mode):
    		APPNAME = "PyAcquirenteUnico"                       # Nome dell'applicazione
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

	def start(self, inputFile, mode):
    		sc, sqlCtx = self.init(mode = mode)
    		schemaPrevStand = StructType([
    		StructField("data", StringType(), True),
    		#StructField("sett", StringType(), True),
    		StructField("c1b1", StringType(), True),
    		StructField("c1c1", StringType(), True),
    		StructField("c1d1", StringType(), True),
		StructField("c1e1", StringType(), True),
    		StructField("c1f1", StringType(), True),
    		StructField("c1b2", StringType(), True),
    		StructField("c1c2", StringType(), True),
	    	StructField("c1d2", StringType(), True),
    		StructField("c1e2", StringType(), True),
    		StructField("c1f2", StringType(), True),
    		StructField("c1b3", StringType(), True),
    		StructField("c1c3", StringType(), True),
    		StructField("c1d3", StringType(), True),
    		StructField("c1e3", StringType(), True),
    		StructField("c1f3", StringType(), True),
    		StructField("c2",   StringType(), True),
    		StructField("c4",   StringType(), True),
    		StructField("t11",  StringType(), True),
    		StructField("t12",  StringType(), True),
    		StructField("t13",  StringType(), True)
    		])

    		rdd = sc.textFile(inputFile).map(lambda line: line.split(","))
    		df = sqlCtx.createDataFrame(rdd, schemaPrevStand)

    		df = df.withColumn("c1b1", df.c1b1.cast(DoubleType()))
    		df = df.withColumn("c1c1", df.c1c1.cast(DoubleType()))
    		df = df.withColumn("c1d1", df.c1d1.cast(DoubleType()))
    		df = df.withColumn("c1e1", df.c1e1.cast(DoubleType()))
    		df = df.withColumn("c1f1", df.c1f1.cast(DoubleType()))
    		df = df.withColumn("c1b2", df.c1b2.cast(DoubleType()))
    		df = df.withColumn("c1c2", df.c1c2.cast(DoubleType()))
    		df = df.withColumn("c1d2", df.c1d2.cast(DoubleType()))
   	 	df = df.withColumn("c1e2", df.c1e2.cast(DoubleType()))
    		df = df.withColumn("c1f2", df.c1f2.cast(DoubleType()))
    		df = df.withColumn("c1b3", df.c1b3.cast(DoubleType()))
    		df = df.withColumn("c1c3", df.c1c3.cast(DoubleType()))
    		df = df.withColumn("c1d3", df.c1d3.cast(DoubleType()))
    		df = df.withColumn("c1e3", df.c1e3.cast(DoubleType()))
    		df = df.withColumn("c1f3", df.c1f3.cast(DoubleType()))

    		df = df.withColumn("c2", df.c2.cast(DoubleType()))
    		df = df.withColumn("c4", df.c4.cast(DoubleType()))
    		df = df.withColumn("t11", df.t11.cast(DoubleType()))
    		df = df.withColumn("t12", df.t12.cast(DoubleType()))
    		df = df.withColumn("t13", df.t13.cast(DoubleType()))

    		#df.show()
    		#df.write.parquet(constants.PATHPROFSTD, 'append')
    		df.write.parquet(constants.PATHPROFSTD, 'overwrite')

# Load 
def main(argv):
        try:
            opts, args = getopt.getopt(argv, "hvi:", ["input="])
        except getopt.GetoptError as err:
            print(err)
            usage()
            sys.exit(2)

        for opt, arg in opts:
            if opt == '-h':
                usage()
                sys.exit()
            elif opt in ('-i', '--input'):
                ProfStandard().start(arg, 'local')        

if __name__ == "__main__":
    main(sys.argv[1:])
