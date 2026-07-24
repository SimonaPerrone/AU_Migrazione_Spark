# coding=utf-8
import dateutil.relativedelta
import sys
import time
from datetime import datetime
# Librerie Pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# import subprocess

def set_sparkcontext(appName, masterEngine, sparkPartitions, logVerbosity, loggerName):
    """Funzione per configurare ed inizializzare lo SparkContext. Per le opzioni messe a disposizione da Spark,
       vedere la guida ufficiale presente al sito: https://spark.apache.org/docs/latest/configuration.html;

       Argomenti:
       @appName:

       @masterEngine:

       @sparkPartitions:

       @logVerbosity:

       @loggerName:
    """
    conf = SparkConf().setAppName(appName) \
                      .setMaster(masterEngine) \
                      .set("spark.scheduler.mode", "FIFO") \
                      .set("spark.shuffle.service.enabled", "true") \
                      .set("spark.locality.wait","3s") \
                      .set("spark.io.compression.codec", "snappy") \
                      .set("spark.rdd.compress", "true") \
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                      .set("spark.kryoserializer.buffer.max","72m") \
                      .set("spark.kryoserializer.buffer", "4m") \
                      .set("spark.sql.parquet.filterPushdown", "true") \
                      .set("spark.sql.orc.filterPushdown", "true") \
                      .set("spark.sql.execution.arrow.enabled", "true") \
                      .set("spark.sql.shuffle.partitions", sparkPartitions) \
                      .set("spark.ui.showConsoleProgress","true") \
                      .set("spark.rdd.compress","true") \
                      .set("spark.speculation","false") \
                      .set("spark.sql.adaptive.enabled","true") \
                      .set("spark.speculation.quantile","0.70") \
                      .set("spark.speculation.multiplier", "1.5") \
                      .set("spark.task.maxFailures","8")\
                      .set("spark.dynamicAllocation.minExecutors", "5")

    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)
    sc._jvm.org.apache.hadoop.hive.conf.HiveConf()

    sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    sc.setLogLevel(logVerbosity)

    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(loggerName)

    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


    return sc, sqlContext, logger

def read_parquet_tab(path_HDFS, sqlContext, logger):
    """Importazione tabella sotto HDFS come dataframe.
    Argomenti:
    @path_HDFS: path sotto HDFS da importare
    @schema_tab: schema per importare correttamente il formati dati
    @sqlContext: sqlContext
    @logger: log4j associato all'app
    """
    try:
        df = sqlContext.read\
                .option("mergeSchema", "true")\
                .option("mode", "DROPMALFORMED")\
                .parquet(path_HDFS)
        return df
    except:
        logger.error("Attenzione! Si e verificato un errore in fase \
            di import del parquet " + str(path_HDFS))
        raise Exception("Attenzione! Si e verificato un errore in fase \
            di import del parquet " + str(path_HDFS))

def write_target_dataframe(df, repartition_factor, path_output_HDFS, partition_column, column_value, logger):
    """Scrivere dataframe nella partizione corretta"""
    try:
        HDFS_write_path = path_output_HDFS + '/' + partition_column + '=' + column_value
        return df.repartition(repartition_factor).write.mode('overwrite').parquet(HDFS_write_path)
    except:
        logger.error("Attenzione! Si e verificato un errore in fase \
            di scrittura del dataframe " + str(path_output_HDFS))
        raise Exception("Attenzione! Si e verificato un errore in fase \
            di scrittura del dataframe " + str(path_output_HDFS))
            
def read_last_partition(path_HDFS, table_name, annomese_sbg, sqlContext, logger):
    list_partitions = sqlContext.sql("SHOW PARTITIONS {}".format(table_name)).select(collect_list('result')).collect()[0][0]
    correct_partitions = filter(lambda k: "annomese="+annomese_sbg in k, list_partitions)
    if len(correct_partitions) == 0:
        logger.error("Attenzione! Non vi è alcuna partizione con annomese " + annomese_sbg)
        raise Exception("Attenzione! Non vi è alcuna partizione con annomese " + annomese_sbg)
    list_split = [partition.split("/") for partition in correct_partitions]
    executionid_partitions_list = [filter(lambda k: "executionid" in k, list)[0] for list in list_split]
    executionid_list = [partition.split("=")[1] for partition in executionid_partitions_list]
    executionid = sorted(executionid_list, reverse=True)[0].encode()
    df = read_parquet_tab(path_HDFS, sqlContext, logger).where(col("annomese") == lit(annomese_sbg)).where(col("executionid") == lit(executionid))
    return df


if __name__ == "__main__":

    nameOfApp =  'ETL consumi SBG per CLG'
    engineUse = 'yarn-client'
    partitionsToUse = '600'
    loggingName = 'APP_SERVICE'

    startTimeApp = time.time()

    sc, sqlContext, logger = set_sparkcontext(nameOfApp, engineUse, partitionsToUse, 'INFO', loggingName)

    script_args_list = sys.argv
    print(script_args_list)
    annomese_sbg = ((str(list(filter(lambda x: 'ANNOMESE' in x, script_args_list))[0])).split('=')[1]).strip()

    #VERIFICA DI ANNOMESE
    try:
        datetime.strptime(annomese_sbg, "%Y%m")
    except:
        logger.error("Si e' verificato un erorre in fase di lettura del parametro ANNOMESE. Verificare che il formato sia yyyyMM.")
        raise Exception("Si e' verificato un erorre in fase di lettura del parametro ANNOMESE. Verificare che il formato sia yyyyMM.")

    input_HDFS_path = '${daily.consumption.hdfs.path}'
    output_HDFS_path = '/user/hive/warehouse/${py.hive.table.sbgmisure.db}.db/${py.hive.table.sbgmisure.name}'
    table_name = '${daily.consumption.table.name}'
    rep_factor = 80
    sbg_type = ((str(list(filter(lambda x: 'SBG_TYPE' in x, script_args_list))[0])).split('=')[1]).strip()
    annomese = 'annomese_rif'
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    ordered_column_table = ['cod_pdr', 'piva_it', 'piva_udd', 'piva_udb', 'piva_rdb', 'cod_remi', 'id_reg_clim',\
                            'cod_prof_std', 'trattamento', 'trattamento_calcolo', 'giorno', 'consumo', 'tipo_cliente', 'unit_mis_prel', \
                            'data_insert', 'sessione_sbg', 'annomese_rif']

    df = read_last_partition(input_HDFS_path, table_name, annomese_sbg, sqlContext, logger)\
            .select(col('pdr').alias('cod_pdr'),\
                col('pivaudd').alias('piva_udd'),\
                col('pivait').alias('piva_it'),\
                col('tipocliente').alias('tipo_cliente'),\
                col('unitmisprel').alias('unit_mis_prel'),\
                col('pivaudb').alias('piva_udb'),\
                col('codremi').alias('cod_remi'),\
                col('treatment').alias('trattamento'),\
                dayofmonth(to_date(col('date'))).alias('giorno').cast(StringType()),\
                col('codprofstd').alias('cod_prof_std'),\
                col('idregclim').alias('id_reg_clim').cast(StringType()),\
                col('value').alias('consumo'),\
                col('pivardb').alias('piva_rdb'))\
        .withColumn('trattamento_calcolo', lit(None).cast(StringType()))\
        .withColumn('sessione_sbg',lit(sbg_type))\
        .withColumn('data_insert', lit(current_time))\
        .withColumn(annomese, lit(annomese_sbg))

    df2 = df.select(*ordered_column_table)

    write_target_dataframe(df2, rep_factor, output_HDFS_path, annomese, annomese_sbg, logger)
    sc.stop()