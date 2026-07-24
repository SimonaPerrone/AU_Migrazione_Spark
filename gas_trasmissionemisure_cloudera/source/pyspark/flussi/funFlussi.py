import logging
import os
import sys
from __builtin__ import min as minx
from datetime import datetime
from itertools import chain

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType

import constants

# Conversione data string in datatime
# dt = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')

class FunFlussi:
    def __init__(self, directoryName, mode):
        self.master = mode
        self.appName = constants.APPNAME
        self.workDirectory = directoryName

        conf = SparkConf().setAppName(self.appName) \
            .setMaster(self.master) \
            .set("spark.shuffle.service.enabled", "false") \
            .set("spark.dynamicAllocation.enabled", "false") \
            .set("spark.io.compression.codec", "snappy") \
            .set("spark.rdd.compress", "true") \
            .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
            .set("spark.sql.execution.arrow.enabled", "true")
        logging.debug("SparkConfig completata")
        # Recupero il context di spark
        sc = SparkContext(conf=conf)
        sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
        logging.debug("HaddopConfiguration completata")
        # sc.setLogLevel(logging.getLevelName)

        sqlCtx = SQLContext(sc)
        sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        sqlCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
        sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
        sqlCtx.setConf("spark.sql.parquet.output.committer.class","org.apache.spark.sql.parquet.ParquetOutputCommitter")
        sqlCtx.setConf("hive.exec.dynamic.partition", "true")
        sqlCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        self.sc = sc
        self.sqlCtx = sqlCtx
        self.hiveContext = HiveContext(sc)

    def CalcoloConsumoAnnuoAltreFreq(self, dateStart, dateEnd):
        # Funzione CA (Complicata "f1")
	    # Input:  dateStart, dateEnd
        # Calcolati: Ce = min (Ck -valore medio )
        # CApdr = df.filter(data <= dateStart and data>=dateEnd )

        # DEBUG: usate per il debug
        #dateStart = datetime.strptime("2018-09-28", '%Y-%m-%d').date()
        #dateEnd   = datetime.strptime("2018-10-12", '%Y-%m-%d').date()

        # Lettura della tabella Parametri: 
        # +----+----+----+---+----+---+----+------+
        # |PROF|  B1|  B2| B3|  B4|Cat|Zona|classe|
        # +----+----+----+---+----+---+----+------+
        # |C1A1| 1.0| 0.0|0.0| 0.0| C1|   A|     1|
        # |C1B1| 1.0| 0.0|0.0| 0.0| C1|   B|     1|
        # |C1C1| 1.0| 0.0|0.0| 0.0| C1|   C|     1|

        #param = sqlContext.read.parquet("/user/acu/au/misure_gas_au/gas_param_carat")
        param = self.sqlCtx.read.parquet(constants.PATHPREVSTAND)

        # Lettura dei profili standard
        # +----------+------+------+------+----------+----------+------+------+------+----------+----------+------+------+------+----------+----------+----------+------+----------+----------+---------+
        # |      data|  c1b1|  c1c1|  c1d1|      c1e1|      c1f1|  c1b2|  c1c2|  c1d2|      c1e2|      c1f2|  c1b3|  c1c3|  c1d3|      c1e3|      c1f3|        c2|    c4|       t11|       t12|      t13|
        # +----------+------+------+------+----------+----------+------+------+------+----------+----------+------+------+------+----------+----------+----------+------+----------+----------+---------+
        # |2018-01-10|1.0E-8|1.0E-8|1.0E-8|    1.0E-8|0.14521181|1.0E-8|1.0E-8|1.0E-8|    1.0E-8|0.17671984|1.0E-8|1.0E-8|1.0E-8|    1.0E-8|0.21194378|0.26200172|1.0E-8|0.24441402|0.29401604|0.3532844|
        # |2018-02-10|1.0E-8|1.0E-8|1.0E-8|    1.0E-8|0.14521181|1.0E-8|1.0E-8|1.0E-8|    1.0E-8|0.17671984|1.0E-8|1.0E-8|1.0E-8|    1.0E-8|0.21194378|0.26200172|1.0E-8|0.24441402|0.29401604|0.3532844|

        #prof_stand = sqlContext.read.parquet("/user/acu/au/misure_gas_au/gas_profstand")
        prof_stand = self.sqlCtx.read.parquet(constants.PATHPROFSTD)
        prof_stand = prof_stand.withColumn("data",expr("from_unixtime(unix_timestamp(data, 'MM/dd/yyyy'))").cast("timestamp"))


        # TODO Recuperare la tabella degli rddWk  
        # 

        # *********************************** TMP *****************************
        rddWk = self.sc.parallelize([
        (0, '2018-10-01', 100.0),
        (1, '2018-10-02', 100.0),
        (2, '2018-10-03', 100.0),
        (3, '2018-10-04', 100.0),
        (4, '2018-10-05', 100.0),
        (5, '2018-10-06', 100.0),
        (6, '2018-10-07', 100.0),
        (7, '2018-10-08', 100.0),
        (8, '2018-10-09', 100.0),
        (9, '2018-10-10', 100.0),
        (10, '2018-10-11', 100.0),
        (11, '2018-10-12', 100.0),
        (12, '2018-10-13', 100.0),
        (13, '2018-10-14', 100.0),
        (14, '2018-10-15', 100.0),
        (15, '2018-10-16', 100.0),
        (16, '2018-10-17', 100.0),
        (17, '2018-10-18', 100.0),
        (18, '2018-10-19', 100.0),
        (19, '2018-10-20', 100.0),
        (20, '2018-10-21', 100.0),
        (21, '2018-10-22', 100.0),
        (22, '2018-10-23', 100.0),
        (23, '2018-10-24', 100.0),
        (24, '2018-10-25', 100.0),
        (25, '2018-10-26', 100.0),
        (26, '2018-10-27', 100.0),
        (27, '2018-10-28', 100.0),
        (28, '2018-10-29', 100.0),
        (29, '2018-10-30', 100.0),
        (30, '2018-10-31', 100.0),
        (31, '2018-11-01', 100.0),
        (32, '2018-11-02', 100.0),
        (33, '2018-11-03', 100.0),
        (34, '2018-11-04', 100.0),
        (35, '2018-11-05', 100.0),
        (36, '2018-11-06', 100.0),
        (37, '2018-11-07', 100.0),
        (38, '2018-11-08', 100.0),
        (39, '2018-11-09', 100.0),
        (40, '2018-11-10', 100.0),
        (41, '2018-11-11', 100.0),
        (42, '2018-11-12', 100.0),
        (43, '2018-11-13', 100.0),
        (44, '2018-11-14', 100.0),
        (45, '2018-11-15', 100.0),
        (46, '2018-11-16', 100.0),
        (47, '2018-11-17', 100.0),
        (48, '2018-11-18', 100.0),
        (49, '2018-11-19', 100.0),
        (50, '2018-11-20', 100.0),
        (51, '2018-11-21', 100.0),
        (52, '2018-11-22', 100.0),
        (53, '2018-11-23', 100.0),
        (54, '2018-11-24', 100.0),
        (55, '2018-11-25', 100.0),
        (56, '2018-11-26', 100.0),
        (57, '2018-11-27', 100.0),
        (58, '2018-11-28', 100.0),
        (59, '2018-11-29', 100.0),
        (60, '2018-11-30', 100.0),
        (61, '2018-12-01', 100.0),
        (62, '2018-12-02', 100.0),
        (63, '2018-12-03', 100.0),
        (64, '2018-12-04', 100.0),
        (65, '2018-12-05', 100.0),
        (66, '2018-12-06', 100.0),
        (67, '2018-12-07', 100.0),
        (68, '2018-12-08', 100.0),
        (69, '2018-12-09', 100.0),
        (70, '2018-12-10', 100.0),
        (71, '2018-12-11', 100.0),
        (72, '2018-12-12', 100.0),
        (73, '2018-12-13', 100.0),
        (74, '2018-12-14', 100.0),
        (75, '2018-12-15', 100.0),
        (76, '2018-12-16', 100.0),
        (77, '2018-12-17', 100.0),
        (78, '2018-12-18', 100.0),
        (79, '2018-12-19', 100.0),
        (80, '2018-12-20', 100.0),
        (81, '2018-12-21', 100.0),
        (82, '2018-12-22', 100.0),
        (83, '2018-12-23', 100.0),
        (84, '2018-12-24', 100.0),
        (85, '2018-12-25', 100.0),
        (86, '2018-12-26', 100.0),
        (87, '2018-12-27', 100.0),
        (88, '2018-12-28', 100.0),
        (89, '2018-12-29', 100.0),
        (90, '2018-12-30', 100.0),
        (91, '2018-12-31', 100.0),
        (92, '2019-01-01', 100.0),
        (93, '2019-01-02', 100.0),
        (94, '2019-01-03', 100.0),
        (95, '2019-01-04', 100.0),
        (96, '2019-01-05', 100.0),
        (97, '2019-01-06', 100.0),
        (98, '2019-01-07', 100.0),
        (99, '2019-01-08', 100.0),
        (100, '2019-01-09', 100.0),
        (101, '2019-01-10', 100.0),
        (102, '2019-01-11', 100.0),
        (103, '2019-01-12', 100.0),
        (104, '2019-01-13', 100.0),
        (105, '2019-01-14', 100.0),
        (106, '2019-01-15', 100.0),
        (107, '2019-01-16', 100.0),
        (108, '2019-01-17', 100.0),
        (109, '2019-01-18', 100.0),
        (110, '2019-01-19', 100.0),
        (111, '2019-01-20', 100.0),
        (112, '2019-01-21', 100.0),
        (113, '2019-01-22', 100.0),
        (114, '2019-01-23', 100.0),
        (115, '2019-01-24', 100.0),
        (116, '2019-01-25', 100.0),
        (117, '2019-01-26', 100.0),
        (118, '2019-01-27', 100.0),
        (119, '2019-01-28', 100.0),
        (120, '2019-01-29', 100.0),
        (121, '2019-01-30', 100.0),
        (122, '2019-01-31', 100.0),
        (123, '2019-02-01', 100.0),
        (124, '2019-02-02', 100.0),
        (125, '2019-02-03', 100.0),
        (126, '2019-02-04', 100.0),
        (127, '2019-02-05', 100.0),
        (128, '2019-02-06', 100.0),
        (129, '2019-02-07', 100.0),
        (130, '2019-02-08', 100.0),
        (131, '2019-02-09', 100.0),
        (132, '2019-02-10', 100.0),
        (133, '2019-02-11', 100.0),
        (134, '2019-02-12', 100.0),
        (135, '2019-02-13', 100.0),
        (136, '2019-02-14', 100.0),
        (137, '2019-02-15', 100.0),
        (138, '2019-02-16', 100.0),
        (139, '2019-02-17', 100.0),
        (140, '2019-02-18', 100.0),
        (141, '2019-02-19', 100.0),
        (142, '2019-02-20', 100.0),
        (143, '2019-02-21', 100.0),
        (144, '2019-02-22', 100.0),
        (145, '2019-02-23', 100.0),
        (146, '2019-02-24', 100.0),
        (147, '2019-02-25', 100.0),
        (148, '2019-02-26', 100.0),
        (149, '2019-02-27', 100.0),
        (150, '2019-02-28', 100.0)
        ])
              
               
        rddWkschema2 = StructType([
                StructField("id", LongType(), True),
                StructField("data", StringType(), True),
                StructField("value", DoubleType(), True)
        ])

        #dfWk = sqlContext.createDataFrame(rddWk, rddWkschema2)
        dfWk = self.sqlCtx.createDataFrame(rddWk, rddWkschema2)
        dfWk = dfWk.withColumn("dataWk",expr("from_unixtime(unix_timestamp(data, 'yyyy-MM-dd'))").cast("timestamp"))

	# print dfWk.rdd.getNumPartitions() 7
        # print prof_stand.rdd.getNumPartitions() 1
        # print param.rdd.getNumPartitions() 1
        

	# Lettura delle misure
        #df = sqlContext.read.parquet("/user/acu/au/misure_gas_au/gas_tal_500")
        df = self.sqlCtx.read.parquet(constants.PATHHDFS_TAL)
        
        df = df.sort(df.cod_pdr)
        df = df.repartition(df.cod_pdr)


        #dfk = df.withColumn('dataElaborazione', F.to_timestamp(df.dataElaborazione)).drop(df.dataElaborazione)
        dfk = df.withColumn("data_com_autolet_cf",expr("from_unixtime(unix_timestamp(data_com_autolet_cf, 'dd/MM/yyyy'))").cast("timestamp"))
        dfk = dfk.withColumn('let_tot_prel', dfk.let_tot_prel.cast(DoubleType()))



        # Recupero le tabelle raggruppate per cod_pdr
        _list = chain(*dfk.groupby(dfk.cod_pdr).agg( F.count(lit(1)).alias("Num")).where(col("Num")> 1).select(col("cod_pdr")).distinct().collect())
        df_by_pdr = {Av: dfk.where(dfk.cod_pdr == Av) for Av in _list }

        # Per ogni tabella
        for it in df_by_pdr:
            dfItem = df_by_pdr[it]
            #dfItem = df_by_pdr["12340000111836"]
            #01510000015833
            # ordina i dati per data lettura
	    
	    #dfItem.show()


            dfItem = dfItem.sort(dfItem.data_com_autolet_cf)
            if (dfItem.count() > 0):
		# ************************ Calcolo fattore A ************************
			
		# Aggiungo alla dataframe un progressivo
                new_schema = StructType([StructField("id",LongType(),True)] + dfItem.schema.fields)
                zipped_rdd = dfItem.rdd.zipWithIndex()
                new_rdd = zipped_rdd.map(lambda (row,rowId): ([rowId] + list(row)))
		#dfItemWithIds = sqlContext.createDataFrame(new_rdd, new_schema)
		dfItemWithIds = self.sqlCtx.createDataFrame(new_rdd, new_schema)

		# al dataframe elimino la prima row
		dfSuc = dfItemWithIds.where(dfItemWithIds.id > 0 ).select(
			dfItemWithIds.let_tot_prel.alias('let_tot_prel2'),
			dfItemWithIds.data_com_autolet_cf.alias('data_com_autolet_cf2'))

		# Generazione della "tabella dei dati Successivi"
		new_schema_dfSuc = StructType([StructField("id2",LongType(),True)] + dfSuc.schema.fields)
		zipped_rdd_dfSuc = dfSuc.rdd.zipWithIndex()
		new_rdd_dfSuc = zipped_rdd_dfSuc.map(lambda (row,rowId): ([rowId] + list(row)))
		#dfSucWithIds = sqlContext.createDataFrame(new_rdd_dfSuc, new_schema_dfSuc)
		dfSucWithIds = self.sqlCtx.createDataFrame(new_rdd_dfSuc, new_schema_dfSuc)

		print "************************************     Calcolo m(z+1) - m(z)"
		# Calcolo m(z+1) - m(z)
		res = dfItemWithIds.join(dfSucWithIds, dfItemWithIds.id == dfSucWithIds.id2).withColumn('sumMeasure', (coalesce(dfSucWithIds.let_tot_prel2, lit(0.0)) - coalesce(dfItemWithIds.let_tot_prel, lit(0.0)) ))

		# ************************ Fine fattore A ************************
		

		#TODO recuperare i dati per stabilire il link con la tabella Parametri Standard, Wk e Profili Standard
		res = res.withColumn("i", lit("B")).withColumn("j", lit(1)).withColumn("cat", lit('C1'))
		res = res.withColumn("PROF_PARAM", concat(res.cat,res.i,res.j))
		res = res.withColumn("classe_uso", concat(lit("t1"),res.j))
			
		# Eseguo join con la tabella dei profili standard. La Join utilizza la data di lettura 1 e 2.
		res2 = res.join(prof_stand, (res.data_com_autolet_cf <= prof_stand.data) & (res.data_com_autolet_cf2 >= prof_stand.data))

		# Eseguo join con la tabella dei Wk. La Join utilizza il campo "data"
		res2 = res2.join(dfWk.drop("id").drop("data"), dfWk.dataWk == res2.data)

		# Calcolo del C1 e T1
		#
		print "************************************     Calcolo del C1 e T1"
		rdd = res2.map(lambda d: (d["id"], d[d.PROF_PARAM.lower()], d[d.classe_uso], d['data_com_autolet_cf'], d['data_com_autolet_cf2']))

		#dfC1T1 = sqlContext.createDataFrame(rdd, StructType([
		#           StructField("id3", LongType(), True),
		#           StructField("C1", DoubleType(), True),
		#           StructField("T1", DoubleType(), True),
		#           StructField("dataI", DateType(), True),
		#           StructField("dataF", DateType(), True)
		#   ]))
		dfC1T1 = self.sqlCtx.createDataFrame(rdd, constants.schemaC1T1)

		# Creazione dataframe con id
		schema_rdd_dfC1T1 = StructType([StructField("id4",LongType(),True)] + dfC1T1.schema.fields)
		zipped_rdd_dfC1T1 = rdd.zipWithIndex()
		new_rdd_dfC1T1 = zipped_rdd_dfC1T1.map(lambda (row,rowId): ([rowId] + list(row)))
		#dfC1T1WithIds = sqlContext.createDataFrame(new_rdd_dfC1T1, schema_rdd_dfC1T1)
		dfC1T1WithIds = self.sqlCtx.createDataFrame(new_rdd_dfC1T1, schema_rdd_dfC1T1)

		# Creazione ID
		schema_res2 = StructType([StructField("id5",LongType(),True)] + res2.schema.fields)
		zipped_rdd_res2 = res2.rdd.zipWithIndex()
		new_rdd_res2 = zipped_rdd_res2.map(lambda (row,rowId): ([rowId] + list(row)))
		#res2WithIds = sqlContext.createDataFrame(new_rdd_res2, schema_res2)
		res2WithIds = self.sqlCtx.createDataFrame(new_rdd_res2, schema_res2)

		# Generazione della tabelle dei dati e dei parametri C1 e T1
		res3 = res2WithIds.join(dfC1T1WithIds, dfC1T1WithIds.id4 == res2WithIds.id5)

		# Join con la tabella Profili Standard
		res3 = res3.join(param, (param.PROF == res3.PROF_PARAM))
		

	# DEBUG: usate per il debug
	#dateStart = datetime.strptime("2018-10-02", '%Y-%m-%d').date()
	#dateEnd   = datetime.strptime("2018-12-16", '%Y-%m-%d').date()


		firstDataStart = dfItem.where( (dfItem.data_com_autolet_cf <= dateStart) ).sort('data_com_autolet_cf',ascending=False).first()
		firstDataEnd = dfItem.where( (dfItem.data_com_autolet_cf >= dateEnd) ).sort('data_com_autolet_cf',ascending=True).first()
		
		dateStartFilter_first = firstDataStart['data_com_autolet_cf'] if firstDataStart != None else None
		dateEndFilter_last = firstDataEnd['data_com_autolet_cf'] if firstDataStart != None else None


		if (dateStartFilter_first != None and dateEndFilter_last != None):
			# ********************************  Calcolo del Profk ********************************
			profk = res3.where((res3.data_com_autolet_cf >= dateStartFilter_first) & (res3.data_com_autolet_cf2 <= dateEndFilter_last)) \
					.groupby(res3.cod_pdr,res3.data_com_autolet_cf, res3.data_com_autolet_cf2, res3.sumMeasure) \
					.agg(F.sum(  (res3.value * res3.B1 * res3.C1 + res3.B2 * res3.c2 + res3.B3 * res3.T1 + res3.B4 * res3.c4) ).alias("Profk"))

			
			# ********************************  Calcolo del Profnk ********************************
			profnk = res3.where((res3.data_com_autolet_cf >= dateStartFilter_first) & (res3.data_com_autolet_cf2 <= dateEndFilter_last)) \
					.groupby(res3.cod_pdr,res3.data_com_autolet_cf, res3.data_com_autolet_cf2, res3.sumMeasure) \
					.agg(F.sum(  (res3.B1 * res3.C1 + res3.B2 * res3.c2 + res3.B3 * res3.T1 + res3.B4 * res3.c4) ).alias("Profnk"))
		

			# ********************************  Calcolo del Profank ********************************
			profank = res3.where((res3.data >= dateStart) & (res3.data <= dateEnd)) \
					.groupby(res3.cod_pdr,res3.data_com_autolet_cf, res3.data_com_autolet_cf2, res3.sumMeasure) \
					.agg(F.sum(  (res3.B1 * res3.C1 + res3.B2 * res3.c2 + res3.B3 * res3.T1 + res3.B4 * res3.c4) ).alias("Profank"))
		


			# Unisco i dati per codice pdr
			res4 = profk.join( profnk, (profnk.cod_pdr == profk.cod_pdr) & (profnk.data_com_autolet_cf == profk.data_com_autolet_cf) & (profnk.data_com_autolet_cf2 == profk.data_com_autolet_cf2) & (profnk.sumMeasure == profk.sumMeasure) ) \
					.drop(profnk.cod_pdr) \
					.drop(profnk.data_com_autolet_cf) \
					.drop(profnk.data_com_autolet_cf2) \
					.drop(profnk.sumMeasure)

			# Unisco i dati per codice pdr
			res4 = res4.join( profank, (res4.cod_pdr == profank.cod_pdr) & (res4.data_com_autolet_cf == profank.data_com_autolet_cf) & (res4.data_com_autolet_cf2 == profank.data_com_autolet_cf2) & (res4.sumMeasure == profank.sumMeasure) ) \
					.drop(profank.cod_pdr) \
					.drop(profank.data_com_autolet_cf) \
					.drop(profank.data_com_autolet_cf2) \
					.drop(profank.sumMeasure)


			# Calcolo della seconda parte del CA (valori esterni)
			result = res4.groupby(res4.cod_pdr) \
					.agg(F.sum( (res4.sumMeasure / res4.Profk) * least(lit(1), res4.Profnk) *  res4.Profank ).alias("CA")) \
					.withColumn("dateStart", lit(dateStart)) \
					.withColumn("dateEnd", lit(dateEnd))


			#result.show()

			#result.write.parquet("/user/acu/au/misure_gas_au/gas_ca_altrefreq", 'overwrite')
			pathHDFS = constants.PATHHDFS_RESULT_CA_ALTRFREQ
			result.write.parquet(pathHDFS, 'append')
			
			#CMD_REFRESH_CA_ALTREFREQ = "MSCK REPAIR TABLE au_test.gas_ca_altrefreq"
			CMD_REFRESH_CA_ALTREFREQ = constants.CMD_REFRESH_CA_ALTREFREQ
			self.hiveContext.sql(CMD_REFRESH_CA_ALTREFREQ)


        return 0

    def CalcoloConsumoAnnuoMensileDettaglio(self, dateStart, dateEnd):
        # Funzione CA (Semplice "f2")
	# Input:  dateStart, dateEnd
        # df = sqlContext.read.parquet("file:///opt/data/GAS/tempP/tgl_parquet")

        # dateStart = datetime.strptime('2018-01-01', '%Y-%m-%d').date()
        # dateEnd = datetime.strptime('2019-01-01', '%Y-%m-%d').date()
        
        # Lettura delle dataframe conservate nel file parquet
        #dfTgl = sqlContext.read.parquet("/user/acu/au/misure_gas_au/gas_tgl_50")
        dfTgl = self.sqlCtx.read.parquet(constants.PATHHDFS_TGL)

        # TODO Inserire il caricamento della tabella Wkr
        # rddWk = ...
        rddWkschema2 = StructType([
                StructField("id", LongType(), True),
                StructField("data", StringType(), True),
                StructField("value", DoubleType(), True)
        ])

        # Converte dataElaborazione in formato Data
        dfkTgl = dfTgl.withColumn('dataElaborazione', F.to_timestamp(dfTgl.dataElaborazione)).drop(dfTgl.dataElaborazione)

        # Converte data_comp in formato Data
        dfkTgl = dfkTgl.withColumn("data_comp",expr("from_unixtime(unix_timestamp(data_comp, 'dd/MM/yyyy'))").cast("timestamp"))
      
        # Converte let_tot_prel in formato double
        dfkTgl = dfkTgl.withColumn('let_tot_prel', dfkTgl.let_tot_prel.cast(DoubleType()))

        # filtro per data inizio e data fine, ordina per data 
        dfkTgl = dfkTgl.where((dfkTgl.data_comp >= dateStart) & (dfkTgl.data_comp <= dateEnd) ).sort(dfkTgl.data_comp)
        dfWkr = dfWkr.where((dfWkr.data_com_autolet_cf >= dateStart) & (dfWkr.data_com_autolet_cf <= dateEnd) ).sort(dfWkr.data_com_autolet_cf)
       
        # Calcolo del Ce
        # filtro per mese != 8 (agosto), calcolo il valore medio
        mean = dfkTgl.where( (month(dfkTgl.data_comp) >= 5) & (month(dfkTgl.data_comp) != 8) & (month(dfkTgl.data_comp) <= 9)) \
                  .groupby(dfkTgl.cod_pdr) \
                  .agg( F.avg(dfkTgl.let_tot_prel).alias("AVG"))
        dfce = mean.join(dfkTgl, dfkTgl.cod_pdr ==  mean.cod_pdr).drop(dfkTgl.cod_pdr)
        

        # Prelevo il min tra Ck e il valore medio
        dfce2 = dfce.join(dfWk, dfce.data_comp == dfWk.dataWk).drop(dfWk.dataWk)
        dfce2 = dfce2.withColumn('Ce', least('let_tot_prel','AVG'))

        # Eseguo formula
        result = dfce2.groupby(dfce2.cod_pdr) \
                      .agg( F.sum (  (dfce2.let_tot_prel - dfce2.Ce) / dfce2.value + dfce2.Ce  ).alias("Ca")) \
                      .withColumn("dateStart", lit(dateStart)) \
                      .withColumn("dateEnd", lit(dateEnd))


        pathHDFS = constants.PATHHDFS_RESULT_CA_MENSILI
        result.write.parquet(pathHDFS, 'append')

        #CMD_REFRESH_CA_MENSILI = "MSCK REPAIR TABLE au_test.gas_ca_mensili"
        CMD_REFRESH_CA_MENSILI = constants.CMD_REFRESH_CA_MENSILI
        self.hiveContext.sql(CMD_REFRESH_CA_MENSILI)
