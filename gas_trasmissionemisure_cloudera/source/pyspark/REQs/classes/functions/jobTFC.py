import os
import zipfile
import re
import datetime
import shutil
from pyspark.sql.types import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.functions import udf, col


class JobTFC:
    def __init__(self, conf):
        now = datetime.datetime.now()

        self.conf = conf
        self.number_obligatory = conf["TFC"]["NUMBER_RECORDS_OBLICATORY"]
        self.pattern = conf["TFC"]["PATTERN_VALID_FILE"]
        self.dirDest = conf["TFC"]["WORKDIR"]
        self.dirOutput = conf["TFC"]["OUTPUT"]

        self.has_header = conf["TFC"]["HEADER_CSV"]
        self.time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
        self.nameTableHive = conf["TFC"]["TABLEHIVE"]
        self.cmdTableRefresh = conf["TFC"]["cmdTableRefresh"]
        self.table = conf["TFC"]["table"]

        self.TABLEHIVE_AMMISSIBILITA = conf["TFC"]["TABLEHIVE_AMMISSIBILITA"]
        self.TABLEHIVE_ANOMALIE = conf["TFC"]["TABLEHIVE_ANOMALIE"]
        self.database = conf["TFC"]["database"]
        self.tableAmmissibilita = conf["TFC"]["tableAmmissibilita"]
        self.tableAnomalie = conf["TFC"]["tableAnomalie"]

    def clear_table(self, sqlCtx):
        print("Clear tables")
        sqlCtx.sql("truncate table " + self.tableAmmissibilita)
        sqlCtx.sql("truncate table " + self.tableAnomalie)
        # sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        # sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAnomalie)
        self.repair_table(sqlCtx, self.tableAmmissibilita)
        self.repair_table(sqlCtx, self.tableAnomalie)

        print("clear_table completed")

    def backup_table_tmp(self, sqlCtx):
        self.backup_table__(sqlCtx, self.tableAmmissibilita, self.TABLEHIVE_AMMISSIBILITA)
        self.backup_table__(sqlCtx, self.tableAnomalie, self.TABLEHIVE_ANOMALIE)
        print("Backup temp tables")

    def backup_table(self, sqlCtx):
        self.backup_table__(sqlCtx, self.table, self.nameTableHive)

    def backup_table__(self, sqlCtx, name_table, path_hive):
        import subprocess
        date = datetime.datetime.now()
        try:
            cmd = ("hdfs dfs -ls " + path_hive + " ").split()  # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if len(filter(None, files)) > 0:
                query = "LOAD DATA INPATH '" + path_hive + "'  INTO TABLE " + name_table + "_backup PARTITION (DATA_BACKUP='" + str(
                    date.year) + str(date.month) + str(date.day) + "')"
                # print query
                sqlCtx.sql(query)

        except:
            pass

    def getWorkdir(self):
        return self.dirDest

    def valida_record(self, obj, file_name):
        """ Procedura per validare il record.
        	La struttura del file CSV: 
            COD_PDR;CAT_USO;CLASSE_PRELIEVO;TIPOL_USO;COD_PROF_PREL_STD

        	Ritorna l'oggetto: 
            VALIDO, COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD
            
        """
        record = obj
        items = record.split(";")

        # print ("Valida Record:", record)

        NUM_ROW = self.get_items(items, 0)
        DATA = self.get_items(items, 1)
        ID_REG_CLIM = self.get_items(items, 2)
        WKR = self.get_items(items, 3)

        # print ("check record: ", COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD)
        file_basename = os.path.basename(file_name)

        # Verifica se gli attributi obblicatori sono presenti
        # print (self.conf["TFC"]["PATTERN_VALID_DATA"], DATA)

        if not DATA:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA non presente)[" + record + "]", NUM_ROW

        if not WKR:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (WKR non presente)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_DATA", DATA):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA)[" + record + "]", NUM_ROW

        if not self.valid_date(DATA):
            return False, file_name, file_basename, "224", "Formato data non valido (DATA)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_ID_REG_CLIM", ID_REG_CLIM):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (ID_REG_CLIM)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_WKR", WKR):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (WKR)[" + record + "]", NUM_ROW

        return True, file_name, file_basename, DATA, int(ID_REG_CLIM), float(WKR), NUM_ROW

    def get_items(self, items_array, index):
        # import traceback

        try:
            return items_array[index]
        except Exception:
            # traceback.print_exc()
            return ""

    def validate_filename(self, s):
        """ Procedura per validare il file.
        	Verifica della nomenclatura del file: <PIVA_UdD>_TFC_<AAAAMM>_<progressivo>.csv
        
        	Ritorna True se il file e' valido
        """

        file_name = s.replace("file://", "")
        file_name = os.path.basename(file_name)

        # print("Validate result: ",file_name, bool(re.match(self.pattern, file_name)))
        return bool(re.match(self.pattern, file_name))

    def valid_date(self, date_value):
        try:
            day, month, year = date_value.split('/')
            datetime.datetime(int(year), int(month), int(day))
            return True
        except ValueError as e:
            return False

    def checkField(self, pattern, field):
        # print (self.conf["TFC"][pattern], field,  bool(re.match(self.conf["TFC"][pattern], field)))
        return not bool(re.match(self.conf["TFC"][pattern], field))

    def check_definition_record(self, header):
        return bool(re.match(self.conf["TFC"]["PATTERN_VALID_TRACC_RECORD"], header))

    def save_to_file(self, name_file, txt):
        f = open(name_file, "a+")
        f.write(txt)

    def getNewFiles(self, sqlCtx, listFiles):
        listResult = []
        for f in listFiles:
            query = "select * from  " + self.tableAmmissibilita + " where file_name like '%" + f + "%'"
            df = sqlCtx.sql(query)
            if df.count() <= 0:
                listResult.append(f)
        return listResult

    def is_zipfile(self, filename):
        fileexts = ['.zip']  # TODO da spostare nel file JSON

        file_name = filename.replace("file://", "")
        file_name = os.path.basename(file_name)
        ext = os.path.splitext(file_name)[1]

        # print("Ext: " , ext, bool(ext in fileexts))
        return bool(ext in fileexts)

    def move_file(self, filename):

        filename = filename.replace("file://", "")
        baseDir = os.path.dirname(filename) + "/"
        files_result = os.path.join(self.dirDest, filename.replace(baseDir, ""))
        try:
            if not os.path.exists(self.dirDest):
                os.makedirs(self.dirDest)
            shutil.copy2(filename, self.dirDest)
        except:
            if not os.path.exists(files_result):
                pass

    def decomprimi(self, fileSrc):
        """ Decomprime il file zip.
        Ritorna True se la decompressione e' stata eseguita altrimenti False 
        """

        fileSrc = fileSrc.replace("file://", "")
        dirDest = self.dirDest
        # print("directory destination: ", dirDest, fileSrc)

        try:
            file_obj = zipfile.ZipFile(fileSrc, "r")
            file_obj.extractall(dirDest)
        except:
            return False

        return True

    def extra(self, sqlCtx, dataframe, rcugas_massivo_dataframe, rcugas_connessioni_distr, rcugas_tmp_va1):
        print("extra")
        return dataframe, None

    def save_data_anomalie(self, sqlCtx, rdd):
        if rdd is None:
            return

        date = datetime.datetime.now()

        # dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema_ANOMALIE_FILE_DATA())
        rdd = rdd.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        rdd = rdd.withColumn('tipo_file', lit('TFC'))

        # rdd.show(truncate = False)

        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')

        self.repair_table(sqlCtx, self.tableAnomalie)
        # sqlCtx.sql("MSCK REPAIR TABLE " + self.tableAnomalie + "  SYNC PARTITIONS")
        # dataFrame.foreach(self.save_csv_anom)

    def repair_table(self, sqlCtx, name_table):
        query = "MSCK REPAIR TABLE " + name_table
        sqlCtx.sql(query)

    def getHeaderCodeError(self):
        # code, message = function.getHeaderCodeError()
        return "200", "Il file non rispetta la struttura prevista - Nome del file non conforme"

    def generate_dataframe_to_csv(self, sqlCtx):
        print("Union dataframe OK - KO TFC")
        date = datetime.datetime.now()
        # query = "SELECT * FROM " + self.database + ".aggregazioni_amm_gas_tfc where data_import = '" +  str(date.year) + str(date.month) + str(date.day) + "' and tipo_file='TFC' and file_name = '" + filename + "'"
        query = "SELECT * FROM settle_gas.aggregazioni_amm_gas_tfc where data_import = '" + str(date.year) + str(
            date.month) + str(date.day) + "' and tipo_file='TFC'"
        print("union dataframe query:{}".format(query))

        dataframe = sqlCtx.sql(query)
        # dataframe.show(truncate = False)
        return dataframe

    def calculate_id_pratica(self, sqlCtx, dataframe):
        return dataframe

    def get_filename_amm(self, filename_input):
        now = datetime.datetime.now()
        time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)

        return filename_input.replace(".csv", "") + "_AMM_" + time_stamp + ".csv"

    def save_data(self, sqlCtx, rdd, partitionTableHive=None):
        """ Procedura usata per savare i dati nella tabella HIVE 
            sqlCtx: Context Spark
            rdd: rdd Table 
            cmdTableRefres: command to refresh Table
            partitionTableHive: list partition field 
        """
        print("Backup")

        self.backup_table(sqlCtx)
        # self.clear_table(sqlCtx)

        # rdd.foreach(self.stampa)
        # dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema())
        #
        # if (partitionTableHive != None):
        #	dataFrame.write.partitionBy(partitionTableHive).parquet(self.nameTableHive, 'append')
        # else:
        #	dataFrame.write.parquet(self.nameTableHive, 'append')

        rdd = rdd.withColumn('GIORNO_RIFERIMENTO', col('DATA'))
        # rdd = rdd.withColumn('regione',col( 'ID_REG_CLIM' ))

        print("Scrittura in {}".format(self.nameTableHive))
        rdd.write.parquet(self.nameTableHive, 'append')

        sqlCtx.sql(self.cmdTableRefresh)

    def save_data_anomalie_file(self, sqlCtx, rdd):
        date = datetime.datetime.now()

        rdd = rdd.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        rdd = rdd.withColumn('tipo_file', lit('TFC'))

        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')

        # self.repair_table(sqlCtx, self.tableAnomalieF)

    def save_ammissibilita_csv(self, dataFrame):
        """ Procedura usata per savare i dati nella tabella HIVE 
            sqlCtx: Context Spark
            rdd: rdd Table 
            cmdTableRefres: command to refresh Table
            partitionTableHive: list partition field 
        """

        date = datetime.datetime.now()

        # Creazione Id
        # res = dataFrame.withColumn("NUM_RIGA", monotonicallyIncreasingId())
        res = dataFrame
        res = res.withColumn("COD_TIPO_FILE", lit('TFC'))
        # res = res.withColumn("PIVA_UTENTE", self.get_piva(col('FILE_NAME')))

        funcPiva = udf(lambda file_name: self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid: self.get_verifica_amm(valid), StringType())
        res = res.withColumn('PIVA_UTENTE', funcPiva(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM', funcVERIFICA_AMM(col('valid')))
        res = res.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        res = res.withColumn('tipo_file', lit('TFC'))

        # res.show()
        print("save table: ", self.TABLEHIVE_AMMISSIBILITA)
        res.write.parquet(self.TABLEHIVE_AMMISSIBILITA, 'append')

    def get_verifica_amm(self, valid):
        if (valid):
            return 'Y'
        else:
            return 'N'

    def get_piva(self, filename):
        file_name = os.path.basename(filename.replace("file://", ""))

        items = file_name.split("_")
        pIvaUdD = items[0]

        return pIvaUdD

    def save_anomalie_csv(self, dataFrame):
        date = datetime.datetime.now()

        # dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema_ANOMALIE_FILE_DATA())
        rdd = rdd.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        rdd = rdd.withColumn('tipo_file', lit('TFC'))
        # rdd.show(truncate = False)

        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')
        # dataFrame.foreach(self.save_csv_anom)

    def save_to_csv(self, sqlCtx, dataframe):
        print("******************  Save to csv")
        dataframe = dataframe.withColumn("NUM_RIGA", expr("CAST(NUM_RIGA AS INTEGER)")).distinct().orderBy('num_riga')
        # dataframe.show(truncate=False)
        for it in dataframe.collect():
            self.save_csv_amm(it)
        # dataframe.foreach(self.save_csv_anom)

    def save_csv_amm(self, x):
        # 
        # 0 valid
        # 1 FILE_NAME
        # 2 FILE_NAME_REL
        # 3 COD_PDR
        # 4 CAT_USO
        # 5 CLASSE_PRELIEVO
        # 6 TIPOL_USO
        # 7 COD_PROF_PREL_STD
        # 8 COD_TIPO_FILE
        # 9 PIVA_UTENTE
        # 10 VERIFICA_AMM
        # 11 COD_CAUSALE
        # 12 DESCRIZIONE
        # 13 tipo_file
        # 14 num_riga
        # 15 data_import
        # NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE

        txt = ";".join([str(x[14]), str(x[8]), str(x[9]), str(x[10]), str(x[11]), str(x[12])]) + "\n"
        path_file = x[1]

        name_file = path_file.replace("file://" + self.dirDest, "").replace(".csv", "") + \
                    "_AMM_" + \
                    self.time_stamp + \
                    ".csv"

        f = open(name_file, "a+")
        f.write(txt)

    def save_csv_anom(self, x):
        # NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE

        txt = ";".join([str(x[14]), str(x[7]), str(x[8]), str(x[9]), str(x[10]), str(x[11])]) + "\n"
        path_file = x[1].replace("file://", "")
        name_file = path_file.replace("file:" + self.dirDest, "").replace(".csv", "") + \
                    "_ANOMALIE_" + \
                    self.time_stamp + \
                    ".csv"

        f = open(name_file, "a+")
        f.write(txt)

    def getSchema(self):
        return self.getSchema_AMMISSIBILITA_FILE()

    def get_duplicate(self, sqlCtx, dataframe):
        return dataframe, None

    def get_rcu_1(self, sqlCtx, dataframe, dataframe_rcumassivo):
        return dataframe, None

    def get_rcu_2(self, sqlCtx, dataframe, dataframe_rcumassivo):
        return dataframe, None

    def get_rcu_rcu_udd(self, sqlCtx, dataframe, dataframe_rcumassivo):
        return dataframe, None

    def load_tables(self, sqlCtx):
        query_rcugas_massivo = "select t_codice_pdr, from_unixtime(unix_timestamp(DATA_FINE_FOR , 'yyyy-MM-dd')) as DATA_FINE_FOR, piva_udd   from rcugas.rcugas_massivo_p "
        query_rcuazienda = "select rcu_azienda_p.t_piva from rcu.rcu_azienda_p"

        print("Load RCU MASSIVO")
        dataframe_rcumassivo = sqlCtx.sql(query_rcugas_massivo).cache()

        print("Load RCU Azienda")
        dataframe_rcuazienda = sqlCtx.sql(query_rcuazienda).cache()

        return dataframe_rcumassivo, dataframe_rcuazienda, None, None

    def check_rcu_filename(self, sqlCtx, dataframe, dataframe_rcuazienda):
        if dataframe is None:
            return dataframe, None

        sqlCtx.registerDataFrameAsTable(dataframe, "df_PIVA_UDD")

        query1 = "with PIVAUDD as (select *, split(FILE_NAME_REL, '_')[0] as piva from df_PIVA_UDD) " \
                 " SELECT PIVAUDD.* from PIVAUDD LEFT JOIN rcu.rcu_azienda_p ON rcu_azienda_p.t_piva = piva WHERE rcu_azienda_p.t_piva IS  NULL"

        query2 = "with PIVAUDD as (select *, split(FILE_NAME_REL, '_')[0] as piva from df_PIVA_UDD) " \
                 " SELECT PIVAUDD.* from PIVAUDD LEFT JOIN rcu.rcu_azienda_p ON rcu_azienda_p.t_piva = piva WHERE rcu_azienda_p.t_piva IS NOT NULL"

        # sqlCtx.sql(query1).show()
        dataframe_OK = sqlCtx.sql(query2)
        rdd_KO = sqlCtx.sql(query1).map(lambda f: ("209",
                                                   "Non e' stata rispettata la corrispondenza delle informazioni inviate - Partita IVA presente nel nome del file non corrispondente alla Regione Sociale indicata contestualmente all'accreditamento al SII",
                                                   f[1], f[6]))

        dataframe_KO = sqlCtx.createDataFrame(
            rdd_KO,
            self.getSchema_ANOMALIE_FILE())

        return dataframe_OK, dataframe_KO

    def stampa(self, x):
        print("*** x: ", x)

    def save_data_ammissiblita(self, sqlCtx, dataFrame):
        if dataFrame is None:
            return dataframe, None

        date = datetime.datetime.now()

        # Creazione Id
        res = dataFrame
        res = res.withColumn("COD_TIPO_FILE", lit('TFC'))

        funcPiva = udf(lambda file_name: self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid: self.get_verifica_amm(valid), StringType())
        res = res.withColumn('PIVA_UTENTE', funcPiva(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM', funcVERIFICA_AMM(col('valid')))
        res = res.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        res = res.withColumn('tipo_file', lit('TFC'))

        # res.show(truncate = False)
        print("save table: ", self.TABLEHIVE_AMMISSIBILITA)
        res.write.parquet(self.TABLEHIVE_AMMISSIBILITA, 'append')
        # sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        self.repair_table(sqlCtx, self.tableAmmissibilita)

    def getSchema_AMMISSIBILITA_FILE(self):
        return StructType([
            StructField("valid", BooleanType(), True),
            StructField("FILE_NAME", StringType(), True),
            StructField("FILE_NAME_REL", StringType(), True),
            StructField("DATA", StringType(), True),
            StructField("ID_REG_CLIM", IntegerType(), True),
            StructField("WKR", DoubleType(), True),
            StructField("NUM_RIGA", StringType(), True)
        ])

    def getSchema_ANOMALIE_FILE_DATA(self):
        return StructType([
            StructField("valid", BooleanType(), True),
            StructField("FILE_NAME", StringType(), True),
            StructField("COD_CAUSALE", StringType(), True),
            StructField("DESCRIZIONE", StringType(), True)

        ])

    def getSchema_ANOMALIE_FILE(self):
        return StructType([
            StructField("COD_CAUSALE", StringType(), True),
            StructField("DESCRIZIONE", StringType(), True),
            StructField("File", StringType(), True),
            StructField("NUM_RIGA", StringType(), True)

        ])
