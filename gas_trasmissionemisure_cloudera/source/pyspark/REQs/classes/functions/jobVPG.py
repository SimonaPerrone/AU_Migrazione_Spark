import os
import zipfile
import re
import datetime
import shutil
from pyspark.sql.types import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.functions import udf, col


class JobVPG:
    def __init__(self, conf):
        now = datetime.datetime.now()

        self.conf = conf
        self.number_obligatory = conf["VPG"]["NUMBER_RECORDS_OBLICATORY"]
        self.pattern = conf["VPG"]["PATTERN_VALID_FILE"]
        self.dirDest = conf["VPG"]["WORKDIR"]
        self.dirOutput = conf["VPG"]["OUTPUT"]
        self.has_header = conf["VPG"]["HEADER_CSV"]
        self.time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
        self.nameTableHive = conf["VPG"]["TABLEHIVE"]
        self.cmdTableRefresh = conf["VPG"]["cmdTableRefresh"]
        self.table = conf["VPG"]["table"]

        self.TABLEHIVE_AMMISSIBILITA = conf["VPG"]["TABLEHIVE_AMMISSIBILITA"]
        self.TABLEHIVE_ANOMALIE = conf["VPG"]["TABLEHIVE_ANOMALIE"]
        self.database = conf["VPG"]["database"]
        self.tableAmmissibilita = conf["VPG"]["tableAmmissibilita"]
        self.tableAnomalie = conf["VPG"]["tableAnomalie"]

    def clear_table(self, sqlCtx):
        print("Clear tables")
        sqlCtx.sql("truncate table " + self.tableAmmissibilita)
        sqlCtx.sql("truncate table " + self.tableAnomalie)
        # sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        # sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAnomalie)
        self.repair_table(sqlCtx, self.tableAmmissibilita)
        self.repair_table(sqlCtx, self.tableAnomalie)

        print("clear_table completed")

    def save_data_anomalie_file(self, sqlCtx, rdd):
        date = datetime.datetime.now()

        rdd = rdd.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        rdd = rdd.withColumn('tipo_file', lit('VPG'))

        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')

        # self.repair_table(sqlCtx, self.tableAnomalieF)

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
            cmd = ("hdfs dfs -ls  " + path_hive + " ").split()  # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if (len(filter(None, files)) > 0):
                query = "LOAD DATA INPATH '" + path_hive + "' INTO TABLE " + name_table + "_backup PARTITION (DATA_BACKUP='" + str \
                    (date.year) + str(date.month) + str(date.day) + "')"
                # print query
                sqlCtx.sql(query)
        except:
            pass

    def load_tables(self, sqlCtx):
        query_rcugas_massivo = "select t_codice_pdr, from_unixtime(unix_timestamp(DATA_FINE_FOR , 'yyyy-MM-dd')) as DATA_FINE_FOR, piva_udd   from rcugas.rcugas_massivo_p "
        query_rcuazienda = "select rcu_azienda_p.t_piva from rcu.rcu_azienda_p"

        print("Load RCU MASSIVO")
        dataframe_rcumassivo = sqlCtx.sql(query_rcugas_massivo).cache()

        print("Load RCU Azienda")
        dataframe_rcuazienda = sqlCtx.sql(query_rcuazienda).cache()

        return dataframe_rcumassivo, dataframe_rcuazienda, None, None

    def getWorkdir(self):
        return self.dirDest

    def getHeaderCodeError(self):
        # code, message = function.getHeaderCodeError()
        return "015", "La richiesta non e' eseguibile - Intestazione file non corretta"

    def check_definition_record(self, header):
        # print "Check header"
        # print ("PATTERN_VALID_TRACC_RECORD: ", self.conf["VPG"]["PATTERN_VALID_TRACC_RECORD"], bool(re.match(self.conf["VPG"]["PATTERN_VALID_TRACC_RECORD"], header)))
        return bool(re.match(self.conf["VPG"]["PATTERN_VALID_TRACC_RECORD"], header))

    def valida_record(self, obj, file_name):
        """ Procedura per validare il record.
        	La struttura del file CSV: 
            COD_PDR;CAT_USO;CLASSE_PRELIEVO;TIPOL_USO;COD_PROF_PREL_STD

        	Ritorna l'oggetto: 
            VALIDO, COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD
            
        """

        record = obj
        # print ("Valida Record:", record)

        items = record.split(";")

        # if len(items) < 24:
        #     return (False, file_name, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati ()[" + record + "]")  

        NUM_ROW = self.get_items(items, 0)
        GIORNO_RIFERIMENTO = self.get_items(items, 1)
        C1_A1 = self.get_items(items, 2)
        C1_B1 = self.get_items(items, 3)
        C1_C1 = self.get_items(items, 4)
        C1_D1 = self.get_items(items, 5)
        C1_E1 = self.get_items(items, 6)
        C1_F1 = self.get_items(items, 7)
        C1_A2 = self.get_items(items, 8)
        C1_B2 = self.get_items(items, 9)
        C1_C2 = self.get_items(items, 10)
        C1_D2 = self.get_items(items, 11)
        C1_E2 = self.get_items(items, 12)
        C1_F2 = self.get_items(items, 13)
        C1_A3 = self.get_items(items, 14)
        C1_B3 = self.get_items(items, 15)
        C1_C3 = self.get_items(items, 16)
        C1_D3 = self.get_items(items, 17)
        C1_E3 = self.get_items(items, 18)
        C1_F3 = self.get_items(items, 19)
        C2 = self.get_items(items, 20)
        C4 = self.get_items(items, 21)
        T1_1 = self.get_items(items, 22)
        T1_2 = self.get_items(items, 23)
        T1_3 = self.get_items(items, 24)

        # print ("check record: ", GIORNO_RIFERIMENTO, C1_A1, C1_B1, C1_C1, C1_D1, C1_E1, C1_F1, C1_A2, C1_B2, C1_C2, C1_D2, C1_E2, C1_F2, C1_A3, C1_B3, C1_C3, C1_D3, C1_E3, C1_F3, C2, C4, T1_1, T1_2, T1_3 )
        file_basename = os.path.basename(file_name)

        # Verifica se gli attributi obblicatori sono presenti
        # if not GIORNO_RIFERIMENTO:
        #    return (False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA non presente)[" + record + "]", NUM_ROW) 

        if self.checkField("PATTERN_VALID_GIORNO_RIFERIMENTO", GIORNO_RIFERIMENTO):
            return False, file_name, file_basename, "224", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (GIORNO_RIFERIMENTO)[" + record + "]", NUM_ROW

        if not self.valid_date(GIORNO_RIFERIMENTO):
            return False, file_name, file_basename, "224", "Formato data non valido (GIORNO_RIFERIMENTO)[" + record + "]", NUM_ROW

        # return (True, file_name, GIORNO_RIFERIMENTO, int(1), int(1), int(1), int(1), int(1), int(1),int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1), int(1))

        if not C1_A1:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_A1 non presente)[" + record + "]", NUM_ROW

        if not C1_B1:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_B1 non presente)[" + record + "]", NUM_ROW

        if not C1_C1:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_C1 non presente)[" + record + "]", NUM_ROW

        if not C1_D1:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_D1 non presente)[" + record + "]", NUM_ROW

        if not C1_E1:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_E1 non presente)[" + record + "]", NUM_ROW

        if not C1_F1:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_F1 non presente)[" + record + "]", NUM_ROW

        if not C1_A2:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_A2 non presente)[" + record + "]", NUM_ROW

        if not C1_B2:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_B2 non presente)[" + record + "]", NUM_ROW

        if not C1_C2:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_C2 non presente)[" + record + "]", NUM_ROW

        if not C1_D2:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_D2 non presente)[" + record + "]", NUM_ROW

        if not C1_E2:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_E2 non presente)[" + record + "]", NUM_ROW

        if not C1_F2:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_F2 non presente)[" + record + "]", NUM_ROW

        if not C1_A3:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_A3 non presente)[" + record + "]", NUM_ROW

        if not C1_B3:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_B3 non presente)[" + record + "]", NUM_ROW

        if not C1_C3:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_C3 non presente)[" + record + "]", NUM_ROW

        if not C1_D3:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_D3 non presente)[" + record + "]", NUM_ROW

        if not C1_E3:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_E3 non presente)[" + record + "]", NUM_ROW

        if not C1_F3:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_F3 non presente)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_A1", C1_A1):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_A1)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_B1", C1_B1):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_B1)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_C1", C1_C1):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_C1)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_D1", C1_D1):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_D1)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_E1", C1_E1):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_E1)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_F1", C1_F1):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_F1)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_A2", C1_A2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_A2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_B2", C1_B2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_B2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_C2", C1_C2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_C2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_D2", C1_D2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_D2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_E2", C1_E2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_E2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_F2", C1_F2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_F2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_A3", C1_A3):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_A3)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_B3", C1_B3):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_B3)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_C3", C1_C3):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_C3)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_D3", C1_D3):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_D3)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_E3", C1_E3):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_E3)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C1_F3", C1_F3):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C1_F3)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C2", C2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_C4", C4):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (C4)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_T1_1", T1_1):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (T1_1)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_T1_2", T1_2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (T1_2)[" + record + "]", NUM_ROW

        if self.checkField("PATTERN_VALID_T1_3", T1_3):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (T1_3)[" + record + "]", NUM_ROW

        return True, file_name, file_basename, GIORNO_RIFERIMENTO, float(C1_A1), float(C1_B1), float(C1_C1), float \
            (C1_D1), float(C1_E1), float(C1_F1), float(C1_A2), float(C1_B2), float(C1_C2), float(C1_D2), float \
                   (C1_E2), float(C1_F2), float(C1_B3), float(C1_A3), float(C1_C3), float(C1_D3), float(C1_E3), float \
                   (C1_F3), float(C2), float(C4), float(T1_1), float(T1_2), float(T1_3), NUM_ROW

    def get_items(self, items_array, index):
        # import traceback

        try:
            return items_array[index]
        except Exception:
            # traceback.print_exc()
            return ""

    def valid_date(self, date_value):
        try:
            day, month, year = date_value.split('/')
            datetime.datetime(int(year), int(month), int(day))
            return True
        except ValueError as e:
            return False

    def checkField(self, pattern, field):
        # print (self.conf["VPG"][pattern], field, bool(re.match(self.conf["VPG"][pattern], field)))
        # print (self.conf["VPG"][pattern], field,  bool(re.match(self.conf["VPG"][pattern], field)))
        return not bool(re.match(self.conf["VPG"][pattern], field))

    def save_to_file(self, name_file, txt):
        f = open(name_file, "a+")
        f.write(txt)

    def validate_filename(self, s):
        """ Procedura per validare il file.
        	Verifica della nomenclatura del file: <PIVA_UdD>_TDS_<AAAAMM>_<progressivo>.csv
        
        	Ritorna True se il file e' valido
        """

        file_name = s.replace("file://", "")
        file_name = os.path.basename(file_name)

        # print("Validate result: ",file_name, bool(re.match(self.pattern, file_name)))
        return bool(re.match(self.pattern, file_name))

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
        return (dataframe, None)

    def calculate_id_pratica(self, sqlCtx, dataframe):
        return dataframe

    def get_filename_amm(self, filename_input):
        now = datetime.datetime.now()
        time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)

        return filename_input.replace(".csv", "") + "_AMM_" + time_stamp + ".csv"

    def save_data(self, sqlCtx, rdd, cmdTableRefresh="", partitionTableHive=None):
        """ Procedura usata per savare i dati nella tabella HIVE 
            sqlCtx: Context Spark
            rdd: rdd Table 
            cmdTableRefres: command to refresh Table
            partitionTableHive: list partition field 
        """
        print("Backup")

        self.backup_table(sqlCtx)

        rdd = rdd.withColumn('data', col('GIORNO_RIFERIMENTO'))

        # self.clear_table(sqlCtx)
        # rdd.foreach(self.stampa)
        # dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema())
        # #print (self.nameTableHive)
        # #dataFrame.show()
        # 
        # if (partitionTableHive != None):
        # 	dataFrame.write.partitionBy(partitionTableHive).parquet(self.nameTableHive, 'append')
        # else:
        # 	dataFrame.write.parquet(self.nameTableHive, 'append')
        print("Scrittura in {}".format(self.nameTableHive))
        rdd.write.parquet(self.nameTableHive, 'append')

        # dataFrame.show()
        sqlCtx.sql(self.cmdTableRefresh)

    def save_ammissibilita_csv(self, dataFrame):
        date = datetime.datetime.now()

        # Creazione Id
        # res = dataFrame.withColumn("NUM_RIGA", monotonicallyIncreasingId())
        res = dataFrame
        res = res.withColumn("COD_TIPO_FILE", lit('VPG'))
        # res = res.withColumn("PIVA_UTENTE", self.get_piva(col('FILE_NAME')))

        funcPiva = udf(lambda file_name: self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid: self.get_verifica_amm(valid), StringType())
        res = res.withColumn('PIVA_UTENTE', funcPiva(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM', funcVERIFICA_AMM(col('valid')))
        res = res.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        res = res.withColumn('tipo_file', lit('VPG'))

        # res.show(truncate = False)
        print("save table: ", self.TABLEHIVE_AMMISSIBILITA)
        res.write.parquet(self.TABLEHIVE_AMMISSIBILITA, 'append')
        # sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        self.repair_table(sqlCtx, self.tableAmmissibilita)

    def save_anomalie_csv(self, dataFrame):
        date = datetime.datetime.now()

        # dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema_ANOMALIE_FILE_DATA())
        rdd = rdd.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        rdd = rdd.withColumn('tipo_file', lit('VPG'))
        # rdd.show(truncate = False)

        print("Scrittura su tabella: {}".format(self.TABLEHIVE_ANOMALIE))
        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')
        # dataFrame.foreach(self.save_csv_anom)

    def repair_table(self, sqlCtx, name_table):
        query = "MSCK REPAIR TABLE " + name_table
        sqlCtx.sql(query)

    def save_to_csv(self, sqlCtx, dataframe):
        dataframe = dataframe.withColumn("NUM_RIGA", expr("CAST(NUM_RIGA AS INTEGER)")).distinct().orderBy('num_riga')
        for it in dataframe.collect():
            self.save_csv_amm(it)

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

    def get_data_year(self, filename):
        file_name = os.path.basename(filename.replace("file://", ""))

        items = file_name.split("_")
        data1 = items[2][:4]
        data2 = items[2][4:]

        return data1, data2

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
        # |tipo_file|num_riga|data_import|
        # NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        # valid=u'true', 
        # file_name=u'/mnt/Settlement/TSG/TSG2/TSG2_10238291008/2019/09/10238291008_VPG_20192020_02.csv', 
        # file_name_rel=u'10238291008_VPG_20192020_02.csv', 
        # cod_tipo_file=u'VPG', 
        # piva_utente=u'10238291008', 
        # verifica_amm=u'Y', 
        # cod_causale=u'', 
        # descrizione=u'', 
        # tipo_file=u'VPG', 
        # data_import=u'2019916',
        # NUM_RIGA=2))
        txt = ";".join([
            str(x.asDict()['NUM_RIGA']),
            str(x.asDict()['cod_tipo_file']),
            str(x.asDict()['piva_utente']),
            str(x.asDict()['verifica_amm']),
            str(x.asDict()['cod_causale']),
            str(x.asDict()['descrizione'])]) + "\n"
        path_file = x[1]

        name_file = path_file.replace("file://" + self.dirDest, "").replace(".csv", "") + \
                    "_AMM_" + \
                    self.time_stamp + \
                    ".csv"

        # print ("filename csv: ", name_file, txt)
        f = open(name_file, "a+")
        f.write(txt)

    def getSchema(self):
        return self.getSchema_AMMISSIBILITA_FILE()

    def get_duplicate(self, sqlCtx, dataframe):
        # dataframe.show()
        return dataframe, None

    def check_data_giorno_rif(self, filename, giorno_rif):
        data_year1 = int(self.get_data_year(filename)[0])
        data_year2 = int(self.get_data_year(filename)[1])
        giorno_rif_year = int(giorno_rif[-4:])
        print(filename, data_year1, data_year2, giorno_rif_year)

        if data_year1 <= giorno_rif_year <= data_year2:
            return True

        return False

        pass

    def get_rcu_1(self, sqlCtx, dataframe, dataframe_rcumassivo):
        if dataframe is None:
            return dataframe, None

        sqlCtx.registerDataFrameAsTable(dataframe, "my_table")

        query1 = " with table1 as (select *, cast(substr(split(FILE_NAME_REL, '_')[2], 1, 4) as int)  as data1,cast(substr(split(FILE_NAME_REL, '_')[2], 5, 4) as int)  as data2, cast(substr(GIORNO_RIFERIMENTO, 7, 4) as int) as GIORNO_RIFERIMENTO_year from my_table)" \
                 + " SELECT table1.* FROM table1 " \
                 + " WHERE data1 > GIORNO_RIFERIMENTO_year and GIORNO_RIFERIMENTO_year > data2"

        query2 = " with table1 as (select *, cast(substr(split(FILE_NAME_REL, '_')[2], 1, 4) as int)  as data1,cast(substr(split(FILE_NAME_REL, '_')[2], 5, 4) as int)  as data2, cast(substr(GIORNO_RIFERIMENTO, 7, 4) as int) as GIORNO_RIFERIMENTO_year from my_table)" \
                 + " SELECT valid,FILE_NAME, GIORNO_RIFERIMENTO,C1_A1,C1_B1,C1_C1,C1_D1,C1_E1,C1_F1,C1_A2,C1_B2,C1_C2,C1_D2,C1_E2,C1_F2,C1_A3,C1_B3,C1_C3,C1_D3,C1_E3,C1_F3,C2  ,C4  ,T1_1,T1_2,T1_3, num_riga FROM table1 " \
                 + " WHERE data1 <= GIORNO_RIFERIMENTO_year and GIORNO_RIFERIMENTO_year <= data2"

        dataframe_OK = sqlCtx.sql(query2)
        rdd_KO = sqlCtx.sql(query1).map(lambda f: ("209",
                                                   "Non e' stata rispettata la corrispondenza delle informazioni inviate - il campo GIORNO_RIFERIMENTO non risulta coerente con l'anno termico",
                                                   f[1], f[8]))

        dataframe_KO = sqlCtx.createDataFrame(
            rdd_KO,
            self.getSchema_ANOMALIE_FILE())

        print("get_rcu_1 fine")
        # dataframe_KO.show()

        return dataframe_OK, dataframe_KO

    def get_rcu_2(self, sqlCtx, dataframe, dataframe_rcumassivo):
        return dataframe, None

    def get_rcu_rcu_udd(self, sqlCtx, dataframe, dataframe_rcumassivo):
        return dataframe, None

    def check_rcu_filename(self, sqlCtx, dataframe, dataframe_rcuazienda):
        if dataframe is None:
            return dataframe, None

        print("Check rcu filename")

        sqlCtx.registerDataFrameAsTable(dataframe, "df_PIVA_UDD")
        # dataframe.show()

        query1 = "with PIVAUDD as (select *, split(FILE_NAME_REL, '_')[0] as piva from df_PIVA_UDD) " \
                 " SELECT PIVAUDD.* from PIVAUDD LEFT JOIN rcu.rcu_azienda_p ON rcu_azienda_p.t_piva = piva WHERE rcu_azienda_p.t_piva IS  NULL"

        query2 = "with PIVAUDD as (select *, split(FILE_NAME_REL, '_')[0] as piva from df_PIVA_UDD) " \
                 " SELECT PIVAUDD.* from PIVAUDD LEFT JOIN rcu.rcu_azienda_p ON rcu_azienda_p.t_piva = piva WHERE rcu_azienda_p.t_piva IS NOT NULL"

        # print ("query1", query1)
        # print ("query2", query2)

        dataframe_OK = sqlCtx.sql(query2)
        rdd_KO = sqlCtx.sql(query1).map(lambda f: ("209",
                                                   "Non e' stata rispettata la corrispondenza delle informazioni inviate - Partita IVA presente nel nome del file non corrispondente alla Regione Sociale indicata contestualmente all'accreditamento al SII",
                                                   f[1], f[8]))

        dataframe_KO = sqlCtx.createDataFrame(
            rdd_KO,
            self.getSchema_ANOMALIE_FILE())

        # dataframe_OK.show()
        # dataframe_KO.show()

        return dataframe_OK, dataframe_KO

    def save_data_anomalie(self, sqlCtx, rdd):
        if rdd is None:
            return

        date = datetime.datetime.now()

        # dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema_ANOMALIE_FILE_DATA())
        rdd = rdd.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        rdd = rdd.withColumn('tipo_file', lit('VPG'))
        # rdd.show(truncate = False)

        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')

        self.repair_table(sqlCtx, self.tableAnomalie)
        # sqlCtx.sql("MSCK REPAIR TABLE " + self.tableAnomalie + "  SYNC PARTITIONS")
        # dataFrame.foreach(self.save_csv_anom)

    def save_data_ammissiblita(self, sc, sqlCtx, dataFrame):
        if dataFrame is None:
            return dataframe, None

        schema = StructType([
            StructField("valid", StringType(), True),
            StructField("file_name", StringType(), True),
            StructField("num_riga", StringType(), True),
        ])

        date = datetime.datetime.now()

        # Creazione Id
        # res = dataFrame.withColumn("NUM_RIGA", monotonicallyIncreasingId())
        res = sc.createDataFrame(dataFrame.map(lambda d: (d['valid'], d['FILE_NAME'], d['num_riga'])), schema)
        res = res.withColumn("COD_TIPO_FILE", lit('VPG'))

        funcPiva = udf(lambda file_name: self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid: self.get_verifica_amm(valid), StringType())
        funcFileNameRel = udf(lambda filename: os.path.basename(filename), StringType())
        res = res.withColumn('PIVA_UTENTE', funcPiva(col('FILE_NAME')))
        res = res.withColumn('file_name_rel', funcFileNameRel(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM', funcVERIFICA_AMM(col('valid')))
        res = res.withColumn('data_import', lit(str(date.year) + str(date.month) + str(date.day)))
        res = res.withColumn('tipo_file', lit('VPG'))

        # res.show()
        print("save table: ", self.TABLEHIVE_AMMISSIBILITA)
        res.write.parquet(self.TABLEHIVE_AMMISSIBILITA, 'append')
        # sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        # self.repair_table(sqlCtx, self.tableAmmissibilita)

    def generate_dataframe_to_csv(self, sqlCtx):
        print("Union dataframe OK - KO")
        date = datetime.datetime.now()
        query = "SELECT * FROM " + self.database + ".aggregazioni_amm_gas_vpg where data_import = '" + str(
            date.year) + str(date.month) + str(date.day) + "' and tipo_file='VPG'"

        print("Query: {}".format(query))
        dataframe = sqlCtx.sql(query)
        print("Query completata")
        # dataframe.show(truncate = False)
        return dataframe

    def getSchema_AMMISSIBILITA_FILE(self):
        return StructType([
            StructField("valid", BooleanType(), True),
            StructField("FILE_NAME", StringType(), True),
            StructField("FILE_NAME_REL", StringType(), True),
            StructField("GIORNO_RIFERIMENTO", StringType(), True),
            StructField("C1_A1", DoubleType(), True),
            StructField("C1_B1", DoubleType(), True),
            StructField("C1_C1", DoubleType(), True),
            StructField("C1_D1", DoubleType(), True),
            StructField("C1_E1", DoubleType(), True),
            StructField("C1_F1", DoubleType(), True),
            StructField("C1_A2", DoubleType(), True),
            StructField("C1_B2", DoubleType(), True),
            StructField("C1_C2", DoubleType(), True),
            StructField("C1_D2", DoubleType(), True),
            StructField("C1_E2", DoubleType(), True),
            StructField("C1_F2", DoubleType(), True),
            StructField("C1_A3", DoubleType(), True),
            StructField("C1_B3", DoubleType(), True),
            StructField("C1_C3", DoubleType(), True),
            StructField("C1_D3", DoubleType(), True),
            StructField("C1_E3", DoubleType(), True),
            StructField("C1_F3", DoubleType(), True),
            StructField("C2", DoubleType(), True),
            StructField("C4", DoubleType(), True),
            StructField("T1_1", DoubleType(), True),
            StructField("T1_2", DoubleType(), True),
            StructField("T1_3", DoubleType(), True),
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
            StructField("valid", BooleanType(), True),
            StructField("COD_CAUSALE", StringType(), True),
            StructField("DESCRIZIONE", StringType(), True),
            StructField("File", StringType(), True),
            StructField("NUM_RIGA", StringType(), True)

        ])
