import os
from zipfile import *
import re
import shutil
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.functions import udf,col
import threading
from pyspark.sql.functions import year, month, dayofmonth
import pyspark.sql.functions as f
from pyspark.sql.functions import datediff, to_date
import time

table_massivo = "rcugas.rcugas_massivo_p"
parquet_amm_tmp = '/user/hive/warehouse/settle_gas.db/AMM_TMP'

#DEBUG
#table_massivo = "au_test.test_massivo"
#parquet_amm_tmp = '/user/hive/warehouse/settle_gas.db/AMM_TMP'
#table_massivo = "rcugas.rcugas_massivo_test"

class JobSAG:
    def __init__(self, conf):
        now = datetime.datetime.now()

        self.conf = conf
        self.number_obligatory = conf["SAG"] ["NUMBER_RECORDS_OBLICATORY"] 
        self.pattern = conf["SAG"]["PATTERN_VALID_FILE"]
        self.dirDest = conf["SAG"]["WORKDIR"]
        self.dirOutput =  conf["SAG"]["OUTPUT"]
        self.has_header = conf["SAG"]["HEADER_CSV"]
        self.time_stamp =  str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
        self.nameTableHive = conf["SAG"]["TABLEHIVE"]
        self.cmdTableRefresh = conf["SAG"]["cmdTableRefresh"]
        self.table = conf["SAG"]["table"]

        self.TABLEHIVE_AMMISSIBILITA = conf["SAG"]["TABLEHIVE_AMMISSIBILITA"]
        self.TABLEHIVE_ANOMALIE = conf["SAG"]["TABLEHIVE_ANOMALIE"]
        self.database = conf["SAG"]["database"]
        self.tableAmmissibilita = conf["SAG"]["tableAmmissibilita"]
        self.tableAnomalie = conf["SAG"]["tableAnomalie"]
        self.TABLEHIVE_ANOMALIE_F = conf["SAG"]["TABLEHIVE_ANOMALIE_F"]
        self.tableAnomalieF = conf["SAG"]["tableAnomalieF"]
        self.directory_zip = conf["SAG"]["directory_zip"]

        self.NUM_RIGA_CONST = 10

    def clear_table(self, sqlCtx):
        print ("Clear tables")
        sqlCtx.sql("truncate table " + self.tableAmmissibilita)
        sqlCtx.sql("truncate table " + self.tableAnomalie)
        #sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        #sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAnomalie)
        self.repair_table(sqlCtx, self.tableAmmissibilita)
        self.repair_table(sqlCtx, self.tableAnomalie)

        print ("clear_table completed")

    def backup_table_tmp(self, sqlCtx):
        print ("Backup temp tables")
        self.backup_table__(sqlCtx, self.tableAmmissibilita, self.TABLEHIVE_AMMISSIBILITA)
        self.backup_table__(sqlCtx, self.tableAnomalie, self.TABLEHIVE_ANOMALIE)

    def backup_table(self, sqlCtx):
        self.backup_table__(sqlCtx, self.table, self.nameTableHive)

    def backup_table__(self, sqlCtx, name_table, path_hive):
        import subprocess
        date = datetime.datetime.now()
        try:
            cmd = ("hdfs dfs -ls "+ path_hive + " ").split() # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if len(filter(None, files)) > 0:
                query = "LOAD DATA INPATH '" + path_hive + "' INTO TABLE " + name_table + "_backup PARTITION (DATA_BACKUP='" + str(date.year) + str(date.month) + str(date.day) + "')"
                # print(query)
                sqlCtx.sql(query)
        except:
            pass    
        
    def getWorkdir(self):
        return self.dirDest

    def getHeaderCodeError(self):
        return "015", "La richiesta non e' eseguibile - Intestazione file non corretta"

    def check_definition_record(self, header):
        return bool(re.match(self.conf["SAG"]["PATTERN_VALID_TRACC_RECORD"], header))

    def valida_record(self, obj, file_name):
        """ Procedura per validare il record.
        	La struttura del file CSV: 
            COD_PDR;CAT_USO;CLASSE_PRELIEVO;TIPOL_USO;COD_PROF_PREL_STD

        	Ritorna l'oggetto: 
            VALIDO, COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD
            
        """
        record = obj
        # Verifica se il record e' empty
        items = record.split(";")
       
        NUM_ROW             = self.get_items(items,0)
        COD_PDR             = self.get_items(items,1)
        COD_REMI            = self.get_items(items,2)
        CONS_ANN            = self.get_items(items,3)
        COD_PROF_PREL_STD   = self.get_items(items,4)
        DATA_DS             = self.get_items(items,5)
        DATA_MIS1           = self.get_items(items,6)
        DATA_MIS2           = self.get_items(items,7)
        

        file_basename = os.path.basename(file_name)

        if not COD_PDR:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (COD_PDR non presente) (COD_PDR)[" + record + "]", NUM_ROW
        if not COD_REMI:
            return False, file_name, file_basename, "297", "Campo COD_REMI non compilato o non correttamente compilato (COD_REMI)[" + record + "]", NUM_ROW
        if not CONS_ANN:
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (CONS_ANN non presente) (CONS_ANN)[" + record + "]", NUM_ROW
        #if not DATA_DS:
        #    return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_DS non presente se presente COD_PROF_PREL_STD) (CONS_ANN)[" + record + "]", NUM_ROW)  
        
        
        #if not DATA_MIS1:
        #    return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS1 non presente se presente COD_PROF_PREL_STD) (CONS_ANN)[" + record + "]", NUM_ROW)  
        #if not DATA_MIS2:
        #    return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS2 non presente se presente COD_PROF_PREL_STD) (CONS_ANN)[" + record + "]", NUM_ROW)  
         
        # Verifica se gli attributi obbligatori sono presenti
       
        if self.checkField("PATTERN_VALID_COD_PDR", COD_PDR): 
            return False, file_name, file_basename, "238", "Codice PdR strutturalmente non corretto (COD_PDR)[" + record + "]", NUM_ROW
        
        if self.checkField("PATTERN_VALID_COD_REMI", COD_REMI): 
            return False, file_name, file_basename, "297", "Campo COD_REMI non compilato o non correttamente compilato (COD_REMI)[" + record + "]", NUM_ROW
        
        if self.checkField("PATTERN_VALID_CONS_ANN", CONS_ANN): 
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (CONS_ANN non coretto) [" + record + "]", NUM_ROW
        
        if COD_PROF_PREL_STD:
            if self.checkField("PATTERN_VALID_COD_PROF_PREL_STD", COD_PROF_PREL_STD): 
                return False, file_name, file_basename, "355", "COD_PROF_PREL_STD non conforme [" + record + "]", NUM_ROW

        if not DATA_DS and COD_PROF_PREL_STD:
            return False, file_name, file_basename, "004", "I campi obbligatori non son ostati compilati o non sono stati correttamente compilati (DATA_DS obbligatoria se presente COD_PROF_PREL_STD)[" + record + "]", NUM_ROW

        if COD_PROF_PREL_STD:
            if self.checkField("PATTERN_VALID_DATA_DS", DATA_DS): 
                return False, file_name, file_basename, "224", "Formato data non valido  (DATA_DS)[<" + DATA_DS + ">]", NUM_ROW
            
        if self.checkField("PATTERN_VALID_DATA_MIS1", DATA_MIS1): 
            return False, file_name, file_basename, "224", "Formato data non valido (DATA_MIS1)[" + record + "]", NUM_ROW
        
        if self.checkField("PATTERN_VALID_DATA_MIS2", DATA_MIS2): 
            return False, file_name, file_basename, "224", "Formato data non valido (DATA_MIS2)[" + record + "]", NUM_ROW
        
        if DATA_DS:
            #print file_name, NUM_ROW, record 
            if not self.validate(DATA_DS):
                return False, file_name, file_basename, "224", "Formato data non valido  (DATA_DS)[" + record + "]", NUM_ROW

        #MODIFICA vedi email
        #11 nuovo: DATA_MIS1 e DATA_MIS2 devono entrambe valorizzati o entrambi null
        if (DATA_MIS1 and not DATA_MIS2) or (not DATA_MIS1 and DATA_MIS2):
            return False, file_name, file_basename, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS1 e DATA_MIS2 devono essere compilati o entrambi non valorizzati)[" + record + "]", NUM_ROW

        if DATA_MIS1:
            if not self.validate(DATA_MIS1):
                return False, file_name, file_basename, "224", "Formato data non valido  (DATA_MIS1)[" + record + "]", NUM_ROW

        if DATA_MIS2:
            if not self.validate(DATA_MIS2):
                return False, file_name, file_basename, "224", "Formato data non valido  (DATA_MIS2)[" + record + "]", NUM_ROW

        data_ds = DATA_DS
        data_mis1 = DATA_MIS1
        data_mis2 = DATA_MIS2

        result = (True, file_name, file_basename, COD_PDR, COD_REMI, float(CONS_ANN), COD_PROF_PREL_STD, data_ds, data_mis1 ,data_mis2 , NUM_ROW)  
        return result

    def validate(self, date_text):
        try:
            datetime.datetime.strptime(date_text, '%d/%m/%Y')
            return True
        except ValueError:
            return False

    def get_items(self, items_array, index):
        try:
            return items_array[index]
        except Exception:
            #traceback.print_exc()
            return ""

    def checkField(self, pattern, field):  
        return not bool(re.match(self.conf["SAG"][pattern], field))
  
    def save_to_file(self, name_file, txt):
        f = open(name_file,"a+")
        f.write(txt)
        f.flush()
        f.close()
        file_name = os.path.basename(name_file)
        #TIMESTAMP_NIDPRATICAORACLE_PIVAUDD_PIVADISTR_SAG1_ANNOTERMICO_PROGRESSIVO.CSV
        #SAG1_AMM_<ID_PRATICA>_<PIVADISTR>_YYYYMMDD.zip.

        #      0            1          2        3      4        5          6  
        #TIMESTAMP_NIDPRATICAORACLE_PIVAUDD_PIVADISTR_SAG1_ANNOTERMICO_PROGRESSIVO.CSV
        #      0            1          2            3        4   5    6  
        #190620160555615_137689742_12883420155_01812230223_SAG1_2020_001_AMM_" + time_stamp + ".csv"
        relative_file = os.path.basename(file_name)
        namefile_without_exc = relative_file.replace(".csv", "").replace(".CSV", "")

        try:
            n_id_pratica = namefile_without_exc.split("_")[1]
        except:
            n_id_pratica = ""

        try:
            PIVAUDD = namefile_without_exc.split("_")[2] 
        except:
            PIVAUDD = ""
        
        try:
            PIVADISTR = namefile_without_exc.split("_")[3]
        except:
            PIVADISTR = ""

        try:
            ANNO = namefile_without_exc.split("_")[5]
        except:
            ANNO = ""    

        try:
            PROGRESSIVO = namefile_without_exc.split("_")[6]
        except:
            PROGRESSIVO = ""    

        print (namefile_without_exc, n_id_pratica,PIVAUDD,PIVADISTR, ANNO ,PROGRESSIVO) 
        filename_amm = n_id_pratica + "_" + PIVAUDD + "_" + PIVADISTR + "_SAG1_" + ANNO + "_" + PROGRESSIVO +\
            "_AMM.csv"

        #print ("file_name: ", file_name)
        #file_name_zip = self.directory_zip + "REPORT_AMM/" +  os.path.splitext(file_name)[0] + ".zip"
        #file_name_zip = self.directory_zip + "REPORT_AMM/" +  os.path.splitext(filename_amm)[0] + ".zip"
        date = datetime.datetime.now()
        dataYYYMMDD = str(date.year) + str(date.month) + str(date.day)
        file_name_zip = self.directory_zip + "REPORT_AMM/SAG1_AMM_" + n_id_pratica + "_" + PIVAUDD +  "_" + PIVADISTR + "_" +  dataYYYMMDD + ".zip"

        fileOutput = os.path.dirname(name_file) + "/" + file_name

        with ZipFile(file_name_zip, 'a') as myzip:
            myzip.write(fileOutput, os.path.basename(filename_amm))

    def validate_filename(self, s):
        """ Procedura per validare il file.
        	Verifica della nomenclatura del file: TIMESTAMP_NIDPRATICAORACLE_PIVAUDD_PIVADISTR_SAG1_ANNOTERMICO_PROGRESSIVO.CSV
        	Ritorna True se il file e' valido
        """

        file_name = s.replace("file://", "")
        file_name = os.path.basename(file_name)

        #print("Validate result: ",file_name, bool(re.match(self.pattern, file_name)))
        return bool(re.match(self.pattern, file_name))

    def getNewFiles(self,sqlCtx, listFiles):
        listResult = []
        for f in listFiles:
            query = "select * from  " + self.tableAmmissibilita + " where file_name like '%" + f +"%'"
            df = sqlCtx.sql(query)
            if df.count() <= 0:
                listResult.append(f)
        return listResult


    def is_zipfile(self, filename):
        fileexts = ['.zip'] #TODO da spostare nel file JSON
        
        file_name = filename.replace("file://", "")
        file_name = os.path.basename(file_name)
        ext = os.path.splitext(file_name)[1]

        #print("Ext: " , ext, bool(ext in fileexts))
        return bool(ext in fileexts)

    def move_file(self, filename):

        filename = filename.replace("file://", "")
        baseDir = os.path.dirname(filename) + "/"
        files_result = os.path.join( self.dirDest , filename.replace(baseDir, ""))
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

        fileSrc = fileSrc.replace("file://","")
        dirDest = self.dirDest
        # print("directory destination: ", dirDest, fileSrc)
        
        try:   
            file_obj = zipfile.ZipFile(fileSrc, "r")
            file_obj.extractall(dirDest) 
        except:
            return False
        
        return True
        
    def load_tables(self, sqlCtx):
        query_rcugas_massivo = "select t_codice_pdr, T_TIPO_OP, T_PROCESSO, from_unixtime(unix_timestamp(rcugas_massivo.d_data_inizio_for , 'yyyy-MM-dd')) as d_data_inizio_for, from_unixtime(unix_timestamp(DATA_FINE_FOR , 'yyyy-MM-dd')) as DATA_FINE_FOR, piva_udd, t_trattamento   from " + table_massivo
        query_rcugas_massivo = "select t_codice_pdr,  from_unixtime(unix_timestamp(rcugas_massivo.d_data_inizio_for , 'yyyy-MM-dd')) as d_data_inizio_for, from_unixtime(unix_timestamp(DATA_FINE_FOR , 'yyyy-MM-dd')) as DATA_FINE_FOR, piva_udd, t_trattamento   from  rcugas.rcugas_massivo"
        query_rcuazienda = "select rcu_azienda_p.t_piva from rcu.rcu_azienda_p"
        query_rcugas_connessioni_distr = "select t_codice_pdr, t_remi, n_id_distr from rcugas.rcugas_connessioni_distr"
        query_rcugas_temp_va1 = "select * from PRT_RCUGAS.RCUGAS_TEMP_VA1" 

        #DEBUG
        #query_rcugas_massivo = "select t_codice_pdr,  from_unixtime(unix_timestamp(d_data_inizio_for , 'yyyy-MM-dd')) as d_data_inizio_for, from_unixtime(unix_timestamp(DATA_FINE_FOR , 'yyyy-MM-dd')) as DATA_FINE_FOR, piva_udd, t_trattamento   from   " + table_massivo

        print("Load RCU MASSIVO")
        dataframe_rcumassivo = sqlCtx.sql(query_rcugas_massivo)

        print("Load RCU Azienda")
        dataframe_rcuazienda = sqlCtx.sql(query_rcuazienda)

        print("Load RCU Connessioni Distr")
        rcugas_connessioni_distr = sqlCtx.sql(query_rcugas_connessioni_distr)

        print("Load RCU TEMP Va1")
        rcugas_temp_va1 = sqlCtx.sql(query_rcugas_temp_va1)


        return dataframe_rcumassivo, dataframe_rcuazienda, rcugas_connessioni_distr, rcugas_temp_va1

    def save_data(self, sqlCtx, dataframe, cmdTableRefresh = "", partitionTableHive = None ):
        """ Procedura usata per savare i dati nella tabella HIVE 
            sqlCtx: Context Spark
            dataframe: dataframe 
            cmdTableRefres: command to refresh Table
            partitionTableHive: list partition field 
        """

        if dataframe is None:
            return
        if dataframe.rdd.isEmpty():
            return

        print ("Scrittura: {}".format(self.nameTableHive))
        dataframe.write.parquet(self.nameTableHive, 'append')

        sqlCtx.sql(self.cmdTableRefresh)
        self.backup_table_tmp(sqlCtx)

    def save_ammissibilita_csv(self, dataFrame):
        date = datetime.datetime.now()

        #Creazione Id
        #res = dataFrame.withColumn("NUM_RIGA", monotonicallyIncreasingId())
        res = dataFrame
        res = res.withColumn("COD_TIPO_FILE", lit('SAG'))
        #res = res.withColumn("PIVA_UTENTE", self.get_piva(col('FILE_NAME')))

        funcPiva = udf(lambda file_name : self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid : self.get_verifica_amm(valid), StringType())
        res = res.withColumn('PIVA_UTENTE',funcPiva(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM',funcVERIFICA_AMM(col('valid')))
        res = res.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        res = res.withColumn('tipo_file',lit( 'SAG' ))

        #res.show(truncate = False)
        print ("save table: ",self.TABLEHIVE_AMMISSIBILITA)
        res.write.parquet(self.TABLEHIVE_AMMISSIBILITA, 'append')
        #sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        self.repair_table(sqlCtx, self.tableAmmissibilita)
    
    def save_anomalie_csv(self, dataFrame):
        date = datetime.datetime.now()

        #dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema_ANOMALIE_FILE_DATA())
        rdd = rdd.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        rdd = rdd.withColumn('tipo_file',lit( 'SAG' ))
        #rdd.show(truncate = False)

        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')
        #dataFrame.foreach(self.save_csv_anom)

    def save_to_csv(self, sqlCtx, dataframe):
        print ("save to csv SAG")

        date = datetime.datetime.now()
        dataYYYMMDD = str(date.year) + str(date.month) + str(date.day)

        dataframe = dataframe.withColumn("NUM_RIGA", expr("CAST(NUM_RIGA AS INTEGER)")).orderBy('num_riga')
        file_dict = {}

        list_files = {}
        #dataframe.show(truncate = False)
        for it in dataframe.collect():
            path_file = it[1].replace("file://", "") # Recupero il nome del file
            fileAMM = self.save_csv_amm("1", it)
            list_files[path_file] = fileAMM

        #      0             1          2          3
        #190624155817637_138274723_08345840964_00446820672_SAG1_2020_1.CSV
        for key, value in list_files.items():
            filename_amm = value
            relative_file = os.path.basename(filename_amm)
            n_id_pratica = relative_file.split("_")[1]
            PIVAUDD = relative_file.split("_")[2] 
            PIVADISTR = relative_file.split("_")[3]
            file_name_zip = self.directory_zip + "REPORT_AMM/SAG1_AMM_" + n_id_pratica + "_" + PIVAUDD +  "_" + PIVADISTR + "_" +  dataYYYMMDD + ".zip"
            print ("zip file", file_name_zip)

            #<PIVA_UdD>_<Piva_Distr>_SAG_<AAAA>_<progressivo>.csv
            filename_amm_res = self.calcola_file_amm(path_file)
            
            # path_file.replace("file://" + self.dirDest, "").replace(".csv", "").replace(".CSV", "") +\
            #         "_AMM_" +\
            #         self.time_stamp +\
            #         ".csv"
            relative_file = os.path.basename(filename_amm_res)
            print ("file_name_zip", file_name_zip)
            print ("file Amm", filename_amm)

            with ZipFile(file_name_zip, 'a', allowZip64 = True) as myzip:
                myzip.write(filename_amm, relative_file)

    def get_verifica_amm(self, valid):
        if valid:
            return 'Y'
        else:
            return 'N'

    def get_piva(self, filename):
        file_name = os.path.basename(filename.replace("file://",""))

        items = file_name.split("_")
        pIvaUdD = items[0]

        return pIvaUdD

    def save_csv_anom(self, x):
        #NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        
        txt = ";".join([ str(x[14]), str(x[7]), str(x[8]),str(x[9]), str(x[10]),str(x[11])]) + "\n"
        path_file = x[1].replace("file://", "")
        name_file = path_file.replace("file:" + self.dirDest, "").replace(".csv", "").replace(".CSV", "") +\
                    "_ANOMALIE_" +\
                    self.time_stamp +\
                    ".csv"

        f = open(name_file,"a+")
        f.write(txt)

    def save_data_anomalie_file(self, sqlCtx, rdd):
        date = datetime.datetime.now()

        rdd = rdd.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        rdd = rdd.withColumn('tipo_file',lit( 'SAG' ))

        rdd.write.parquet(self.TABLEHIVE_ANOMALIE_F, 'append')

        self.repair_table(sqlCtx, self.tableAnomalieF)

    def get_filename_amm(self, filename_input):
        now = datetime.datetime.now()
        time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
        return self.calcola_file_amm(filename_input)

    def calcola_file_amm(self, filename):
        #ID_pratica_<PIVA_UDD>_<PIVA_Distr>_SAG1_<AAAA>_<progressivo>_AMM.csv
        #      0             1          2          3        4    5   6 
        #190624155817637_138274723_08345840964_00446820672_SAG1_2020_1.CSV
        relative_file = os.path.basename(filename)
        namefile_without_exc = relative_file.replace(".csv", "").replace(".CSV", "")
        n_id_pratica = namefile_without_exc.split("_")[1]
        PIVAUDD = namefile_without_exc.split("_")[2] 
        PIVADISTR = namefile_without_exc.split("_")[3]
        ANNO = namefile_without_exc.split("_")[5]
        PROGRESSIVO = namefile_without_exc.split("_")[6]

        print (namefile_without_exc, n_id_pratica,PIVAUDD,PIVADISTR, ANNO ,PROGRESSIVO) 
        filename_amm = n_id_pratica + "_" + PIVAUDD + "_" + PIVADISTR + "_SAG1_" + ANNO + "_" + PROGRESSIVO +\
            "_AMM.csv"

        return os.path.dirname(filename) + "/" + filename_amm

    def save_csv_amm(self, id_pratica, x):
        # 
        # 0 valid
        # 1 file_name
        # 2 file_name_rel
        # 3 cod_pdr
        # 4 cod_remi
        # 5 cons_ann
        # 6 cod_prof_prel_std
        # 7 data_ds
        # 8 data_mis1
        # 9 data_mis2
        # 10 cod_tipo_file
        # 11 piva_utente
        # 12 verifica_amm
        # 13 cod_causale
        # 14 descrizione
        # 15 tipo_file
        # 16 num_riga
        # 17 data_import
        #      
        # 0  aggregazioni_amm_gas_sag.valid 	       
        # 1  aggregazioni_amm_gas_sag.file_name 	
        # 2  aggregazioni_amm_gas_sag.file_name_rel 	
        # 3  aggregazioni_amm_gas_sag.cod_pdr 	    
        # 4  aggregazioni_amm_gas_sag.cod_remi 	
        # 5  aggregazioni_amm_gas_sag.cons_ann 	
        # 6  aggregazioni_amm_gas_sag.cod_prof_prel_std 	
        # 7  aggregazioni_amm_gas_sag.data_ds 	    
        # 8  aggregazioni_amm_gas_sag.data_mis1 	
        # 9  aggregazioni_amm_gas_sag.data_mis2 	
        # 10 aggregazioni_amm_gas_sag.cod_tipo_file
        # 11 aggregazioni_amm_gas_sag.piva_utente 	
        # 12 aggregazioni_amm_gas_sag.verifica_amm 
        # 13 aggregazioni_amm_gas_sag.cod_causale 	
        # 14 aggregazioni_amm_gas_sag.descrizione 	
        # 15 aggregazioni_amm_gas_sag.tipo_file 	
        # 16 aggregazioni_amm_gas_sag.num_riga 	
        # 17 aggregazioni_amm_gas_sag.data_import
        # 
        #                                                                                                                                                                 |tipo_file|num_riga|data_import|
        #NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        #print x
            
        #TIMESTAMP_NIDPRATICAORACLE_PIVAUDD_PIVADISTR_SAG1_ANNOTERMICO_PROGRESSIVO.CSV
        #190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        path_file = x[1]
        
        filename = os.path.basename(path_file)
        #n_id_pratica = filename.split("_")[1] # Recupero l'id della pratica
        PIVAUDD = filename.split("_")[0] 
        PIVADISTR = filename.split("_")[1]
        #print ("------>",filename, (id_pratica + "_" + PIVAUDD +  "_" + PIVADISTR))

        path_file = (path_file.replace("file://", "")) # Recupero il nome del file
        #print ("file save", path_file)

        name_file = path_file.replace("file://" + self.dirDest, "").replace(".csv", "").replace(".CSV", "") +\
                    "_AMM_" +\
                    self.time_stamp +\
                    ".csv"

        num_riga = str(x[16])
        COD_TIPO_FILE ="" if not x[10] else str(x[10].encode('utf-8'))
        piva ="" if not x[11] else str(x[11].encode('utf-8'))
        VERIFICA_AMM = "" if not x[12] else str(x[12].encode('utf-8'))
        COD_CAUSALE = "" if not x[13] else str(x[13].encode('utf-8'))
        DESCRIZIONE = "" if not x[14] else str(x[14].encode('utf-8'))

        txt = ";".join([ num_riga, COD_TIPO_FILE, piva, VERIFICA_AMM, COD_CAUSALE, DESCRIZIONE]) + "\n"
        self.write(name_file, txt)
        #return (id_pratica + "_" + PIVAUDD +  "_" + PIVADISTR, name_file)
        return name_file 

    def write(self, name_file, txt):
        f = open(name_file,"a+")
        f.write(txt)
        f.close()
        
    def getSchema(self):
        return self.getSchema_AMMISSIBILITA_FILE()

    def get_duplicate(self, sqlCtx, dataframe):
        return dataframe, None

    def repair_table(self, sqlCtx, name_table):
        query = "MSCK REPAIR TABLE " + name_table 
        sqlCtx.sql(query)

    def get_rcu_1(self,sqlCtx, dataframe, dataframe_rcumassivo):
        print("RCU inesistente")
        if dataframe is None:
            return None, None
        
        if dataframe.rdd.isEmpty():
            return dataframe, None

        #time1 = time.time()
        dataframe = dataframe.alias('tab')
        dataframe_rcumassivo = dataframe_rcumassivo.alias('dataframe_rcumassivo')

        dataframeOK = dataframe.join(dataframe_rcumassivo, dataframe.COD_PDR == dataframe_rcumassivo.t_codice_pdr, how='left')\
                               .filter(col('t_codice_pdr').isNotNull())\
                               .select('tab.*')\
                               .distinct()

        rddKO = dataframe.join(dataframe_rcumassivo, dataframe.COD_PDR == dataframe_rcumassivo.t_codice_pdr, how='left')\
                               .filter(col('t_codice_pdr').isNull())\
                               .select('tab.*')\
                               .distinct()\
                               .map(lambda f: ("226", "Il PdR e' inesistente",  f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
            rddKO,\
            self.getSchema_ANOMALIE_FILE())

        return dataframeOK, dataframeKO
        
    def get_rcu_2(self,sqlCtx, dataframe, dataframe_rcumassivo):
        print("RCU attivo")
        if dataframe is None:
            return None, None

        dataframe = dataframe.alias('tab')
        dataframe_rcumassivo = dataframe_rcumassivo.alias('dataframe_rcumassivo')

        dataframeOK = dataframe.join(dataframe_rcumassivo, dataframe.COD_PDR == dataframe_rcumassivo.t_codice_pdr, how='left')\
                               .filter(col('t_codice_pdr').isNotNull() & col('DATA_FINE_FOR').isNull())\
                               .select('tab.*')\
                               .distinct()

        dataframe_rcumassivo2 = dataframe_rcumassivo.where(col('DATA_FINE_FOR').isNull()).alias('dataframe_rcumassivo')
        rddKO = dataframe.join(dataframe_rcumassivo2, dataframe.COD_PDR == dataframe_rcumassivo.t_codice_pdr, how='left')\
                         .filter(col('t_codice_pdr').isNull())\
                         .select('tab.*')\
                         .distinct()\
                         .map(lambda f: ( "226", "Il PdR non e' attivo", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())

        return dataframeOK, dataframeKO

    def check11(self, sqlCtx, dataframe, dataframe_rcumassivo):
        """
            Check 11 - Non richiesto
            Campo DATA_MIS1/DATA_MIS2 obbligatorio se compilato TRATTAMENTO in RCU Y o M
            004 I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS1 E DATA_MIS2 obbligatori se presente TRATTAMENTO_RCU Y o M)
        """
       
        sqlCtx.registerDataFrameAsTable(dataframe, "tab1")
        query1OK = " SELECT tab1.* from tab1                                                                                                                                "\
                   "    LEFT JOIN (select t_codice_pdr, t_trattamento, data_fine_for from rcugas.rcugas_massivo) AS rcugas_massivo ON rcugas_massivo.t_codice_pdr = cod_pdr "\
                   " WHERE rcugas_massivo.t_codice_pdr IS NOT NULL                                                                                                          "\
                   "    AND (rcugas_massivo.t_trattamento = 'Y' OR rcugas_massivo.t_trattamento = 'M')                                                                      "\
                   "    AND from_unixtime(unix_timestamp(rcugas_massivo.data_fine_for , 'yyyy-MM-dd')) is  null                                                             "\
                   "    AND from_unixtime(unix_timestamp(DATA_MIS1 , 'yyyy-MM-dd')) is not null                                                                             "

        query1KO = " SELECT tab1.* from tab1                                                                                                                                "\
                   "    LEFT JOIN (select t_codice_pdr, t_trattamento from rcugas.rcugas_massivo) as rcugas_massivo ON rcugas_massivo.t_codice_pdr = cod_pdr                "\
                   " WHERE rcugas_massivo.t_codice_pdr IS NULL                                                                                                              "\
                   "    AND (rcugas_massivo.t_trattamento <> 'Y' AND rcugas_massivo.t_trattamento <> 'M')                                                                   "

        #print query1OK
        dataframeOK = sqlCtx.sql(query1OK)   
        rddKO = sqlCtx.sql(query1KO)\
                .map(lambda f: ( "226", "I campi obbligatori non sono stati cmpilati o non sono stati correttamente compilati (DATA_MIS1 e DATA_MIS2 obbligatori se presente TRATTAMENTO_RCU Y o M", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())
            
        return dataframeKO, dataframeOK

    def check12(self, sqlCtx, dataframe):
        """
            Check 12 - Non richiesto
            Se TRATTAMENTO in RCU NULL
            015	La richiesta non e' eseguibile (Assenza Trattamento in RCU - inserire flusso AD1)
        """
        sqlCtx.registerDataFrameAsTable(dataframe, "tab1")
        query1OK = " SELECT tab1.* from tab1                                                                                                                "\
                   "  LEFT JOIN (select t_codice_pdr, t_trattamento, data_fine_for from rcugas.rcugas_massivo) AS rcugas_massivo ON rcugas_massivo.t_codice_pdr = cod_pdr "\
                   " WHERE rcugas_massivo.t_trattamento <> ''"

        query1KO = " SELECT tab1.* from tab1 "\
                   " LEFT JOIN (select t_codice_pdr, t_trattamento from rcugas.rcugas_massivo) as rcugas_massivo ON rcugas_massivo.t_codice_pdr = cod_pdr "\
                   " WHERE rcugas_massivo.t_trattamento = ''"

        #dataframeOK = sqlCtx.sql(query1OK)   
        rddKO = sqlCtx.sql(query1KO)\
                 .map(lambda f: ( "015", "La richiesta non e' eseguibile (Assenza Trattamento in RCU - inserire flusso AD1)", f[1], f[self.NUM_RIGA_CONST]))

        dataframeOK = sqlCtx.sql(query1KO)
        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())
            
        return (dataframeKO, dataframeOK)

    def check13(self, sqlCtx, dataframe):
        """
            Check 13 - Non richiesto
            Campo DATA_MIS1/DATA_MIS2 non presenti se il campo  TRATTAMENTO in RCU G	
            004	I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS1 E DATA_MIS2 non devono essere compilati se presente TRATTAMENTO RCU G)
        """


        sqlCtx.registerDataFrameAsTable(dataframe, "tab1")
        query1OK = " SELECT tab1.* from tab1 "\
                   " LEFT JOIN (select t_codice_pdr, t_trattamento, data_fine_for from rcugas.rcugas_massivo) AS rcugas_massivo ON rcugas_massivo.t_codice_pdr = cod_pdr "\
                   " WHERE rcugas_massivo.t_trattamento <> 'G' and (DATA_MIS1 = NULL and DATA_MIS2 <> NULL)"

        query1KO = " SELECT tab1.* from tab1 "\
                   " LEFT JOIN (select t_codice_pdr, t_trattamento from rcugas.rcugas_massivo) as rcugas_massivo ON rcugas_massivo.t_codice_pdr = cod_pdr "\
                   " WHERE rcugas_massivo.t_trattamento <> 'G' and (DATA_MIS1 = NULL and DATA_MIS2 <> NULL)"

        #dataframeOK = sqlCtx.sql(query1OK)   
        rddKO = sqlCtx.sql(query1KO)\
                .map(lambda f: ( "004", "I campi obbligatori non sono stati compilati o non son ostati correttamente compilati (DATA_MIS1 e DATA_MIS2 non devono essere compilati se presente TRATTAMENTO RCU G)", f[1], f[self.NUM_RIGA_CONST]))

        dataframeOK = sqlCtx.sql(query1KO)
        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())
            
        return (dataframeKO, dataframeOK)

    def check11_nuovo(self, sqlCtx, dataframe):
        
        if (dataframe == None):
            return (None, None)

        dataframeOK = dataframe.filter((col("data_mis1").isNotNull()) & (col("data_mis2").isNotNull()))
        dataframe_null = dataframe.filter((col("data_mis1").isNull()) & (col("data_mis2").isNull()))

        rddKO = dataframe_null.map(lambda f: ( "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS1 E DATA_MIS2 devono essere compilati o entrambi non valorizzati)", f[1], f[self.NUM_RIGA_CONST]))
        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())

        return (dataframeKO, dataframeOK)
    
    # OK
    def check14(self, sqlCtx, dataframe, rcugas_massivo_dataframe):
        """
            Check 14
            14 DATA_MIS1, DATA_MIS2 non deve essere superiore alla data trasmissione del file

                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        """
        
        if (dataframe == None):
            return (None, None)

        sqlCtx.registerDataFrameAsTable(dataframe, "tab")
        dataframe = dataframe.alias('tab')
       
        dataframe_notnull = sqlCtx.sql("SELECT * from tab "\
                                    +" WHERE DATA_MIS1 is not null and DATA_MIS2 is not null ")

        dataframe_null = sqlCtx.sql("SELECT * from tab "\
                                    +" WHERE DATA_MIS1 is  null and DATA_MIS2 is  null ")

       

        dataframeOK = dataframe_notnull.withColumn("anno", f.split(col("FILE_NAME_REL"), "_")[5]).alias('tab')\
                               .filter((year("data_mis1") <= col("anno")) & (year("data_mis2") <= col("anno")))\
                               .drop('anno')
        dataframeOK = dataframeOK.unionAll(dataframe_null)

        rddKO = dataframe_notnull.withColumn("anno", f.split(col("FILE_NAME_REL"), "_")[5]).alias('tab')\
                         .filter((year("data_mis1") > col("anno")) | (year("data_mis2") > col("anno")))\
                         .select('tab.*')\
                         .drop('anno')
                         
        #rddKO.show(truncate = False)
        rddKO = rddKO.map(lambda f: ( "004", "I campi obbligatori non sono stati compilati o non son stati correttmente compilati (DATA_MIS1/DATA_MIS2/DATA_DS non devono essere superiori all data trasmissione", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())

       
        return (dataframeKO, dataframeOK)
       
    # OK
    def check15(self, sqlCtx, dataframe):
        """
            Check 15
            004	I campi obbligatori non sono stati compilati o non sono stati correttamente compilati 
                (DATA_MIS2-DATA_MIS1 non risulta maggiore uguale a 270 giorni)
        
                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        """
        
        if (dataframe == None):
            return (None, None)


        sqlCtx.registerDataFrameAsTable(dataframe, "tab")
        dataframe = dataframe.alias('tab')
       
        dataframe_notnull = sqlCtx.sql("SELECT * from tab "\
                                    +" WHERE DATA_MIS1 is not null and DATA_MIS2 is not null ")

        dataframe_null = sqlCtx.sql("SELECT * from tab "\
                                    +" WHERE DATA_MIS1 is  null and DATA_MIS2 is  null ")


        dataframeOK = dataframe_notnull.filter( datediff( col('data_mis2') , col('data_mis1')) >= 270)
        dataframeOK = dataframeOK.unionAll(dataframe_null)

        rddKO = dataframe_notnull.filter( datediff( col('data_mis2') , col('data_mis1')) < 270)\
                         .map(lambda f: ( "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS2-DATA_MIS1 non risulta maggiore uguale a 300 giorni)", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())

        return (dataframeKO, dataframeOK)

    # OK
    def check16(self, sqlCtx, dataframe):
        """
            Check 16 
            004	Codice PDR presente piu di una volta nello stesso file ma con valori diversi
                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        """

        if (dataframe == None):
            return (None, None)
        
        sqlCtx.registerDataFrameAsTable(dataframe, "tab")
        dataframe = dataframe.alias('tab')
        #datframe_group = dataframe.groupBy(dataframe.COD_PDR, dataframe.FILE_NAME, dataframe.COD_REMI, dataframe.CONS_ANN, dataframe.DATA_DS, dataframe.DATA_MIS1, dataframe.DATA_MIS2)\
        #                           .agg({'*': 'count'})\
        #                           .filter(col('count(1)') == 1)\
        #                           .alias('table3')

        dataframe_group = sqlCtx.sql("SELECT COD_PDR, FILE_NAME, COD_REMI, CONS_ANN, DATA_DS, DATA_MIS1, DATA_MIS2, count(*) as count from tab "\
                                    +" group by COD_PDR, FILE_NAME, COD_REMI, CONS_ANN, DATA_DS, DATA_MIS1, DATA_MIS2")

        datframe_group = dataframe_group.filter(col('count') == 1)\
                                        .alias('table3')

        dataframeOK = dataframe.join(datframe_group, dataframe.COD_PDR == datframe_group.COD_PDR )\
                               .select('tab.*')\
                               .distinct()
        

        #datframe_group_ko = dataframe.groupBy(dataframe.COD_PDR, dataframe.FILE_NAME, dataframe.COD_REMI, dataframe.CONS_ANN, dataframe.DATA_DS, dataframe.DATA_MIS1, dataframe.DATA_MIS2)\
        #                             .agg({'*': 'count'})\
        #                             .filter(col('count(1)') > 1)\
        #                             .alias('table3')

        datframe_group_ko = dataframe_group.filter(col('count') > 1)\
                                        .alias('table3')
        rddKO = dataframe.join(datframe_group_ko, dataframe.COD_PDR == datframe_group_ko.COD_PDR)\
                         .select('tab.*')\
                         .distinct()\
                         .map(lambda f: ( "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (" + f[3] + " presente piu' volte nel file " + f[2] + ")", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                            rddKO,\
                            self.getSchema_ANOMALIE_FILE())

       
        return (dataframeKO, dataframeOK)

    # OK
    def check17_v2(self, sqlCtx, dataframe):
        """
            Check 17
            Codice PDR presente piu di una volta nella pratica in file diversi ma con valori diversi

                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        """

        if (dataframe == None):
            return (None, None)
        
        sqlCtx.registerDataFrameAsTable(dataframe, "tab")
        dataframe = dataframe.alias('tab')
        #datframe_group = dataframe.groupBy(dataframe.COD_PDR, dataframe.FILE_NAME, dataframe.COD_REMI, dataframe.CONS_ANN, dataframe.DATA_DS, dataframe.DATA_MIS1, dataframe.DATA_MIS2)\
        #                           .agg({'*': 'count'})\
        #                           .filter(col('count(1)') == 1)\
        #                           .alias('table3')

        dataframe_group = sqlCtx.sql("SELECT COD_PDR, COD_REMI, CONS_ANN, DATA_DS, DATA_MIS1, DATA_MIS2, count(*) as count from tab "\
                                    +" group by COD_PDR, COD_REMI, CONS_ANN, DATA_DS, DATA_MIS1, DATA_MIS2")

        datframe_group = dataframe_group.filter(col('count') == 1)\
                                        .alias('table3')

        dataframeOK = dataframe.join(datframe_group, dataframe.COD_PDR == datframe_group.COD_PDR )\
                               .select('tab.*')\
                               .distinct()
        

        #datframe_group_ko = dataframe.groupBy(dataframe.COD_PDR, dataframe.FILE_NAME, dataframe.COD_REMI, dataframe.CONS_ANN, dataframe.DATA_DS, dataframe.DATA_MIS1, dataframe.DATA_MIS2)\
        #                             .agg({'*': 'count'})\
        #                             .filter(col('count(1)') > 1)\
        #                             .alias('table3')

        datframe_group_ko = dataframe_group.filter(col('count') > 1)\
                                        .alias('table3')
        rddKO = dataframe.join(datframe_group_ko, dataframe.COD_PDR == datframe_group_ko.COD_PDR)\
                         .select('tab.*')\
                         .distinct()\
                         .map(lambda f: ( "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (" + f[3] + " presente piu' volte nel file " + f[2] + ")", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                            rddKO,\
                            self.getSchema_ANOMALIE_FILE())

       
        return (dataframeKO, dataframeOK)


    def check17(self, sqlCtx, dataframe):
        """
            Check 17
            Codice PDR presente piu di una volta nella pratica in file diversi ma con valori diversi

                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        """

        if (dataframe == None):
            return (None, None)

        dataframe_sag_importati = sqlCtx.sql("SELECT * FROM " + self.database + ".gas_sag").alias('tab_sag')
        dataframe = dataframe.alias('tab')


        dataframeOK = dataframe.join(dataframe_sag_importati, \
            (dataframe.COD_PDR == dataframe_sag_importati.cod_pdr) &
            (dataframe.COD_REMI == dataframe_sag_importati.cod_remi) &
            (dataframe.CONS_ANN == dataframe_sag_importati.cons_ann) &
            (dataframe.DATA_MIS1 == dataframe_sag_importati.data_mis1) &
            (dataframe.DATA_MIS2 == dataframe_sag_importati.data_mis2), how='left' 
        )\
            .filter( (dataframe_sag_importati.cod_pdr.isNull()) \
                & (dataframe_sag_importati.cod_remi.isNull())\
                & (dataframe_sag_importati.cons_ann.isNull())\
                & (dataframe_sag_importati.data_mis1.isNull())\
                & (dataframe_sag_importati.data_mis2.isNull())\
            )\
            .select('tab.*')

        rddKO = dataframe.join(dataframe_sag_importati, \
            (dataframe.COD_PDR == dataframe_sag_importati.cod_pdr) &
            (dataframe.COD_REMI == dataframe_sag_importati.cod_remi) &
            (dataframe.CONS_ANN == dataframe_sag_importati.cons_ann) &
            (dataframe.DATA_MIS1 == dataframe_sag_importati.data_mis1) &
            (dataframe.DATA_MIS2 == dataframe_sag_importati.data_mis2), how='left' 
        )\
            .filter( (dataframe_sag_importati.cod_pdr.isNotNull()) \
                & (dataframe_sag_importati.cod_remi.isNotNull())\
                & (dataframe_sag_importati.cons_ann.isNotNull())\
                & (dataframe_sag_importati.data_mis1.isNotNull())\
                & (dataframe_sag_importati.data_mis2.isNotNull())\
            )\
            .select('tab.*')\
            .distinct()\
            .map(lambda f: ( "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (" + f[3] + " presente piu' volte nel file " + f[2] + ")", f[1], f[self.NUM_RIGA_CONST]))


        dataframeKO = sqlCtx.createDataFrame(\
                            rddKO,\
                            self.getSchema_ANOMALIE_FILE())
      

        return (dataframeKO, dataframeOK)
    


    # OK
    def check22(self, sqlCtx, dataframe, rcugas_connessioni_distr):
        """
            Check 22
            Verifica relazione PDR - Remi alla data della richiesta

                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        """
        if (dataframe == None):
            return (None, None)

        dataframe = dataframe.alias('tab')
        rcugas_connessioni_distr = rcugas_connessioni_distr.alias('rcugas_connessioni_distr')
        dataframeOK = dataframe.join(rcugas_connessioni_distr, dataframe.COD_REMI == rcugas_connessioni_distr.t_remi, how='left')\
                               .filter(col('t_remi').isNotNull())\
                               .select('tab.*')\
                               .distinct()

        rddKO = dataframe.join(rcugas_connessioni_distr, dataframe.COD_REMI == rcugas_connessioni_distr.t_remi, how='left')\
                               .filter(col('t_remi').isNull())\
                               .select('tab.*')\
                               .distinct()\
                               .map(lambda f: ( "214", "Il codice Remi e il PdR non sono coerenti", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())

        return (dataframeKO, dataframeOK)
        
    # OK
    def check23(self, sqlCtx, dataframe, rcugas_massivo_dataframe, rcugas_temp_va1):
        """
            Check 23
            PdR rispetto al quale risulta pervenuto nell'anno 2018 - 2019 una VTG o Attivazione 
            
                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
        """
        if (dataframe == None):
            return (None, None)

        sqlCtx.registerDataFrameAsTable(dataframe, "tab")
        dataframe = dataframe.alias('tab')
        #DEBUG 
        #rcugas_massivo_dataframe = sqlCtx.sql("select * from au_test.test_massivo")

        dataframe_notnull = sqlCtx.sql("SELECT * from tab "\
                                    +" WHERE DATA_MIS1 is not null and DATA_MIS2 is not null ")

        dataframe_null = sqlCtx.sql("SELECT * from tab "\
                                    +" WHERE DATA_MIS1 is  null and DATA_MIS2 is  null ")
        
        rcugas_massivo_dataframe = rcugas_massivo_dataframe.alias('rcugas_massivo_dataframe')




        mas_dataframe2 = sqlCtx.sql(
            " select t_codice_pdr, max(d_data_inizio_for) as d_data_inizio_for from " + table_massivo + \
            " where (T_PROCESSO  = 'VTG' or T_PROCESSO  = 'VSG') and T_TIPO_OP  = 'InserisciFORNITURA' "\
            " group by t_codice_pdr").alias("mas_dataframe2")

        #dataframe_no_vtg_vsg = self.sqlContext.sql(
        #    " select t_codice_pdr, max(d_data_inizio_for) as d_data_inizio_for from test_massivo        "\
        #    " where (t_tipo_op <> 'VTG' and t_tipo_op <> 'VSG') and T_PROCESSO  <> 'InserisciFORNITURA' "\
        #    " group by t_codice_pdr")



        # prendo i codice pdr e data inizio for maggiore della data dell'elemento contrassegnato VTG*
        dataframe_vtg = rcugas_massivo_dataframe.join(mas_dataframe2, mas_dataframe2.t_codice_pdr == rcugas_massivo_dataframe.t_codice_pdr)\
                                                .filter(mas_dataframe2.d_data_inizio_for < rcugas_massivo_dataframe.d_data_inizio_for)\
                                                .select(rcugas_massivo_dataframe.t_codice_pdr, rcugas_massivo_dataframe.d_data_inizio_for)
        #print("dataFrame vtg")
        #dataframe_vtg.show(truncate = False)
        dataframe_vtg = dataframe_vtg.alias('vtg')
        dataframe_not_vtg = rcugas_massivo_dataframe.join(dataframe_vtg, rcugas_massivo_dataframe.t_codice_pdr == dataframe_vtg.t_codice_pdr, how='left')\
                                                    .filter(col('vtg.t_codice_pdr').isNull())\
                                                    .select(col('rcugas_massivo_dataframe.t_codice_pdr'), col('rcugas_massivo_dataframe.d_data_inizio_for'))

        #print("dataframe_not_vtg")
        #dataframe_not_vtg.show(truncate = False)
        #dataframe.show(truncate = False)

        dataframe_not_vtg = dataframe_not_vtg.alias('dataframe_not_vtg')

        #
        dataframeOK1 = dataframe_notnull.join(dataframe_vtg, dataframe_notnull.COD_PDR == dataframe_vtg.t_codice_pdr, how='left')\
                                .filter(col('vtg.t_codice_pdr').isNotNull() & (col('tab.DATA_MIS1') >= col('vtg.d_data_inizio_for')))\
                                .select('tab.*')
                                #.filter(dataframe_vtg.t_codice_pdr.isNotNull() & (dataframe.DATA_MIS1 >= dataframe_vtg.d_data_inizio_for))\
                                #.select('tab.*')

        #print("dataframeOK1")
        #dataframeOK1.show(truncate = False)


        dataframeOK2 = dataframe_notnull.join(dataframe_not_vtg, dataframe_notnull.COD_PDR == dataframe_not_vtg.t_codice_pdr, how='left')\
                                .filter(col('dataframe_not_vtg.t_codice_pdr').isNotNull() & (col('tab.DATA_MIS1') >= col('dataframe_not_vtg.d_data_inizio_for')))\
                                .select('tab.*')
                                #.filter(dataframe_not_vtg.t_codice_pdr.isNotNull() & (dataframe.DATA_MIS1 >= dataframe_not_vtg.d_data_inizio_for))\
                                #.select('tab.*')
        #print("dataframeOK2")
        #dataframeOK2.show(truncate = False)

        dataframeOK3 =  dataframeOK1.unionAll(dataframeOK2)    
        #print("dataframeOK3")
        #dataframeOK3.show(truncate = False)

        dataframeOK3 = dataframeOK3.alias('dataframeOK3')
        dataframeOK4 = dataframe_notnull.join(dataframeOK3, col("tab.COD_PDR") == col("dataframeOK3.COD_PDR"))\
                                .select('tab.*')
        #print("dataframeOK")
        #dataframeOK4.show(truncate = False)

        dataframeOK = dataframeOK4 #.filter(dataframeOK4.COD_PDR.isNotNull())

        dataframeOK.alias('dataframeOK')


        df2 = dataframeOK4.select(*(col(x).alias(x + '_df2') for x in dataframeOK4.columns))
        dataframeKO1 = dataframe_notnull.join(df2, df2.COD_PDR_df2 == dataframe_notnull.COD_PDR, how='left')\
                                .filter(df2.COD_PDR_df2.isNull())\
                                .distinct()\
                                .select('tab.*')


        #23bis INZIO
       
        mas_dataframe2 = sqlCtx.sql(
            " select t_codice_pdr from " + table_massivo + \
            " where ( T_PROCESSO  = 'VA') and T_TIPO_OP  = 'InserisciFORNITURA'").alias("mas_dataframe2")

        dataframeOK.alias('tab')
        dataframe_va = mas_dataframe2.join(dataframeOK, dataframeOK.COD_PDR == mas_dataframe2.t_codice_pdr)\
                                     .select('tab.*')

        dataframe_va.alias('tab')
        #dataframeOK = dataframe_va.join(rcugas_temp_va1, dataframe_va.COD_PDR==rcugas_temp_va1.cod_pdr)\
        #               .filter((rcugas_temp_va1.cod_prestazione != "A01") &
        #                       (rcugas_temp_va1.cod_prestazione != "A40") &
        #                       (rcugas_temp_va1.cod_prestazione != "VL1") 
        #                      )\
        #               .select('tab.*')

        sqlCtx.registerDataFrameAsTable(dataframeOK, "tab")
        sqlCtx.registerDataFrameAsTable(dataframe_va, "dataframe_va")

        dataframeOK = sqlCtx.sql("select distinct tab.* from tab   "\
                           " left join (            "\
                           "     select dataframe_va1.* from            "\
                           "         (select dataframe_va.*, split(file_name_rel,'_')[1] as n_id_pratica   from dataframe_va ) as    dataframe_va1          "\
                           "     join PRT_RCUGAS.RCUGAS_TEMP_VA1 on RCUGAS_TEMP_VA1.n_id_pratica = dataframe_va1.n_id_pratica           "\
                           "     where  rcugas_temp_va1.cod_prestazione = 'A01'             "\
                           "         and rcugas_temp_va1.cod_prestazione = 'A40'            "\
                           "         and rcugas_temp_va1.cod_prestazione = 'VL1'            "\
                           " ) as tmp on tmp.cod_pdr = tab.cod_pdr          "\
                           " where tmp.cod_pdr is null")

        rddKO_va = sqlCtx.sql("select distinct tab.* from tab   "\
                           " left join (            "\
                           "     select dataframe_va1.* from            "\
                           "         (select dataframe_va.*, split(file_name_rel,'_')[1] as n_id_pratica   from dataframe_va ) as    dataframe_va1          "\
                           "     join PRT_RCUGAS.RCUGAS_TEMP_VA1 on RCUGAS_TEMP_VA1.n_id_pratica = dataframe_va1.n_id_pratica           "\
                           "     where  rcugas_temp_va1.cod_prestazione = 'A01'             "\
                           "         and rcugas_temp_va1.cod_prestazione = 'A40'            "\
                           "         and rcugas_temp_va1.cod_prestazione = 'VL1'            "\
                           " ) as tmp on tmp.cod_pdr = tab.cod_pdr          "\
                           " where tmp.cod_pdr is null")\
                           .map(lambda f: ( "004", "PDR rispetto al quale risulta pervenuto nell'anno 2018 - 2019 una VTG o Attivazione", f[1], f[self.NUM_RIGA_CONST]))
        
        dataframeKO_va = sqlCtx.createDataFrame(\
                    rddKO_va,\
                    self.getSchema_ANOMALIE_FILE())

        #23bis FINE
        rddKO = dataframeKO1\
                         .map(lambda f: ( "004", "PDR rispetto al quale risulta pervenuto nell'anno 2018 - 2019 una VTG o Attivazione", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())

        dataframeOK = dataframeOK.unionAll(dataframe_null)

        return (dataframeKO, dataframeOK) 

    # OK
    def check24(self, sqlCtx, dataframe, rcugas_massivo_dataframe):
        """
            Check 24
            Pdr - UDD presenza di una relazione valida alla data trasmissione 
            
                   0            1          2          3        4    5    6
            190620160555615_137689742_12883420155_01812230223_SAG1_2020_001.csv
            TIMESTAMP_NIDPRATICAORACLE_PIVAUDD_PIVADISTR_SAG1_ANNOTERMICO_PROGRESSIVO.CSV
        """

        if (dataframe == None):
            return (None, None)
            
        dataframe = dataframe.alias('tab')
        


        rcugas_massivo_dataframe = rcugas_massivo_dataframe.alias('rcugas_massivo_dataframe')

        dataframe2 = dataframe.withColumn("piva_udd", f.split(col("FILE_NAME_REL"), "_")[2]).alias('tab')
        dataframeOK = dataframe2.join(rcugas_massivo_dataframe,  (dataframe.COD_PDR ==rcugas_massivo_dataframe.t_codice_pdr) & (rcugas_massivo_dataframe.piva_udd == dataframe2.piva_udd) & (rcugas_massivo_dataframe.DATA_FINE_FOR.isNull()), how='left')\
                                .filter(rcugas_massivo_dataframe.t_codice_pdr.isNotNull() & rcugas_massivo_dataframe.piva_udd.isNotNull())\
                                .select('tab.*')\
                                .distinct()\
                                .drop("piva_udd")

        dataframeKO = dataframe2.join(rcugas_massivo_dataframe,  (dataframe.COD_PDR ==rcugas_massivo_dataframe.t_codice_pdr) & (rcugas_massivo_dataframe.piva_udd == dataframe2.piva_udd) & (rcugas_massivo_dataframe.DATA_FINE_FOR.isNull()), how='left')\
                                .filter(rcugas_massivo_dataframe.t_codice_pdr.isNull() | rcugas_massivo_dataframe.piva_udd.isNull())\
                                .select('tab.*')\
                                .distinct()\
                                .drop("piva_udd")
                                
                                
        rddKO = dataframeKO.map(lambda f: ( "213", "L'UdD non e' titolare del PdR", f[1], f[self.NUM_RIGA_CONST]))

        dataframeKO = sqlCtx.createDataFrame(\
                    rddKO,\
                    self.getSchema_ANOMALIE_FILE())
      
        return (dataframeKO, dataframeOK)



    def extra(self,sqlCtx, dataframe, rcugas_massivo_dataframe, rcugas_connessioni_distr, rcugas_tmp_va1):
        print ("extra")
        if (dataframe == None):
            return (None, None)

        if (dataframe.rdd.isEmpty()):
            return (dataframe,None)

        dataframeOK = dataframe

        print 'task 14'
        dataframeKO, dataframeOK = self.check14(sqlCtx, dataframeOK, rcugas_massivo_dataframe)

        print 'task 15'
        dataframeKO, dataframeOK = self.check15(sqlCtx, dataframeOK)

        print 'task 16'
        dataframeKO, dataframeOK = self.check16(sqlCtx, dataframeOK)

        print 'task 17'
        dataframeKO, dataframeOK = self.check17(sqlCtx, dataframeOK)
        
        print 'task 22'
        dataframeKO, dataframeOK = self.check22(sqlCtx, dataframeOK, rcugas_connessioni_distr)
      
        print 'task 23'
        dataframeKO, dataframeOK = self.check23(sqlCtx, dataframeOK, rcugas_massivo_dataframe, rcugas_tmp_va1)

        print 'task 24'
        dataframeKO, dataframeOK = self.check24(sqlCtx, dataframeOK, rcugas_massivo_dataframe)
       
        #self.save_data_anomalie(sqlCtx, dataframeKO)
        return (dataframeOK, dataframeKO)

    def get_rcu_rcu_udd(self, sqlCtx, dataframe, dataframe_rcumassivo):
        print "Check rcu UdD"
        if (dataframe == None): 
            return (None, None)
        
        if (dataframe.rdd.isEmpty()):
            return (dataframe, None)

        dataframe2 = dataframe.withColumn("piva", f.split(col("FILE_NAME_REL"), "_")[2]).alias('tab')
        dataframe_rcumassivo = dataframe_rcumassivo.alias('dataframe_rcumassivo')

        dataframe_pdr = dataframe2.join(dataframe_rcumassivo, dataframe2.COD_PDR == dataframe_rcumassivo.t_codice_pdr, how='left')\
                                  .filter(dataframe_rcumassivo.DATA_FINE_FOR.isNull() & (dataframe2.piva == dataframe_rcumassivo.piva_udd))\
                                  .select(dataframe2.COD_PDR.alias("codice_pdr"))
                                  

        dataframeOK = dataframe2.join(dataframe_pdr, dataframe2.COD_PDR == dataframe_pdr.codice_pdr, how='left')\
                                .filter(dataframe_pdr.codice_pdr.isNotNull())\
                                .distinct()

        rddKO = dataframe2.join(dataframe_pdr, dataframe2.COD_PDR == dataframe_pdr.codice_pdr, how='left')\
                          .filter(dataframe_pdr.codice_pdr.isNull())\
                          .select('tab.*')\
                          .distinct()\
                          .map(lambda f: ("213", "L'UdD non e' titolare del PdR", f[1], f[self.NUM_RIGA_CONST]))

        
        dataframe_KO = sqlCtx.createDataFrame(\
            rddKO,\
            self.getSchema_ANOMALIE_FILE()) 

        return dataframeOK, dataframe_KO    

    def check_rcu_filename(self, sqlCtx, dataframe, dataframe_rcuazienda):
        return (dataframe, None)
        pass

    def save_data_amm_tmp(self, sqlCtx, dataframe, parquet_amm_tmp, mode):
        if (dataframe):
            print parquet_amm_tmp
            dataframe.write.parquet(parquet_amm_tmp, mode)


    def save_data_anomalie(self, sqlCtx, rdd):
        if (rdd == None): 
            return

        date = datetime.datetime.now()

        #dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema_ANOMALIE_FILE_DATA())
        rdd = rdd.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        rdd = rdd.withColumn('tipo_file',lit( 'SAG' ))
        #rdd.show(truncate = False)

        print self.TABLEHIVE_ANOMALIE
        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')

        self.repair_table(sqlCtx, self.tableAnomalie)
        #sqlCtx.sql("MSCK REPAIR TABLE " + self.tableAnomalie + "  SYNC PARTITIONS")
        #dataFrame.foreach(self.save_csv_anom)

    def save_data_ammissiblita(self, sqlCtx, dataFrame):
        print ("save_data_ammissiblita")
        if (dataFrame == None):
            return (None, None)

        date = datetime.datetime.now()
        
        #Creazione Id
        #res = dataFrame.withColumn("NUM_RIGA", monotonicallyIncreasingId())
        res = dataFrame
        res = res.withColumn("COD_TIPO_FILE", lit('SAG'))
        #res = res.withColumn("PIVA_UTENTE", self.get_piva(col('FILE_NAME')))

        funcPiva = udf(lambda file_name : self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid : self.get_verifica_amm(valid), StringType())
        res = res.withColumn('PIVA_UTENTE',funcPiva(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM',funcVERIFICA_AMM(col('valid')))
        res = res.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        res = res.withColumn('tipo_file',lit( 'SAG' ))

        #res.show()
        #print ("save table: ",self.TABLEHIVE_AMMISSIBILITA)
        res.write.parquet(self.TABLEHIVE_AMMISSIBILITA, 'append')
        self.repair_table(sqlCtx, self.tableAmmissibilita)
        print("Salva Dataframe OK")

    def generate_dataframe_to_csv(self, sqlCtx):
        print "Union dataframe OK - KO"
        date = datetime.datetime.now()
        query = "SELECT * FROM au_test.sag_csv where tipo_file='SAG'"
        
        #DEBUG
        #query = "SELECT * FROM au_test.aggregazioni_amm_gas_sag where tipo_file='SAG'"
        print query
        dataframe = sqlCtx.sql(query) 
        #dataframe.show(truncate = False)
        return dataframe

    def getSchema_AMMISSIBILITA_FILE(self):
        return StructType([
                StructField("valid", BooleanType(), True),
                StructField("FILE_NAME", StringType(), True),
                StructField("FILE_NAME_REL", StringType(), True),
                StructField("COD_PDR", StringType(), True),
                StructField("COD_REMI", StringType(), True),
                StructField("CONS_ANN", DoubleType(), True),
                StructField("COD_PROF_PREL_STD", StringType(), True),
                StructField("DATA_DS", StringType(), True),
                StructField("DATA_MIS1", StringType(), True),
                StructField("DATA_MIS2", StringType(), True),
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
