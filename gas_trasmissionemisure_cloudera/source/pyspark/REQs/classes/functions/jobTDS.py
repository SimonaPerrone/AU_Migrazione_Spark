import os
import zipfile
import re
import shutil
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.functions import udf,col
from pyspark.sql.functions import col
import pyspark.sql.functions as f

class JobTDS:
    def __init__(self, conf):
        now = datetime.datetime.now()

        self.conf = conf
        self.number_obligatory = conf["TDS"] ["NUMBER_RECORDS_OBLICATORY"]
        self.pattern = conf["TDS"]["PATTERN_VALID_FILE"]
        self.dirDest = conf["TDS"]["WORKDIR"]
        self.dirOutput =  conf["TDS"]["OUTPUT"]
        self.has_header = conf["TDS"]["HEADER_CSV"]
        self.time_stamp =  str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
        self.nameTableHive = conf["TDS"]["TABLEHIVE"]
        self.cmdTableRefresh = conf["TDS"]["cmdTableRefresh"]
        self.table = conf["TDS"]["table"]
        self.TABLEHIVE_AMMISSIBILITA = conf["TDS"]["TABLEHIVE_AMMISSIBILITA"]
        self.TABLEHIVE_ANOMALIE = conf["TDS"]["TABLEHIVE_ANOMALIE"]
        self.database = conf["TDS"]["database"]
        self.tableAmmissibilita = conf["TDS"]["tableAmmissibilita"]
        self.tableAnomalie = conf["TDS"]["tableAnomalie"]
        self.TABLEHIVE_ANOMALIE_F = conf["TDS"]["TABLEHIVE_ANOMALIE_F"]
        self.tableAnomalieF = conf["TDS"]["tableAnomalieF"]


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
        self.backup_ammissibilita(sqlCtx)
        self.backup_anomalie(sqlCtx)
        #self.backup_table__(sqlCtx, self.tableAmmissibilita, self.TABLEHIVE_AMMISSIBILITA)
        #self.backup_table__(sqlCtx, self.tableAnomalie, self.TABLEHIVE_ANOMALIE)
        print ("Backup temp tables")

    def backup_anomalie(self, sqlCtx):
        print("Backup table: settle_gas.anomalie_file_tds_backup")
        date = datetime.datetime.now()
        data=date.strftime('%Y%m%d')
        query="""INSERT INTO settle_gas.anomalie_file_tds_backup PARTITION(data_backup)
            select
               file,
               cod_causale,
               descrizione,
               tipo_file,
               data_import,
               num_riga,
               data_creazione,
               '{}' as data_backup
            from settle_gas.anomalie_file_tds
        """.format(data)

        #print("Query:{}".format(query))
        #TODO attivare
        sqlCtx.sql(query)


    def backup_ammissibilita(self, sqlCtx):
        print("Backup table: settle_gas.ammissibilita_file_tds")
        date = datetime.datetime.now()
        data=date.strftime('%Y%m%d')
        query="""INSERT INTO settle_gas.ammissibilita_file_tds_backup PARTITION(data_backup)
            select
               valid,
               file_name,
               file_name_rel,
               cod_pdr,
               cat_uso,
               classe_prelievo,
               tipol_uso,
               cod_prof_prel_std,
               cod_tipo_file,
               piva_utente,
               verifica_amm,
               tipo_file,
               data_import,
               num_riga,
               data_creazione,
               '{}' as data_backup
            from settle_gas.ammissibilita_file_tds
        """.format(data)

        #print("Query:{}".format(query))
        #TODO attivare
        sqlCtx.sql(query)

    def backup_table(self, sqlCtx):
        self.backup_table__(sqlCtx, self.table, self.nameTableHive)

    def backup_table__(self, sqlCtx, name_table, path_hive):
        import subprocess
        date = datetime.datetime.now()
        try:
            cmd = ("hdfs dfs -ls "+ path_hive + " ").split() # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if len(filter(None, files)) > 0:
                query = "LOAD DATA INPATH '{}' INTO TABLE {}_backup PARTITION (DATA_BACKUP='{}')"\
                    .format(path_hive, name_table, str(date.year) + str(date.month) + str(date.day))

                #TODO attivare
                #sqlCtx.sql(query)
        except Exception as ex:
            print("Error Backup_table__: {}".format(ex))
            pass

    def getWorkdir(self):
        return self.dirDest

    def getHeaderCodeError(self):
        #code, message = function.getHeaderCodeError()
        return ("200", "Il file non rispetta la struttura prevista - Nome del file non conforme")

    def check_definition_record(self, header):
        #print "Check header"
        #print header
        #print ("PATTERN_VALID_TRACC_RECORD: ", self.conf["TDS"]["PATTERN_VALID_TRACC_RECORD"], bool(re.match(self.conf["TDS"]["PATTERN_VALID_TRACC_RECORD"], header)))
        return bool(re.match(self.conf["TDS"]["PATTERN_VALID_TRACC_RECORD"], header))

    def valida_record(self, obj, file_name):
        """ Procedura per validare il record.
                La struttura del file CSV:
            COD_PDR;CAT_USO;CLASSE_PRELIEVO;TIPOL_USO;COD_PROF_PREL_STD

                Ritorna l'oggetto:
            VALIDO, COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD

        """
        record = obj
        # Verifica se il record e' empty

        ll = [word for word in record.split(';')]
        items = list(filter(None, ll))

        NUM_ROW             = self.get_items(items,0)
        COD_PDR             = self.get_items(items,1)
        CAT_USO             = self.get_items(items,2)
        CLASSE_PRELIEVO     = self.get_items(items,3)
        TIPOL_USO           = self.get_items(items,4)
        COD_PROF_PREL_STD   = ""


        #print ("check record: ", COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD)
        file_basename = os.path.basename(file_name)
        # Verifica se gli attributi obblicatori sono presenti
        # and COD_PDR_OBLICATORY
        #print (re.match(self.conf["TDS"]["PATTERN_VALID_COD_PDR"], COD_PDR))
        #print (self.conf["TDS"][pattern], field,  bool(re.match(self.conf["TFC"][pattern], field)))
        if not COD_PDR:
            return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (COD_PDR non presente) (COD_PDR)[" + record + "]", NUM_ROW)
        if not CAT_USO:
            return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (CAT_USO non presente) (COD_PDR)[" + record + "]", NUM_ROW)
        if not CLASSE_PRELIEVO:
            return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (CLASSE_PRELIEVO non presente) (COD_PDR)[" + record + "]", NUM_ROW)
        #if not TIPOL_USO:
        #    return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (TIPOL_USO non presente) (COD_PDR)[" + record + "]", NUM_ROW)

        if not bool(re.match(self.conf["TDS"]["PATTERN_VALID_COD_PDR"], COD_PDR)) :
            #print("Error COD_PDR: ", COD_PDR)
            return (False, file_name, file_basename,  "238", "Codice PdR strutturalmente non corretto (COD_PDR)[" + record + "]", NUM_ROW)

        if not bool(re.match(self.conf["TDS"]["PATTERN_VALID_CAT_USO"], CAT_USO)) :
            #print("Error CAT_USO: ", CAT_USO)
            return (False, file_name, file_basename,  "273", "Categoria d'uso incongruente (CAT_USO)[" + record + "]", NUM_ROW)

        if not bool(re.match(self.conf["TDS"]["PATTERN_VALID_CLASSE_PRELIEVO"], CLASSE_PRELIEVO)) :
            #print("Error CLASSE_PRELIEVO: ", CLASSE_PRELIEVO)
            return (False, file_name, file_basename,  "287", "Classe_Prelievo non conforme (CLASSE_PRELIEVO)[" + record + "]", NUM_ROW)

        if TIPOL_USO:
            if not bool(re.match("^(01|02|03|04|05|06|07)$", TIPOL_USO)) :
                return (False, file_name, file_basename,  "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (TIPOL_USO non corretto)[" + record + "]", NUM_ROW)


        return (True, file_name, file_basename,  COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD, NUM_ROW)

    def get_items(self, items_array, index):
        #import traceback

        try:
            return items_array[index]
        except Exception:
            #traceback.print_exc()
            return ""

    def validate_filename(self, s):
        """ Procedura per validare il file.
                Verifica della nomenclatura del file: <PIVA_UdD>_TDS_<AAAAMM>_<progressivo>.csv

                Ritorna True se il file e' valido
        """

        file_name = s.replace("file://", "")
        file_name = os.path.basename(file_name)


        # print("Validate result: ",file_name, bool(re.match(self.pattern, file_name)))
        return bool(re.match(self.pattern, file_name))

    def getNewFiles(self,sqlCtx, listFiles):
        listResult = []
        for f in listFiles:
            query = "select * from  " + self.database + ".ammissibilita_file_tds where file_name like '%" + f +"%'"
            df = sqlCtx.sql(query)
            if df.count() <= 0:
                listResult.append(f)
        return listResult

    def is_zipfile(self, filename):
        fileexts = ['.zip'] #TODO da spostare nel file JSON

        file_name = filename.replace("file://", "")
        file_name = os.path.basename(file_name)
        ext = os.path.splitext(file_name)[1]

        return bool(ext in fileexts)

    def extra(self,sqlCtx, dataframe, rcugas_massivo_dataframe, rcugas_connessioni_distr, rcugas_tmp_va1):
        print ("extra")
        return (dataframe, None)

    def move_file(self, filename):

        filename = filename.replace("file://", "")
        baseDir = os.path.dirname(filename) + "/"
        files_result = os.path.join( self.dirDest , filename.replace(baseDir, ""))

        print("[move_file] {} filename:{}, dirDest:{}", self, filename, self.dirDest)
        try:
            if not os.path.exists(self.dirDest):
                os.makedirs(self.dirDest)
            shutil.copy2(filename, self.dirDest)
        except Exception as ex:
            print("Error move_file: {}".format(ex))
            if not os.path.exists(files_result):
                pass

    def decomprimi(self, fileSrc):
        """ Decomprime il file zip.
        Ritorna True se la decompressione e' stata eseguita altrimenti False
        """

        fileSrc = fileSrc.replace("file://","")
        dirDest = self.dirDest

        try:
            file_obj = zipfile.ZipFile(fileSrc, "r")
            file_obj.extractall(dirDest)
        except Exception as ex:
            print("Error decomprimi: {}".format(ex))
            return False

        return True

    def save_data_part(self, sqlCtx, rdd):

        data_part_udf = udf (
           lambda d: datetime.datetime.today().strftime('%Y%m%d')
        )
        data_creazione_udf = udf(
            lambda filename: datetime.datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%d/%m/%y %H:%M:%s')
        )
        rdd = rdd.withColumn('data_creazione', data_creazione_udf(col('FILE_NAME')))
        rdd = rdd.withColumn('data_elab', data_part_udf(col('FILE_NAME')))
	location="/user/hive/warehouse/settle_gas.db/gas_tds_part"
        print("save in :{}".format(location))
        rdd.write.partitionBy(["data_elab"]).parquet(location, 'append')
        sqlCtx.sql("MSCK REPAIR TABLE settle_gas.gas_tds")


    def save_data(self, sqlCtx, rdd,  partitionTableHive = None ):
        """ Procedura usata per savare i dati nella tabella HIVE
            sqlCtx: Context Spark
            rdd: rdd Table
            cmdTableRefres: command to refresh Table
            partitionTableHive: list partition field
        """

        data_part_udf = udf (
           lambda d: datetime.datetime.today().strftime('%Y%m%d')
        )

        data_creazione_udf = udf(
            lambda filename: datetime.datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%d/%m/%y %H:%M:%s')
        )
        rdd = rdd.withColumn('data_creazione', data_creazione_udf(col('FILE_NAME')))

        #print("[save_data] Count: {}".format(rdd.count()))
        #rdd.show(truncate = False);

        #TODO Eliminare set del nome path
        print("save in :{}".format(self.nameTableHive))
        rdd.write.parquet(self.nameTableHive, 'append')
        sqlCtx.sql(self.cmdTableRefresh)

        #self.save_data_part(sqlCtx, rdd)


    def save_data_ammissiblita(self, sqlCtx, dataFrame):
        """ Salva tabella in formato CSV

            NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE

            NUM_RIGA:  SI - Identifica la corrispondente riga del file inviato - Numerico (14)
            COD_TIPO_FILE: SI - Codice che identifica il tipo file inviato - Valori in lista (TDS, TFC, VPG, TCG, CGR)
            PIVA_UTENTE: SI - Partiva IVA dell'Utente che trasmesso il file - Alfanumerico (16)
            VERIFICA_AMM: SI - Campo che definisce l'ammissibilita della singola riga - Valori in lista (Y, N)
            COD_CAUSALE: SI -  (se VERICA_AMM = N) Codice d'inammissibilita - Numerico (3)
            MOTIVAZIONE: SI - (se VERICA_AMM = N) Motivazione dello scarto della singola riga - Alfanumerico (256)

            Formato file
            <PIVA_UdD>_TDS_<AAAAMM>_<progressivo>_AMM_<Timestamp>.csv
        """
        date = datetime.datetime.now()

        data_creazione_udf = udf(
            lambda filename: datetime.datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%d/%m/%y %H:%M:%s')
        )

        #Creazione Id
        #res = dataFrame.withColumn("NUM_RIGA", monotonicallyIncreasingId())
        res = dataFrame
        res = res.withColumn("COD_TIPO_FILE", lit('TDS'))
        #res = res.withColumn("PIVA_UTENTE", self.get_piva(col('FILE_NAME')))

        funcPiva = udf(lambda file_name : self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid : self.get_verifica_amm(valid), StringType())
        res = res.withColumn('PIVA_UTENTE',funcPiva(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM',funcVERIFICA_AMM(col('valid')))
        res = res.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        res = res.withColumn('tipo_file',lit( 'TDS' ))
        res = res.withColumn('data_creazione', data_creazione_udf(col('FILE_NAME')))

        #res.show(truncate = False)

        print("save ammisibilita table: {}".format(self.TABLEHIVE_AMMISSIBILITA))
        res.write.parquet(self.TABLEHIVE_AMMISSIBILITA, 'append')
        #sqlCtx.sql("MSCK REPAIR TABLE "+ self.tableAmmissibilita)
        self.repair_table(sqlCtx, self.tableAmmissibilita)

        print("Salva Dataframe OK")

    def repair_table(self, sqlCtx, name_table):
        query = "MSCK REPAIR TABLE " + name_table
        sqlCtx.sql(query)

    def save_data_anomalie_file(self, sqlCtx, rdd):
        date = datetime.datetime.now()

        rdd = rdd.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        rdd = rdd.withColumn('tipo_file',lit( 'TDS' ))

        print("save anomalie table: {}".format(self.TABLEHIVE_ANOMALIE))
        rdd.write.parquet(self.TABLEHIVE_ANOMALIE_F, 'append')

        self.repair_table(sqlCtx, self.tableAnomalieF)

    def save_data_anomalie(self, sqlCtx, rdd):
        if rdd is None:
            print("rdd is empty or null;")
            return

        date = datetime.datetime.now()

        data_creazione_udf = udf(
            lambda filename: datetime.datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%d/%m/%y %H:%M:%s')
        )
        rdd = rdd.withColumn('data_creazione', data_creazione_udf(col('file')))

        #dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema_ANOMALIE_FILE_DATA())
        rdd = rdd.withColumn('data_import',lit( str(date.year) + str(date.month) + str(date.day) ))
        rdd = rdd.withColumn('tipo_file',lit( 'TDS' ))
        #rdd.show(truncate = False)

        print("Write to:{}".format(self.TABLEHIVE_ANOMALIE))
        rdd.write.parquet(self.TABLEHIVE_ANOMALIE, 'append')

        self.repair_table(sqlCtx, "settle_gas.anomalie_file_tds")

    def save_to_csv(self, sqlCtx, dataframe):
        for it in dataframe.collect():
            print("[save_to_csv] it:{}".format(it))
            self.save_csv_amm(it)
        #dataframe.foreach(self.save_csv_anom)

    def save_csv_anom(self, x):
        #NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        #print x
        #print str(x[8])
        #self.dirDest="/tmp/test"

        txt = ";".join([ str(x[13]), str(x[7]), str(x[8]),str(x[9]), str(x[10]),str(x[11])]) + "\n"
        path_file = x[1]
        name_file = path_file.replace("file://" + self.dirDest, "").replace(".csv", "") +\
                    "_ANOMALIE_" +\
                    self.time_stamp +\
                    ".csv"

        #TODO ATTIVARE
        #f = open(name_file,"a+")
        #f.write(txt)

    def save_to_file(self, name_file, txt):

        #TODO Attivare
        #f = open(name_file,"a+")
        #f.write(txt)
        pass

    def save_csv_amm(self, x):
        #
        #0 valid
        #1 FILE_NAME
        #2 FILE_NAME_REL
        #3 COD_PDR
        #4 CAT_USO
        #5 CLASSE_PRELIEVO
        #6 TIPOL_USO
        #7 COD_PROF_PREL_STD
        #8 COD_TIPO_FILE
        #9 PIVA_UTENTE
        #10 VERIFICA_AMM
        #11 COD_CAUSALE
        #12 DESCRIZIONE
        #13 tipo_file
        #14 num_riga
        #15 data_import
        #                                                                                                                                                                           |tipo_file|num_riga|data_import|
        #NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        path_file = x[1]
        #TODO ELIMINARE
        #self.dirDest="/tmp/test"

        name_file = path_file.replace("file://" + self.dirDest, "").replace(".csv", "") +\
                    "_AMM_" +\
                    self.time_stamp +\
                    ".csv"
        #name_file = "/tmp/prova.csv"

        num_riga = str(x[14].encode('utf-8'))
        COD_TIPO_FILE = str(x[8].encode('utf-8'))
        piva = str(x[9].encode('utf-8'))
        VERIFICA_AMM = str(x[10].encode('utf-8'))
        COD_CAUSALE = str(x[11].encode('utf-8'))
        DESCRIZIONE = str(x[12].encode('utf-8'))

        txt = ";".join([ num_riga, COD_TIPO_FILE, piva, VERIFICA_AMM, COD_CAUSALE, DESCRIZIONE]) + "\n"
        #self.write(name_file, txt)

        f = open(name_file,"a+")
        f.write(txt)



    def save_to_csv_test(self, sqlCtx, dataframe):
        for it in dataframe.collect():
            #print("[save_to_csv] it:{}".format(it))
            self.save_csv_amm_test(it)

    def save_csv_amm_test(self, x):
        #
        #0 valid
        #1 FILE_NAME
        #2 FILE_NAME_REL
        #3 COD_PDR
        #4 CAT_USO
        #5 CLASSE_PRELIEVO
        #6 TIPOL_USO
        #7 COD_PROF_PREL_STD
        #8 COD_TIPO_FILE
        #9 PIVA_UTENTE
        #10 VERIFICA_AMM
        #11 COD_CAUSALE
        #12 DESCRIZIONE
        #13 tipo_file
        #14 num_riga
        #15 data_import
        #                                                                                                                                                                           |tipo_file|num_riga|data_import|
        #NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        path_file = x[1]
        #TODO ELIMINARE
        #self.dirDest="/tmp/test"

        name_file = path_file.replace("file://" + self.dirDest, "").replace(".csv", "") +\
                    "_AMM_" +\
                    self.time_stamp +\
                    ".csv"
        #name_file = "/tmp/prova.csv"
        #name_file = "/tmp/test/" + os.path.basename(name_file)

        num_riga = str(x[14].encode('utf-8'))
        COD_TIPO_FILE = str(x[8].encode('utf-8'))
        piva = str(x[9].encode('utf-8'))
        VERIFICA_AMM = str(x[10].encode('utf-8'))
        COD_CAUSALE = str(x[11].encode('utf-8'))
        DESCRIZIONE = str(x[12].encode('utf-8'))

        txt = ";".join([ num_riga, COD_TIPO_FILE, piva, VERIFICA_AMM, COD_CAUSALE, DESCRIZIONE]) + "\n"

        #print("Scrittura file:{}".format(name_file))
        f = open(name_file,"a+")
        f.write(txt)



    def write(self, name_file, txt):
        #TODO Attivare
        #f = open(name_file,"a+")
        #f.write(txt)
        pass

    def get_verifica_amm(self, valid):
        if (valid):
            return 'Y'
        else:
            return 'N'

    def get_piva(self, filename):
        file_name = os.path.basename(filename.replace("file://",""))

        items = file_name.split("_")
        pIvaUdD = items[0]

        return pIvaUdD

    def getSchema(self):
        return self.getSchema_AMMISSIBILITA_FILE()

    def stampa(self, value):
        print("***** ", value)

    def load_tables(self, sqlCtx):
        query_rcugas_massivo = "select t_codice_pdr, from_unixtime(unix_timestamp(DATA_FINE_FOR , 'yyyy-MM-dd')) as DATA_FINE_FOR, piva_udd   from rcugas.rcugas_massivo_p"
        query_rcuazienda = "select rcu_azienda_p.t_piva from rcu.rcu_azienda_p"

        print "Load RCU MASSIVO"
        dataframe_rcumassivo = sqlCtx.sql(query_rcugas_massivo).cache()

        print "Load RCU Azienda"
        dataframe_rcuazienda = sqlCtx.sql(query_rcuazienda).cache()

        return dataframe_rcumassivo, dataframe_rcuazienda, None, None

    def get_duplicate(self, sqlCtx, dataframe):
        print "Verifica Doppio"
        if dataframe is None:
            return None, dataframe

        dataframe2 = dataframe.alias('tab')
        dataframe3 = dataframe2.groupBy(dataframe2.COD_PDR, dataframe2.FILE_NAME)\
                               .agg({'*': 'count'})\
                               .filter(col('count(1)') == 1)\
                               .alias('table3')

        dataframe3 = dataframe3.alias('table3')
        dataframeOK = dataframe2.join(dataframe3, (dataframe3.COD_PDR == dataframe2.COD_PDR) & (dataframe3.FILE_NAME == dataframe2.FILE_NAME))\
                                .select('tab.*')

        dataframe4 = dataframe2.groupBy(dataframe2.COD_PDR, dataframe2.FILE_NAME)\
                               .agg({'*': 'count'})\
                               .filter(col('count(1)') > 1)\
                               .alias('table3')
        rddKO = dataframe2.join(dataframe4, (dataframe3.COD_PDR == dataframe2.COD_PDR) & (dataframe4.FILE_NAME == dataframe2.FILE_NAME))\
                          .select('tab.*')\
                          .map(lambda f: ("004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (" + str(f[3]) + " presente piu volte nel file " + str(f[2]) + ")", f[1], f[8]))


        dataframeKO = sqlCtx.createDataFrame(
                            rddKO,
                            self.getSchema_ANOMALIE_FILE())


        return dataframeOK, dataframeKO

    def get_rcu_1(self,sqlCtx, dataframe, dataframe_rcumassivo):
        #from pyspark.sql.functions import col
        #import time

        print "RCU inesistente"
        if dataframe is None:
            return (None, dataframe)

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
                               .map(lambda f: ("226", "Il PdR e' inesistente",  f[1], f[8]))

        dataframeKO = sqlCtx.createDataFrame(
            rddKO,
            self.getSchema_ANOMALIE_FILE())

        #dataframeOK.show(truncate = False)
        #dataframeKO.show(truncate = False)

        #time2 = time.time()
        #print 'function took %0.3f ms' % ( (time2-time1)*1000.0)
        #return dataframeOK, dataframeKO

        return dataframeOK, dataframeKO

    def get_rcu_2(self,sqlCtx, dataframe, dataframe_rcumassivo):
        print "RCU attivo"
        if dataframe is None:
            return (None, dataframe)

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
                         .map(lambda f: ( "212", "Il PdR non e' attivo", f[1], f[8]))

        dataframeKO = sqlCtx.createDataFrame(
                    rddKO,
                    self.getSchema_ANOMALIE_FILE())

        return dataframeOK, dataframeKO

    def check_rcu_filename(self, sqlCtx, dataframe, dataframe_rcuazienda):
        print "Check rcu filename"
        if dataframe is None:
            return (None, dataframe)


        #dataframe = dataframe.alias('tab')
        dataframe_rcuazienda = dataframe_rcuazienda.alias('dataframe_rcuazienda')

        dataframe2 = dataframe.withColumn("piva", f.split(col("FILE_NAME_REL"), "_")[0]).alias('tab')
        dataframeOK = dataframe2.join(dataframe_rcuazienda, dataframe2.piva == dataframe_rcuazienda.t_piva, how='left')\
                                .filter(dataframe_rcuazienda.t_piva.isNotNull())\
                                .select('tab.*')\
                                .distinct()\
                                .drop('piva')

        rddKO = dataframe2.join(dataframe_rcuazienda, dataframe2.piva == dataframe_rcuazienda.t_piva, how='left')\
                          .filter(dataframe_rcuazienda.t_piva.isNull())\
                          .select('tab.*')\
                          .distinct()\
                          .drop('piva')\
                          .map(lambda f: ("209", "Non e' stata rispettata la corrispondenza delle informazioni inviate - Partita IVA presente nel nome del file non corrispondente alla Regione Sociale indicata contestualmente all'accreditamento al SII",f[1], f[8]))


        dataframe_KO = sqlCtx.createDataFrame(\
            rddKO,\
            self.getSchema_ANOMALIE_FILE())


        return dataframeOK, dataframe_KO

    def get_rcu_rcu_udd(self, sqlCtx, dataframe, dataframe_rcumassivo):
        print "Check rcu UdD"

        dataframe2 = dataframe.withColumn("piva", f.split(col("FILE_NAME_REL"), "_")[0]).alias('tab')
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
                          .map(lambda f: ("213", "L'UdD non e' titolare del PdR", f[1], f[8]))


        dataframe_KO = sqlCtx.createDataFrame(\
            rddKO,\
            self.getSchema_ANOMALIE_FILE())

        return dataframeOK, dataframe_KO


    def generate_dataframe_to_csv(self, sqlCtx):
        print "Union dataframe OK - KO"
        date = datetime.datetime.now()
        query = "SELECT * FROM settle_gas.aggregazioni_amm_gas where data_import = '" +  str(date.year) + str(date.month) + str(date.day) + "' and tipo_file='TDS'"
        print("query:{}".format(query))
        dataframe = sqlCtx.sql(query)
        print("Count:{}".format(dataframe.count()))
        #dataframe.show(truncate = False)
        return dataframe


    def get_filename_amm(self, filename_input):
        now = datetime.datetime.now()
        time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)

        return filename_input.replace(".csv", "") + "_AMM_" + time_stamp + ".csv"

    def getSchema_AMMISSIBILITA_FILE(self):
        return StructType([
                StructField("valid", BooleanType(), True),
                StructField("FILE_NAME", StringType(), True),
                StructField("FILE_NAME_REL", StringType(), True),
                StructField("COD_PDR", StringType(), True),
                StructField("CAT_USO", StringType(), True),
                StructField("CLASSE_PRELIEVO", StringType(), True),
                StructField("TIPOL_USO", StringType(), True),
                StructField("COD_PROF_PREL_STD", StringType(), True),
                StructField("NUM_RIGA", StringType(), True)
        ])

    def getSchema_ANOMALIE_FILE_DATA(self):
        return StructType([
                StructField("valid", BooleanType(), True),
                StructField("FILE_NAME", StringType(), True),
                StructField("COD_CAUSALE", StringType(), True),
                StructField("DESCRIZIONE", StringType(), True),
                StructField("data_import", StringType(), True)

        ])

    def getSchema_ANOMALIE_FILE(self):
        return StructType([
                StructField("COD_CAUSALE", StringType(), True),
                StructField("DESCRIZIONE", StringType(), True),
                StructField("File", StringType(), True),
                StructField("NUM_RIGA", StringType(), True)

        ])
