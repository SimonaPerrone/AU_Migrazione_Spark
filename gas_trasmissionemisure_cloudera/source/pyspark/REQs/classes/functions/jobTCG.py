
import os
import zipfile
import re
import datetime
import shutil
from pyspark.sql.types import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.functions import udf,col

class JobTCG:
    def __init__(self, conf):
        now = datetime.datetime.now()

        self.conf = conf
        self.number_obligatory = conf["TCG"] ["NUMBER_RECORDS_OBLICATORY"] 
        self.pattern = conf["TCG"]["PATTERN_VALID_FILE"]
        self.dirDest = conf["TCG"]["WORKDIR"]
        self.dirOutput =  conf["TCG"]["OUTPUT"]
        self.has_header = conf["TCG"]["HEADER_CSV"]
        self.time_stamp =  str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
        self.nameTableHive = conf["TCG"]["TABLEHIVE"]
        self.cmdTableRefresh = conf["TCG"]["cmdTableRefresh"]
        self.table = conf["TCG"]["table"]
  
    def clear_table(self, sqlCtx):
        query = "TRUNCATE TABLE " + self.table
        sqlCtx.sql(query)

    def backup_table(self, sqlCtx):
        import subprocess
        date = datetime.datetime.now()
        try:
            cmd = ("hdfs dfs -ls "+ self.nameTableHive + " ").split() # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if (len(filter(None,files)) > 0):
                query = "LOAD DATA INPATH '" + self.nameTableHive + "' INTO TABLE " + self.table + "_backup PARTITION (DATA_BACKUP='" + str(date.year) + str(date.month) + str(date.day) + "')"
                print query
                sqlCtx.sql(query)
        except:
            pass
            
        
    def getWorkdir(self):
        return self.dirDest

    def valida_record(self, obj):
        """ Procedura per validare il record.
        	La struttura del file CSV: 
            COD_PDR;CAT_USO;CLASSE_PRELIEVO;TIPOL_USO;COD_PROF_PREL_STD

        	Ritorna l'oggetto: 
            VALIDO, COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD
            
        """
        
        file_name = obj[0]
        record = obj[1]
        #print ("Valida Record:", record)
        
        # Verifica se il record e' empty
        if not record:
            return (False, file_name, "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo()[" + record + "]")  
        
        if not record.strip():
            return (False, file_name, "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo()[" + record + "]")  

        items = record.split(";")

        if len(items) < int(self.number_obligatory):
            return (False, file_name, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati ()[" + record + "]")  

        COD_REMI            = items[0]
        COD_PROF_PREL_STD   = items[1]
        TIPO_PREL           = items[2]
        UNIT_MIS_PREL       = items[3]
        PRELIEVO_GIORN      = items[4]

        #print ("check record: ", COD_PDR, CAT_USO, CLASSE_PRELIEVO, TIPOL_USO, COD_PROF_PREL_STD)

        # Verifica se gli attributi obblicatori sono presenti
       
        if self.checkField("COD_REMI", COD_REMI): 
            return (False, file_name, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (COD_REMI)[" + record + "]")  
        
        if self.checkField("COD_PROF_PREL_STD", COD_PROF_PREL_STD): 
            return (False, file_name, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (COD_PROF_PREL_STD)[" + record + "]")  
        
        if self.checkField("TIPO_PREL", TIPO_PREL): 
            return (False, file_name, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (TIPO_PREL)[" + record + "]")  

        if self.checkField("UNIT_MIS_PREL", UNIT_MIS_PREL): 
            return (False, file_name, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (UNIT_MIS_PREL)[" + record + "]")    

        if self.checkField("PRELIEVO_GIORN", PRELIEVO_GIORN): 
            return (False, file_name, "004", "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (PRELIEVO_GIORN)[" + record + "]")  


        return (True, file_name,COD_REMI, COD_PROF_PREL_STD, TIPO_PREL, UNIT_MIS_PREL, PRELIEVO_GIORN)
        
    def checkField(self, pattern, field):
        return not bool(re.match(self.conf["TCG"][pattern], field))

    def validate_filename(self, s):
        """ Procedura per validare il file.
        	Verifica della nomenclatura del file: <PIVA_UdD>_TDS_<AAAAMM>_<progressivo>.csv
        
        	Ritorna True se il file e' valido
        """

        file_name = s.replace("file://", "")
        file_name = os.path.basename(file_name)

        # print("Validate result: ",file_name, bool(re.match(self.pattern, file_name)))
        return bool(re.match(self.pattern, file_name))
    
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
    
    def save_data(self, sqlCtx, rdd, cmdTableRefresh = "", partitionTableHive = None ):
        """ Procedura usata per savare i dati nella tabella HIVE 
            sqlCtx: Context Spark
            rdd: rdd Table 
            cmdTableRefres: command to refresh Table
            partitionTableHive: list partition field 
        """
        
        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)

        dataFrame = sqlCtx.createDataFrame(rdd, schema=self.getSchema())
        
        if (partitionTableHive != None):
        	dataFrame.write.partitionBy(partitionTableHive).parquet(self.nameTableHive, 'append')
        else:
        	dataFrame.write.parquet(self.nameTableHive, 'append')
        
        sqlCtx.sql(self.cmdTableRefresh)
        
    def save_anomalie_csv(self, dataFrame):
        dataFrame.coalesce(1).foreach(self.save_csv_anom)

    def save_ammissibilita_csv(self, dataFrame):
        res = dataFrame.withColumn("NUM_RIGA", monotonicallyIncreasingId())
        res = res.withColumn("COD_TIPO_FILE", lit('TDS'))
        #res = res.withColumn("PIVA_UTENTE", self.get_piva(col('FILE_NAME')))

        funcPiva = udf(lambda file_name : self.get_piva(file_name), StringType())
        funcVERIFICA_AMM = udf(lambda valid : self.get_verifica_amm(valid), StringType())
        res = res.withColumn('PIVA_UTENTE',funcPiva(col('FILE_NAME')))
        res = res.withColumn('VERIFICA_AMM',funcVERIFICA_AMM(col('valid')))

        res.coalesce(1).foreach(self.save_csv_amm)

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

    def save_csv_anom(self, x):
        #NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        txt = ";".join([ "", "", "", "N", str(x[2]), str(x[3]) ]) + "\n"
        
        name_file = self.dirOutput +\
                    x[1].replace("file:" + self.dirDest, "").replace(".csv", "") +\
                    "_ANOMALIE_" +\
                    self.time_stamp +\
                    ".csv"

        f = open(name_file,"a+")
        f.write(txt)
       
    def save_csv_amm(self, x):
        #NUM_RIGA;COD_TIPO_FILE;PIVA_UTENTE;VERIFICA_AMM;COD_CAUSALE;MOTIVAZIONE
        txt = ";".join([ str(x[8]), str(x[9]), str(x[10]), str(x[11]), ";", ";"]) + "\n"
        
        name_file = self.dirOutput +\
                    x[1].replace("file:" + self.dirDest, "").replace(".csv", "") +\
                    "_AMM_" +\
                    self.time_stamp +\
                    ".csv"

        f = open(name_file,"a+")
        f.write(txt)

    def getSchema(self):
        return self.getSchema_AMMISSIBILITA_FILE()

    def get_duplicate(self, dataframe):
        pass

    def check_rcu(self,sqlCtx, dataframe):
        return (None, dataframe)
        pass

    def check_rcu_udd(self, sqlCtx, dataframe):
        return (None, dataframe)
        pass

    def check_rcu_filename(self, sqlCtx, dataframe):
        return (None, dataframe)
        pass

    def getSchema_AMMISSIBILITA_FILE(self):
        return StructType([
                StructField("valid", BooleanType(), True),
                StructField("FILE_NAME", StringType(), True),
                StructField("COD_REMI", StringType(), True),
                StructField("COD_PROF_PREL_STD", StringType(), True),
                StructField("TIPO_PREL", StringType(), True),
                StructField("UNIT_MIS_PREL", StringType(), True),
                StructField("PRELIEVO_GIORN", StringType(), True)
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
                StructField("File", StringType(), True)
                
        ])
