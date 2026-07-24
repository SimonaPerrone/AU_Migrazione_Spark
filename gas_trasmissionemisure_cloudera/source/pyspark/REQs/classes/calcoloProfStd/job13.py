

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *
from datetime import datetime

class Job13:
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job13"]["nameTableHive"]
        self.table = conf["job13"]["table"]
        self.cmdTableRefresh = conf["job13"]["cmdTableRefresh"]

        self.pathTabellaHDFS = conf["job13"]["pathTabellaHDFS"]
        self.database = conf["job13"]["database"]
    
    def clear_table(self, sqlCtx):
        sqlCtx.sql("TRUNCATE TABLE " + self.table)

    def backup_table(self, sqlCtx):
        try:
            cmd = ("hdfs dfs -ls "+ self.nameTableHive + " ").split() # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if (len(filter(None,files)) > 0):
                query = "LOAD DATA INPATH '" + self.nameTableHive + "' OVERWRITE INTO TABLE " + self.table + "_backup PARTITION (DATA_BACKUP='" + str(date.year) + str(date.month) + str(date.day) + "')"
                print query
                sqlCtx.sql(query)
        except:
            pass

    def run(self, sc, sqlCtx, params):
        """
        Si richiede l’implementazione di una tabella (TAB_PARAMETRI_CARATTERISTICI_PROF_PREL) e successivo caricamento 
        dei dati caratteristici dei profili di prelievo ß1, ß2, ß3, ß4.
        Req. Nella directory HDFS (/user/acu/au_test/misure_gas_au/TAB_PARAMETRI_CARATTERISTICI_PROF_PREL) il file csv 
        da importare
        """


        #PROF;b1;b2;b3;b4;Categoria d'uso;Zona climatica;Classe di prelievo
        externalTable =   "CREATE EXTERNAL TABLE IF NOT EXISTS " + self.table +        "    "\
                        + "   (                                                             "\
                        + "        PROF            String,                                  "\
                        + "        b1              double,                                  "\
                        + "        b2              double,                                  "\
                        + "        b3              double,                                  "\
                        + "        b4              double,                                  "\
                        + "        cat_uso         String,                                  "\
                        + "        zona_clim       String,                                  "\
                        + "        class_prelievo  double                                   "\
                        + "    )                                                            "\
                        + "    ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'                "\
                        + "    STORED AS TEXTFILE location '" + self.pathTabellaHDFS + "'   "\
                        + "    TBLPROPERTIES(\"skip.header.line.count\"=\"1\")              "
        sqlCtx.sql(externalTable)
        sqlCtx.sql(self.cmdTableRefresh)
        

        #print ("RUN: ", externalTable)


    def getSchema(self):
        return StructType([
                StructField("B1", StringType(), True),
                StructField("B2", StringType(), True),
                StructField("B3", StringType(), True),
                StructField("B4", StringType(), True)
        ])
