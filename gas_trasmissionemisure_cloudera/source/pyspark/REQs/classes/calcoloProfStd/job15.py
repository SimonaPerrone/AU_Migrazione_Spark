

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job15:
    """
    Popolamento della tabella TAB_COMUNE_ZON_CLIM.
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job15"]["nameTableHive"]
        self.table = conf["job15"]["table"]
        self.cmdTableRefresh = conf["job15"]["cmdTableRefresh"]

        self.pathTabellaGradiGiorni = conf["job15"]["pathTabellaGradiGiorni"]
        self.database = conf["job15"]["database"]


    def clear_table(self, sqlCtx):
        sqlCtx.sql("TRUNCATE TABLE " + self.table)

    def backup_table(self, sqlCtx):
        #query = "INSERT INTO " + self.table + "_backup select *, current_date() from " + self.table
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
        Si richiede una procedura che popoli la tabella TAB_COMUNE_ZONA_CLIM  a partire dalla tabella A del Decreto 412 del 26 Agosto 1993
        Req. Nella directory HDFS (/user/acu/au_test/misure_gas_au/TAB_COMUNE_ZONA_CLIM) il file csv da importare
        """

        externalTable = "CREATE EXTERNAL TABLE IF NOT EXISTS " + self.table +          "    "\
                      + "(                                                                  "\
                      + "  pz       String,                                                 "\
                      + "  z        String,                                                 "\
                      + "  gz       String,                                                 "\
                      + "  alt      String,                                                 "\
                      + "  comune   String                                                  "\
                      + " )                                                                 "\
                      + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'                     "\
                      + " STORED AS TEXTFILE location '" + self.pathTabellaGradiGiorni + "' "\
                      + " TBLPROPERTIES(\"skip.header.line.count\"=\"1\")                   "
        sqlCtx.sql(externalTable)
        sqlCtx.sql(self.cmdTableRefresh)
        

        print ("RUN: ", externalTable)
   
