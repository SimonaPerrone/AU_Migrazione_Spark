

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job21:
    """
    Job di calcolo dei segmenti di Consumo per i PdR MM e YY (mensile e altro).    
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job21"]["nameTableHive"]
        self.cmdTableRefresh = conf["job21"]["cmdTableRefresh"]
        self.table = conf["job21"]["table"]
        self.database = conf["job21"]["database"]


    def clear_table(self, sqlCtx):
        sqlCtx.sql("TRUNCATE TABLE " + self.table)

    def backup_table(self, sqlCtx):
        import subprocess
        date = datetime.datetime.now()
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
        Si richiede un job che incrociando le informazioni delle tabelle:
        -	TAB_PDR_SEGMENTO_DELTA_MIS_MM contenente le differenze delle misure per ogni 
            segmento identificato

        -	TAB_PROFILI_GIORN_STD_PERC contenente i codici profilo, regione climatica

        -	TAB_SETTLE_GAS_PROF_PDR contenente i profili percentuali giornalieri calcolati 
            al requisito 18 e la regione climatica

        Calcoli il consumo medio per singolo PDR e segmento applicando la formula:
        
        Dove:
        mis2 – mis1	corrisponde al valore Delta_Mis

        corrisponde ai valori percentuali estratti dalla tabella TAB_PROFILI_GIORN_STD_PERC 
        per il codice profilo e la regione climatica indicate nella tabella TAB_SETTLE_GAS_PROF_PDR 
        per il PdR

        I risultati devono essere inseriti nella tabella TAB_PDR_SEGMENTO_CONSUMO_MM. 
        In particolare nella tabella deve essere memorizzato anche il risultato della sommatoria .


        """
        self.backup_table(sqlCtx)

        #TAB_PDR_SEGMENTO_DELTA_MIS_MM => PDR, DATA_DZ, DATA_DZ1, DELTA_MIS, DATA_AZ, DATA_Az1, TRATTAMENTO
        #TAB_PROFILI_GIORN_STD_PERC => DATA, Wk, PProfk
        #TAB_SETTLE_GAS_PROF_PDR => anno_rif, pdr, cat_uso, class_prel, zona_clim, cod_prof_prel_std, trattamento, cons_annuo, regione_clim
        query0 = "      SELECT                                                                                                                                                                                                           "\
        + "                 TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr                                                                                                                                                "\
        + "             FROM                                                                                                                                                                                                             "\
        + "             " + self.database + ".TAB_PROFILI_GIORN_STD_PERC," + self.database + ".TAB_PDR_SEGMENTO_DELTA_MIS_MM                                                                                                                                        "\
        + "             WHERE                                                                                                                                                                                                           "\
        + "                 TAB_PROFILI_GIORN_STD_PERC.data >= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ                                                                                                                                    "\
        + "                 AND TAB_PROFILI_GIORN_STD_PERC.data <= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ1                                                                                                                                                     "\
        + "             GROUP BY TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr "
        df = sqlCtx.sql(query0)
        #print df.schema
        #df.show()
        if (df.count() == 0 ):
            print ("La tabella TAB_PROFILI_GIORN_STD_PERC e' vuota")
            return
        
        #
        query = "WITH                                                                                                                                                                                                                   "\
        + "        TDenominatore as (                                                                                                                                                                                                   "\
        + "            SELECT                                                                                                                                                                                                           "\
        + "                 TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr,                                                                                                                                                                       "\
        + "                 SUM(TAB_PROFILI_GIORN_STD_PERC.pprofk) AS Denominatore                                                                                                                                                   "\
        + "             FROM                                                                                                                                                                                                             "\
        + "             " + self.database + ".TAB_PROFILI_GIORN_STD_PERC," + self.database + ".TAB_PDR_SEGMENTO_DELTA_MIS_MM                                                                                                                                        "\
        + "             WHERE                                                                                                                                                                                                           "\
        + "                 TAB_PROFILI_GIORN_STD_PERC.data >= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ                                                                                                                                    "\
        + "                 AND TAB_PROFILI_GIORN_STD_PERC.data <= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ1                                                                                                                               "\
        + "                 AND TAB_PROFILI_GIORN_STD_PERC.wkr <> cast(1 as double)                                                                                                                                                     "\
        + "             GROUP BY TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr                                                                                                                                                                        "\
        + "        )                                                                                                                                                                                                                    "\
        + "                                                                                                                                                                                                                             "\
        + "    SELECT DISTINCT                                                                                                                                                                                                          "\
        + "        TDenominatore.pdr,                                                                                                                                                                                                   "\
        + "        (DELTA_MIS / TDenominatore.Denominatore) AS CA,                                                                                                                                                                      "\
        + "        TDenominatore.Denominatore as SUMPProfK,                                                                                                                                                                             "\
        + "        TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ,                                                                                                                                                                               "\
        + "        concat(YEAR(TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ),'-', YEAR(TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ1)) as ANNO_TERMICO    "\
        + "    FROM " + self.database + ".TAB_PDR_SEGMENTO_DELTA_MIS_MM                                                                                                                                                                 "\
        + "    INNER JOIN TDenominatore on TDenominatore.pdr = TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr                                                                                                                                        "\
        + "    where TDenominatore.Denominatore <> 0 and DELTA_MIS is not NULL             "        
        #schema - TAB_PDR_SEGMENTO_CONSUMO_MM
        
	    # PDR, Denominatore, CA
        TAB_PDR_SEGMENTO_CONSUMO_MM  = sqlCtx.sql(query)
	    #TAB_PDR_SEGMENTO_CONSUMO_MM.show()

        #Scrittura della tabella di Output
        TAB_PDR_SEGMENTO_CONSUMO_MM.write.parquet(self.nameTableHive, 'append')
        sqlCtx.sql(self.cmdTableRefresh)
        
        pass
