

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job14:
    """
    Job Calcolo Profilo Standard per ogni giorno dell'anno termico.
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job14"]["nameTableHive"]
        self.table = conf["job14"]["table"]
        self.cmdTableRefresh = conf["job14"]["cmdTableRefresh"]

        self.TAB_PARAMETRI_CARATTERISTICI_PROF_PREL = conf["job13"]["nameTableHive"]
        self.TAB_PARAMETRI_PERC_ANNUI_PREL_STD = conf["job14"]["TAB_PARAMETRI_PERC_ANNUI_PREL_STD"]
        self.TAB_FATT_CLIM_WK = conf["job14"]["TAB_FATT_CLIM_WK"]
        self.database = conf["job14"]["database"]
    
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

    def calcola(self, item, sqlCtx):
        #TODO utilizzare una lista
        list_items = ['C1','c1']
        c1 = "" + item[1] + str(int(item[2])) if item[0] in list_items  else ""
        t1 = "" + str(int(item[2])) 

        #print (c1, t1)
        if (c1 != ""):
            query = " select  tab_fatt_clim_wk.wkr, cast(unix_timestamp(tab_fatt_clim_wk.data , 'dd/MM/yyyy') as TIMESTAMP) as data,      "\
            + "     cast(( tab_fatt_clim_wk.wkr * PARAMS.b1 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.c1_"+ c1 +"  +       "\
            + "         PARAMS.b2 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.c2 +        "\
            + "         PARAMS.b3 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.t1_" + str(int(item[2])) + " +      "\
            + "         PARAMS.b4 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.c4      "\
            + "     ) as double) as PProfk,  PARAMS.prof      "\
            + " from " + self.database + ".tab_fatt_clim_wk, " + self.database + ".TAB_PARAMETRI_PERC_ANNUI_PREL_STD ,        "\
            + "     (Select * from "+self.database + ".TAB_PARAMETRI_CARATTERISTICI_PROF_PREL where TAB_PARAMETRI_CARATTERISTICI_PROF_PREL.prof = 'C1" + c1 + "') as PARAMS       "\
            + " WHERE TAB_PARAMETRI_PERC_ANNUI_PREL_STD.giorno_riferimento = tab_fatt_clim_wk.data        "




            #print query
            df = sqlCtx.sql(query)
            #df.show()
            df.write.parquet(self.nameTableHive, 'append')

            query = " select 1 as Wkr, cast(unix_timestamp(tab_fatt_clim_wk.data , 'dd/MM/yyyy') as TIMESTAMP) as data ,       "\
            + "     ( 1 * PARAMS.b1 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.c1_"+ c1 +"  +       "\
            + "         PARAMS.b2 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.c2 +        "\
            + "         PARAMS.b3 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.t1_" + str(int(item[2])) + " +      "\
            + "         PARAMS.b4 * TAB_PARAMETRI_PERC_ANNUI_PREL_STD.c4      "\
            + "     ) as PProfk, PARAMS.prof      "\
            + " from " + self.database + ".tab_fatt_clim_wk, " + self.database + ".TAB_PARAMETRI_PERC_ANNUI_PREL_STD ,        "\
            + "     (Select * from " + self.database + ".TAB_PARAMETRI_CARATTERISTICI_PROF_PREL where TAB_PARAMETRI_CARATTERISTICI_PROF_PREL.prof = 'C1" + c1 + "') as PARAMS       "\
            + " WHERE TAB_PARAMETRI_PERC_ANNUI_PREL_STD.giorno_riferimento = tab_fatt_clim_wk.data        "

            #print query
            df = sqlCtx.sql(query)

            #from_unixtime(unix_timestamp(TAB_PROFILI_GIORN_STD_PERC.data , 'dd/MM/yyyy'))
            #df.show()
            df.write.parquet(self.nameTableHive, 'append')
            print ("Scrittura tabella: ", self.nameTableHive)
            sqlCtx.sql(self.cmdTableRefresh)

            # query = " CREATE TABLE  " + self.database + "."+ self.table + "_tmp"\
            # + " ROW FORMAT DELIMITED"\
            # + " FIELDS TERMINATED BY '\t'"\
            # + " STORED AS PARQUET"\
            # + " LOCATION '" + self.nameTableHive + "_tmp' as "\
            # + " select from_unixtime(unix_timestamp(data , 'dd/MM/yyyy')) as data,"\
            # + "         wkr   ,"\
            # + "         PProfk,"\
            # + "         prof "\
            # + " from au_test."+ self.table + ";"

        
    def run(self, sc, sqlCtx, params):
        #self.sqlCtx1 = sqlCtx
        """
        Si richiede un job per il calcolo del Profilo Standard per ogni giorno dell'anno termico per 
        ciascun codice Profilo STD e per ogni zona climatica (A,B,C,D,E,F) attraverso la seguente formula:

        Per il calcolo della formula dovranno essere utilizzati i valori presenti nella tabella 
        "TAB_PARAMETRI_PERC_ANNUI_PREL_STD" e "TAB_FATT_CLIM" caricati al punto 4 e nella tabella 
        dei Profili Standard e parametri caratteristici (TAB_PARAMETRI_CARATTERISTICI_PROF_PREL) 
        descritta al punto precedente.
        I risultati calcolati devono essere inseriti in un archivio dei profili giornalieri dell'anno 
        (TAB_PROFILI_GIORN_STD_PERC) necessari ai calcoli descritti nei requisiti successivi.
        Al fine di eseguire gli opportuni confronti, lo stesso job deve poter essere configurato per 
        eseguire il calcolo considerando il Wkr pari a 1 in tutti i casi. 
        La tabella TAB_PROFILI_GIORN_STD_PERC deve essere strutturata per contenere, per ogni anno, 
        entrambi i risultati (con WKr diverso per giorno/zone e WKr =1).

        Schema: 
        TAB_PROFILI_GIORN_STD_PERC => DATA, Wk, PProfk
        """

        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)

        query = "select cat_uso, zona_clim, class_prelievo from " + self.database + ".TAB_PARAMETRI_CARATTERISTICI_PROF_PREL"
        df = sqlCtx.sql(query)
        
        index = 0
        for row in df.rdd.collect():
            if (index != 0 ):
                #print ("***", row)
                self.calcola(row, sqlCtx)

            index+=1

   
