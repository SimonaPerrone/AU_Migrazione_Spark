

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job24:
    """
    Job di coerenza tra CA_PdR e Codice Prof StD
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job24"]["nameTableHive"]
        self.cmdTableRefresh = conf["job24"]["cmdTableRefresh"]
        self.table = conf["job24"]["table"]
        self.database = conf["job24"]["database"]

        self.tableExternal = conf["job24"]["tableExternal"]
        self.pathTabellaHDFS = conf["job24"]["pathTabellaHDFS"]
        self.cmdTableRefresh2 = conf["job24"]["cmdTableRefresh2"]

        
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

        enable_check_externaltable = False
        if (params):
            enable_check_externaltable = params[0]

        print ("Enable check from external Table: ", enable_check_externaltable)

        """
        Si richiede l’implementazione di una funzionalità che successivamente al calcolo del CA per ciascun PdR 
        effettui il controllo di coerenza tra valore calcolato e Categoria D’uso profilato secondo la regola
        descritta al punto 17.
        Qualora ci fosse un’incoerenza deve essere previsto il ricalcolo del CA per uno o un set di PdR in maniera 
        reiterata.

        """
      
        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)
      
        #TAB_PDR_SEGMENTO_CONSUMO_MM => PDR, Denominatore, CA, TRATTAMENTO
        #TAB_PDR_SEGMENTO_PDR_MIS_MM => PDR - DATA_Dz - DATA_ Dz+1 – ?ProfNkDZ - ?ProfNkAz 

        if (enable_check_externaltable):
            # Creazione tabella esterna (per punto 25.2)
            # dropTable = "DROP TABLE " + self.tableExternal
            # sqlCtx.sql(dropTable)
            externalTable =   "CREATE EXTERNAL TABLE IF NOT EXISTS " + self.tableExternal +                        ""\
                            + "   (                                                                                 "\
                            + "   pdr                   String,                                                     "\
                            + "   capdr                 double,                                                     "\
                            + "   cod_prof_prel_std     String,                                                     "\
                            + "   trattamento           String,                                                     "\
                            + "   from_table            String                                                      "\
                            + "   ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'                                   "\
                            + "   STORED AS TEXTFILE location '" + self.pathTabellaHDFS +                         "'"\
                            + "   TBLPROPERTIES(\"skip.header.line.count\"=\"1\")"
            sqlCtx.sql(externalTable)
            sqlCtx.sql(self.cmdTableRefresh2)

            query = " INSERT INTO TABLE " + self.database + ".TAB_DATI_SETTLE_SAG   "\
                +   " SELECT  *                                                     "\
                +   " FROM " + self.tableExternal     
            
            result = sqlCtx.sql(query)
            sqlCtx.sql(self.cmdTableRefresh2)

            #TAB_DATI_SETTLE_SAG.show()
            #TAB_DATI_ANOMALIE_CA_CATUSO.write.parquet(self.nameTableHive, 'append')


        query = "select                                                     "\
            + "        pdr,                                                 "\
            + "        capdr,                                               "\
            + "        cod_prof_prel_std,                                   "\
            + "        (case                                                "\
            + "            when capdr < 500 then 'C2'                       "\
            + "            when capdr >= 500 and capdr <= 5000 then 'C3'    "\
            + "            else 'C1'                                        "\
            + "        end) as RES_CAT_USO                                  "\
            + "    from " + self.database + ".TAB_DATI_SETTLE_SAG           "\
            + "    where                                                    "\
            + "        (case                                                "\
            + "            when capdr < 500 then 'C2'                       "\
            + "            when capdr >= 500 and capdr <= 5000 then 'C3'    "\
            + "            else 'C1'                                        "\
            + "        end) <> substr(cod_prof_prel_std,0,2)                "
        
        TAB_DATI_ANOMALIE_CA_CATUSO = sqlCtx.sql(query)
        #TAB_DATI_SETTLE_SAG.show()
        TAB_DATI_ANOMALIE_CA_CATUSO.write.parquet(self.nameTableHive, 'append')

        sqlCtx.sql(self.cmdTableRefresh)

        pass
