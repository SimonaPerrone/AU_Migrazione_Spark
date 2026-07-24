

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job23:
    """
    Job di calcolo dati SAG nuovo anno termico
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job23"]["nameTableHive"]
        self.cmdTableRefresh = conf["job23"]["cmdTableRefresh"]
        self.table = conf["job23"]["table"]
        self.database = conf["job23"]["database"]

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
        Si richiede un job che incrociando:
        -	i dati della tabella TAB_CONSUMO_ANNUO 
        -	i dati della tabella TAB_SETTLE_GAS_PROF_PDR 

        Popola la tabella finale TAB_DATI_SETTLE_SAG per avere:
        -	tutti i dati dei PDR su cui è stato calcolato il nuovo consumo, presi da TAB_CONSUMO_ANNUO, 
            con le informazioni sul COD_PROF estratto dalla TAB_SETTLE_GAS_PROF_PDR.
        -	Tutti i dati dei PDR presenti su TAB_SETTLE_GAS_PROF_PDR, e non presenti in  TAB_CONSUMO_ANNUO.

        Nella tabella deve essere memorizzato quale dei due casi ha portato al popolamento della tabella 
        TAB_DATI_SETTLE_SAG.

        """

        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)
     
        #TAB_PDR_SEGMENTO_CONSUMO_MM => PDR, Denominatore, CA, TRATTAMENTO
        #TAB_PDR_SEGMENTO_PDR_MIS_MM => PDR - DATA_Dz - DATA_ Dz+1 – ?ProfNkDZ - ?ProfNkAz 

        query = "    select                                                                                         "\
        + "        TAB_CONSUMO_ANNUO.pdr as pdr,                                                                    "\
        + "        TAB_CONSUMO_ANNUO.capdr as capdr,                                                                "\
        + "        TAB_SETTLE_GAS_PROF_PDR.cod_prof_prel_std,                                                       "\
        + "        TAB_CONSUMO_ANNUO.trattamento,                                                                   "\
        + "        'TAB_CONSUMO_ANNUO' as from_table                                                                "\
        + "    from " + self.database + ".TAB_CONSUMO_ANNUO, " + self.database + ".TAB_SETTLE_GAS_PROF_PDR                                      "\
        + "    where TAB_SETTLE_GAS_PROF_PDR.pdr = TAB_CONSUMO_ANNUO.pdr                                            "\
        + "          AND TAB_SETTLE_GAS_PROF_PDR.cod_prof_prel_std is not null                                      "\
        + "    UNION ALL                                                                                            "\
        + "    select                                                                                               "\
        + "        TAB_SETTLE_GAS_PROF_PDR.pdr as pdr,                                                              "\
        + "        TAB_SETTLE_GAS_PROF_PDR.cons_annuo as capdr,                                                     "\
        + "        TAB_SETTLE_GAS_PROF_PDR.cod_prof_prel_std,                                                       "\
        + "        TAB_SETTLE_GAS_PROF_PDR.trattamento,                                                             "\
        + "        'TAB_SETTLE_GAS_PROF_PDR' as from_table                                                          "\
        + "    from " + self.database + ".TAB_SETTLE_GAS_PROF_PDR                                                                 "\
        + "    left join " + self.database + ".TAB_CONSUMO_ANNUO on TAB_CONSUMO_ANNUO.pdr = TAB_SETTLE_GAS_PROF_PDR.pdr           "\
        + "    WHERE TAB_CONSUMO_ANNUO.pdr IS NULL                                                                  "\
        + "          AND TAB_SETTLE_GAS_PROF_PDR.cod_prof_prel_std is not null                                      "
        
        TAB_DATI_SETTLE_SAG = sqlCtx.sql(query)
        #TAB_DATI_SETTLE_SAG.show()
        TAB_DATI_SETTLE_SAG.write.parquet(self.nameTableHive, 'append')
        sqlCtx.sql(self.cmdTableRefresh)

        pass
