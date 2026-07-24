

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job20:
    """
    Job estrazioni dati dal File SAG per i PdR GG (giornaliero).  
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job20"]["nameTableHive"]
        self.cmdTableRefresh = conf["job20"]["cmdTableRefresh"]
        self.table = conf["job20"]["table"]
        self.database = conf["job20"]["database"]


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
        Si richiede un job che a partire dal file SAG popoli la tabella 
            TAB_CONSUMO_ANNUO con i PDR aventi TRATTAMENTO= G.

            Le informazioni della tabella saranno compilate come di seguito:
            -	PDR: estratto dal file SAG
            -	CAPdR: estratto dal campo Prel_annuo_prev
            -	?Prof,kz - ?Prof,Nk,DZ -  ?Prof,Nk,Az : vuoti
            -	TRATTAMENTO: ‘G’ estratto dal RCUGAS
        """

        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)

        query = " select                                                                                    "\
        + "        gas_sag.cod_pdr as PDR,                                                                  "\
        + "        gas_sag.cons_ann as CAPdR,                                                               "\
        + "        rcugas_massivo.t_trattamento as trattamento,                                             "\
        + "        cast(NULL as double) as PProfkz,                                                         "\
        + "        cast(NULL as double) as PProfNkDZ,                                                       "\
        + "        cast(NULL as double) as PProfNkAz                                                        "\
        + "    from " + self.database + ".gas_sag                                                                         "\
        + "    inner join rcugas.rcugas_massivo on rcugas_massivo.t_codice_pdr = gas_sag.cod_pdr            "\
        + "    where rcugas_massivo.t_trattamento = 'G'                                                     "
        
        #print query

        TAB_CONSUMO_ANNUO = sqlCtx.sql(query)
        #TAB_CONSUMO_ANNUO.show()

        TAB_CONSUMO_ANNUO.write.parquet(self.nameTableHive, 'append')
        sqlCtx.sql(self.cmdTableRefresh)

        pass
