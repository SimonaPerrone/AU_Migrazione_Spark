

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job22:
    """
    Job di calcolo del CAPdR per i punti MM e YY (mensile e altro).      
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job22"]["nameTableHive"]
        self.cmdTableRefresh = conf["job22"]["cmdTableRefresh"]
        self.table = conf["job22"]["table"]
        self.database = conf["job22"]["database"]

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
        - i dati identificati al punto precedente  ( PDR - DATA_Dz - DATA_Dz+1 -   CAz - ?Prof,kz ) 
        - la tabella TAB_PDR_SEGMENTO_PDR_MIS_MM

        effettui il calcolo completo del CAPdR a partire dalla seguente formula:

        CAPdR =?_z?Z(CAz*min??(1; 1/(?Prof,Nkz))*?Prof,Nka? )   

        Tale Job deve andare a scrivere su una tabella TAB_CONSUMO_ANNUO secondo le seguenti informazioni:

        PDR – CAPdR – ?Prof,kz - ?Prof,Nk,DZ -  ?Prof,Nk,Az – TRATTAMENTO

        """

        #self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)
        
        #TAB_PDR_SEGMENTO_CONSUMO_MM => PDR, Denominatore, CA, TRATTAMENTO
        #TAB_PDR_SEGMENTO_PDR_MIS_MM => PDR - DATA_Dz - DATA_ Dz+1 – ?ProfNkDZ - ?ProfNkAz 

        query = "SELECT TAB_PDR_SEGMENTO_CONSUMO_MM.pdr,                                                                                            "\
        + "        SUM( CA * (CASE WHEN (1/PProfNkDZ) < 1 THEN (1/PProfNkDZ) ELSE 1 END)  * PProfNkAz) as CAPdR,                                    "\
        + "        PProfNkDZ, PProfNkAz, rcugas_massivo.t_trattamento as trattamento		                                                    "\
        + "    FROM                                                                                                                                 "\
        + "        " + self.database + ".TAB_PDR_SEGMENTO_CONSUMO_MM                                                                                "\
        + "    INNER JOIN " + self.database + ".TAB_PDR_SEGMENTO_PDR_MIS_MM on TAB_PDR_SEGMENTO_PDR_MIS_MM.pdr = TAB_PDR_SEGMENTO_CONSUMO_MM.pdr    "\
        + "    INNER JOIN rcugas.rcugas_massivo on rcugas_massivo.t_codice_pdr = TAB_PDR_SEGMENTO_CONSUMO_MM.pdr                                    "\
	+ "    where PProfNkDZ is not NULL 										    "\
        + "    GROUP BY TAB_PDR_SEGMENTO_CONSUMO_MM.pdr, PProfNkDZ, PProfNkAz, rcugas_massivo.t_trattamento                                         "
        
        TAB_CONSUMO_ANNUO = sqlCtx.sql(query)
        TAB_CONSUMO_ANNUO.write.parquet(self.nameTableHive, 'append')
        sqlCtx.sql(self.cmdTableRefresh)

        pass
