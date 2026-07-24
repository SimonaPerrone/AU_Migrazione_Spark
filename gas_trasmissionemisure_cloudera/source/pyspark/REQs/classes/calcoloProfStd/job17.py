

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job17:
    """
    Job di Assegnazione della CAT_USO e CLASSE_PREL.
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job17"]["nameTableHive"]
        self.cmdTableRefresh = conf["job17"]["cmdTableRefresh"]
        self.table = conf["job17"]["table"]
        self.database = conf["job17"]["database"]

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
        | USO DEL GAS	| PRELIEVO ANNUO PRESUNTO | CATEGORIA D’USO ASSEGNATA | CLASSE DI PRELIEVO |
        | Civile	    | inferiore a 500 Smc	  | cottura cibi e/o produzione di acqua calda sanitaria (codice C2)	| 7 giorni/settimana (cod. 1)
        | Civile	    | compreso tra 500 e 5.000 Smc	| riscaldamento + cottura cibi e/o produzione di acqua calda sanitaria (codice C3) |	7 giorni/settimana (cod. 1)
        | Civile	    | superiore a 5.000 Smc	  | riscaldamento (codice C1)	| 7 giorni/settimana (cod. 1)
        | Tecnologico	| nessuna limitazione	  | tecnologico + riscaldamento (codice T2) | 5 giorni/settimana (cod. 3)

        TAB_RCUGAS_NEW_SETTLE_GAS_PDR
        """

        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)

        query = " select anno_rif,                                                                          "\
          + "        pdr,                                                                                   "\
          + "        (case                                                                                  "\
          + "                when array_contains(array('C1','C2','C3','C4','C5'), cat_uso) then (           "\
          + "                    case                                                                       "\
          + "                        when cons_annuo < 500 then 'C2'                                        "\
          + "                        when cons_annuo >= 500 and cons_annuo <= 5000 then 'C3'                "\
          + "                        else 'C1'                                                              "\
          + "                    end                                                                        "\
          + "                )                                                                              "\
          + "                when array_contains(array('T1','T2'), cat_uso) then 'T2'                       "\
          + "                else null                                                                      "\
          + "            end                                                                                "\
          + "        ) as cat_uso,                                                                          "\
          + "        (                                                                                      "\
          + "            case                                                                               "\
          + "                when array_contains(array('C1','C2','C3','C4','C5'), cat_uso) then 1           "\
          + "                when array_contains(array('T1','T2'), cat_uso) then 3                          "\
          + "            end                                                                                "\
          + "        ) as classe_prel,                                                                      "\
          + "        zona_clim,                                                                             "\
          + "        cod_prof_std,                                                                          "\
          + "        trattamento,                                                                           "\
          + "        cons_annuo,                                                                            "\
          + "        regione_clim                                                                           "\
          + " from " + self.database + ".TAB_RCUGAS_SETTLE_GAS_PDR                                                        "

        TAB_RCUGAS_NEW_SETTLE_GAS_PDR = sqlCtx.sql(query)
        #TAB_RCUGAS_NEW_SETTLE_GAS_PDR.show()

        TAB_RCUGAS_NEW_SETTLE_GAS_PDR.write.parquet(self.nameTableHive, 'append')
        print ("Scrittura tabella: ", self.nameTableHive)
        sqlCtx.sql(self.cmdTableRefresh)

        pass
