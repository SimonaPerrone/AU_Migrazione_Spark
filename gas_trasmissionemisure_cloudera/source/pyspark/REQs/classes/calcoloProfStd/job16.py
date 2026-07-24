

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *


class Job16:
    """
    Job di Caricamento dei dati Settlement GAS dal RCUGAS all’ambiente HIVE.
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job16"]["nameTableHive"]
        self.table = conf["job16"]["table"]
        self.cmdTableRefresh = conf["job16"]["cmdTableRefresh"]
        self.database = conf["job16"]["database"]

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
        Si richiede una procedura che crei una tabella HIVE (TAB_RCUGAS_SETTLE_GAS_PDR) popolando la stessa a partire 
        dai dati del RCUGAS incrociando i dati ISTAT con la tabella TAB_COMUNE_ZONA_CLIM per estrarre 
        la zona climatica (ZONA_CLIM). Tale tabella dovrà avere le seguenti informazioni:
        ANNO_RIF - PdR - CAT_USO – CLASSE_PREL – ZONA_CLIM - COD_PROF_STD - TRATTAMENTO – CONS_ANNUO – REGIONE_CLIM

        ANNO_RIF        --> rcugas_pdr       
        PdR             --> rcugas_pdr   
        CAT_USO         --> RCUGAS_MASSIVO
        CLASSE_PREL     --> RCUGAS_MASSIVO
        ZONA_CLIM       --> TAB_COMUNE_ZONA_CLIM
        COD_PROF_STD    --> RCUGAS_MASSIVO.T_COD_PROFILO 
        TRATTAMENTO     --> RCUGAS_MASSIVO
        CONS_ANNUO      --> RCUGAS_MASSIVO.n_prelievo_annuo 
        REGIONE_CLIM    --> RCUGAS_MASSIVO.REGIONE_CLIM ? da sistemare. Utilizzare id o codice rcugas_massivo

        RCUGAS_MASSIVO è utilizzato in join con TAB_COMUNE_ZONA_CLIM 
        """

        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)

        query = "select YEAR(rcugas_pdr.d_data_rif) as ANNO_RIF, rcugas_pdr.t_codice_pdr as PDR,                                                                    "\
            + "   RCUGAS_MASSIVO.t_cod_cat_uso as CAT_USO,                                                                                                          "\
            + "   RCUGAS_MASSIVO.t_cod_classe_prelievo as CLASSE_PREL,                                                                                              "\
            + "   TAB_COMUNE_ZONA_CLIM.z as ZONA_CLIM,                                                                                                              "\
            + "   RCUGAS_MASSIVO.T_COD_PROFILO as COD_PROF_STD,                                                                                                     "\
            + "   RCUGAS_MASSIVO.t_trattamento as TRATTAMENTO,                                                                                                      "\
            + "   cast (RCUGAS_MASSIVO.n_prelievo_annuo as double) as CONS_ANNUO,                                                                                   "\
            + "   RCUGAS_MASSIVO.n_prelievo_annuo as REGIONE_CLIM                                                                                                   "\
            + " FROM                                                                                                                                                "\
            + "   rcugas.rcugas_pdr                                                                                                                                 "\
            + " INNER JOIN                                                                                                                                          "\
            + "   rcugas.RCUGAS_MASSIVO on RCUGAS_MASSIVO.n_id_pdr = rcugas_pdr.n_id_pdr                                                                            "\
            + " INNER JOIN                                                                                                                                          "\
            + " " + self.database + ".TAB_COMUNE_ZONA_CLIM on upper (TAB_COMUNE_ZONA_CLIM.comune) = upper(RCUGAS_MASSIVO.t_comune_pdr)                              "\
            + " where RCUGAS_MASSIVO.t_cod_cat_uso IS NOT NULL and RCUGAS_MASSIVO.t_cod_classe_prelievo IS NOT NULL and  RCUGAS_MASSIVO.t_trattamento IS NOT NULL   "\
            + "  and RCUGAS_MASSIVO.t_cod_cat_uso <> 'null' and RCUGAS_MASSIVO.t_cod_classe_prelievo <> 'null' and  RCUGAS_MASSIVO.t_trattamento <> 'null'          "\

        #print query
        df = sqlCtx.sql(query)
        df.write.parquet(self.nameTableHive, 'append')
        print ("Scrittura tabella: ", self.nameTableHive)
        
        sqlCtx.sql(self.cmdTableRefresh)
        