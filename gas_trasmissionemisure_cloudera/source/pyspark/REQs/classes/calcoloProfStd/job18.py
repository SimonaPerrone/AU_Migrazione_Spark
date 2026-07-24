

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job18:
    """
    Job di Assegnazione della CAT_USO e CLASSE_PREL.
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHive = conf["job18"]["nameTableHive"]
        self.cmdTableRefresh = conf["job18"]["cmdTableRefresh"]
        self.table = conf["job18"]["table"]
        self.database = conf["job18"]["database"]

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
        Si richiede un job che per ciascun PdR della tabella (TAB_RCUGAS_NEW_SETTLE_GAS_PDR), 
        in relazione alle comunicazioni delle Dichiarazioni Sostitutive e del SAG identifichi  
        le informazioni da utilizzare per assegnazione finale della Categoria d’Uso (CAT_USO), 
        Classe di Prelievo (CLASSE_PREL), Profilo di Prelievo(COD_PROF_STD).

        Dichiarazioni Sostitutive => tabella gas_TDS


        -	Se e' presente una dichiarazione Sostitutiva trasmessa dal UdD (req. 1), considero 
          validi i nuovi valori dei campi CAT_USO, CLASSE_PREL,  COD_PROF_STD presenti nella 
          comunicazione.

        -	Se per il PDR e' presente una trasmissione SAG (req.9) e il campo COD_PROF_STD del 
          SAG e' popolato, considero valido quest’ultimo valore. I campi CAT_USO e CLASSE_PREL 
          devono essere valorizzati con i relativi valori in linea con il COD_PROF_STD.

        -	Se per il PDR e' presente una trasmissione SAG (req.9) e il campo trattamento del 
          SAG e' G, si assegna classe di prelievo 1 o quella maggiormente rappresentata dalle 
          misure secondo le seguenti logiche (vedi tabella)

        -	Se per il PDR e' presente una trasmissione SAG (req.9) e sia il COD_PROF_STD e 
          trattamento sono vuoti vengono considerati i dati ricalcolati sul RCUGAS al punto 16 
          (TAB_RCUGAS_NEW_SETTLE_GAS_PDR) 
        """

        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)
 
        query = "select                                                                                                                                                                         "\
        +"       TAB_RCUGAS_NEW_SETTLE_GAS_PDR.anno_rif,                                                                                                                                        "\
        +"       TAB_RCUGAS_NEW_SETTLE_GAS_PDR.pdr as pdr,                                                                                                                                      "\
        +"       (case                                                                                                                                                                          "\
        +"           when gas_tds.cod_pdr IS NOT NULL then gas_tds.cat_uso                                                                                                                      "\
        +"           when gas_sag.cod_pdr IS NOT NULL and gas_sag.cod_prof_prel_std is not null then substr(gas_sag.cod_prof_prel_std,0,2)                                                      "\
        +"           else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cat_uso                                                      									"\
        +"       end) as cat_uso,                                                                                                                                                               "\
        +"       (case                                                                                                                                                                          "\
        +"           when gas_tds.cod_pdr IS NOT NULL then gas_tds.classe_prelievo                                                                                                              "\
        +"           when gas_sag.cod_pdr IS NOT NULL and gas_sag.cod_prof_prel_std is not null and TAB_RCUGAS_NEW_SETTLE_GAS_PDR.trattamento <> 'G' then substr(gas_sag.cod_prof_prel_std,4,1) "\
        +"           when gas_sag.cod_pdr IS NOT NULL  and TAB_RCUGAS_NEW_SETTLE_GAS_PDR.trattamento = 'G' then                                                                                 "\
        +"           (                                                                                                                                                                          "\
        +"               case                                                                                                                                                                   "\
        +"                   when gas_sag.cons_ann < 500 and TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cat_uso ='C2' then 1                                                                                 "\
        +"                   when gas_sag.cons_ann >= 500 and gas_sag.cons_ann<=5000 and TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cat_uso ='C3' then 1                                                     "\
        +"                   when gas_sag.cons_ann > 5000 and TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cat_uso ='C1' then 1                                                                                "\
        +"                   when TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cat_uso ='T1' OR TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cat_uso ='T2' then 3                                                             "\
        +"               end                                                                                                                                                                    "\
        +"           )                                                                                                                                                                          "\
        +"           else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.classe_prel                                                                                                                             "\
        +"       end) as classe_prel,                                                                                                                                                           "\
        +"       ( case                                                                                                                                                                         "\
        +"           when gas_sag.cod_pdr IS NOT NULL and gas_sag.cod_prof_prel_std is not null and TAB_RCUGAS_NEW_SETTLE_GAS_PDR.trattamento <> 'G' then substr(gas_sag.cod_prof_prel_std,3,1) "\
        +"                                                                                                                                                                                      "\
        +"           else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.zona_clim                                                                                                                               "\
        +"       end) as zona_clim,                                                                                                                                                             "\
        +"       ( case                                                                                                                                                                         "\
        +"           when gas_sag.cod_pdr IS NOT NULL and gas_sag.cod_prof_prel_std is not null and TAB_RCUGAS_NEW_SETTLE_GAS_PDR.trattamento <> 'G' then gas_sag.cod_prof_prel_std             "\
        +"           else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cod_prof_std                                                                                                                                            "\
        +"       end) as cod_prof_prel_std,                                                                                                                                                     "\
        +"       TAB_RCUGAS_NEW_SETTLE_GAS_PDR.trattamento,                                                                                                                                     "\
        +"       TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cons_annuo,                                                                                                                                      "\
        +"       TAB_RCUGAS_NEW_SETTLE_GAS_PDR.regione_clim,                                                                                                                                    "\
        +"        (case                                                                                                                                                                         "\
        +"            when gas_tds.cod_pdr IS NOT NULL then 'TDS'                                                                                                                               "\
        +"            when gas_sag.cod_pdr IS NOT NULL and gas_sag.cod_prof_prel_std is not null then 'SAG'                                                                                     "\
        +"            else ''                                                                                                                                                                   "\
        +"        end) as from_table                                                                                                                                                            "\
        +"   from " + self.database + ".TAB_RCUGAS_NEW_SETTLE_GAS_PDR                                                                                                                           "\
        +"   left join " + self.database + ".gas_tds on gas_tds.cod_pdr = TAB_RCUGAS_NEW_SETTLE_GAS_PDR.pdr                                                                                     "\
        +"   left join " + self.database + ".gas_sag on gas_sag.cod_pdr = TAB_RCUGAS_NEW_SETTLE_GAS_PDR.pdr                                                                                     "

        TAB_SETTLE_GAS_PROF_PDR = sqlCtx.sql(query)
        #TAB_SETTLE_GAS_PROF_PDR.show()
        
        TAB_SETTLE_GAS_PROF_PDR.write.parquet(self.nameTableHive, 'append')
        print self.nameTableHive
        sqlCtx.sql(self.cmdTableRefresh)
        pass
 