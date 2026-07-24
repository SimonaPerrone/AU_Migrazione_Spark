import xml.etree.ElementTree as ET
import constants

from pyspark import SparkContext, SparkConf, SQLContext
from functions.time import UtilTime 
import datetime
import hashlib
import os
import re

from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType

class FlussoTMV:

    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        pattern ="^\\w{11,16}_\\w{11,16}_\\d{6}_(T|t)(M|m)(V|v)(\\d{4}|.\\d{4}|)_\\d*(_\\d{1,}|).\\w{3}$"
        result = bool(re.match(pattern, filename))
	#print ("isValid Result", result)
	return result

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []   

	codice_servizio, cod_flusso, pivaUtente, piva_distr, cod_prat_attivazione, \
        cod_pdr, matr_mis, data_att_contr, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, segn_mis_sost, \
        tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff, \
        segn_mis_eff, segn_conv_eff, note = self.reset()

        result.append( (codice_servizio, cod_flusso, dataElaborazione, pivaUtente, piva_distr, cod_prat_attivazione,
            cod_pdr, matr_mis, data_att_contr, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, segn_mis_sost,
            tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff,
            segn_mis_eff, segn_conv_eff, note, anno, mese, local_file) )

        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = None
        #print ("==============> codice_servizio:", codice_servizio, constants.CODSERVIZIO_TMV, codice_flusso)
        # Flusso TML
        if codice_servizio == constants.CODSERVIZIO_TMV and codice_flusso == "0350":
            file_xsd = constants.FILE_XSD_TMV_0350

        #print ("file_xsd:", file_xsd)
        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        print("Thread TMV")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        dfTMV = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoTMV)

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        annomese_rif_udf = udf(
            lambda d: str(os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else "20" + os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2])
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

	#data_racc gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        dfTMV = dfTMV.withColumn("t_name_file",  filename_udf(col('local_file')))
        #dfTMV = dfTMV.withColumn("data_att_contr", expr("from_unixtime(unix_timestamp(data_att_contr, 'dd/MM/yyyy'))").cast("timestamp"))
        #dfTMV = dfTMV.withColumn("data_mis_eff", expr("from_unixtime(unix_timestamp(data_mis_eff, 'dd/MM/yyyy'))").cast("date"))
        dfTMV = dfTMV.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfTMV = dfTMV.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))
        dfTMV = dfTMV.withColumn("annomese", annomese_udf(col('data_mis_eff')))
        dfTMV = dfTMV.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

        dataElaborazione = str(datetime.datetime.now().isoformat())
	print ("dataElaborazione:", dataElaborazione)
        dfTMV = dfTMV.withColumn("d_caricamento",  lit(dataElaborazione))

	#dfTMV.show(truncate = False)
        #dfTMV.show(truncate = False)
	#constants.PATHDFS_TMV="/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tmv_p"
        print ("Write: ", constants.PATHDFS_TMV)
        print ("Partition: ", constants.PARTITIONLIST_TMV)
        dfTMV.write.partitionBy(constants.PARTITIONLIST_TMV).parquet(constants.PATHDFS_TMV, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_TMV)
        sc.setLocalProperty("spark.scheduler.pool", None)

    # Ritorna l'elemento
    def getItems(self, root, dataElaborazione, anno, mese, local_file):
        result          = []   
	codice_servizio, cod_flusso, pivaUtente, piva_distr, cod_prat_attivazione, \
        cod_pdr, matr_mis, data_att_contr, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, segn_mis_sost, \
        tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff, \
        segn_mis_eff, segn_conv_eff, note = self.reset()

        #print ("ROOT:" ,root)
        if root == None:
	    result.append( (codice_servizio, cod_flusso, dataElaborazione, pivaUtente, piva_distr, cod_prat_attivazione,
                cod_pdr, matr_mis, data_att_contr, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, segn_mis_sost,
                tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff,
                segn_mis_eff, segn_conv_eff, note, anno, mese, local_file  )  )

            return result
 
        codice_servizio = None if root.get("cod_servizio") is None else root.get("cod_servizio")
        cod_flusso      = None if root.get("cod_flusso") is None else root.get("cod_flusso")

        pivaUtente      = None if root.find("IdentificativiRichiesta/piva_utente") is None else root.find("IdentificativiRichiesta/piva_utente").text
        piva_distr      = None if root.find("IdentificativiRichiesta/piva_distr") is None else root.find("IdentificativiRichiesta/piva_distr").text

        if anno == None or mese == None or piva_distr == None or pivaUtente ==None:                     
	    result.append( (codice_servizio, cod_flusso, dataElaborazione, pivaUtente, piva_distr, cod_prat_attivazione,
                cod_pdr, matr_mis, data_att_contr, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, segn_mis_sost,
                tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff,
                segn_mis_eff, segn_conv_eff, note, "EE", "EE", local_file  )  )
            return result

        cod_prat_attivazione = None if root.find("IdentificativiRichiesta/cod_prat_attivazione") is None else root.find("IdentificativiRichiesta/cod_prat_attivazione").text
        
        cod_pdr           = None if root.find("DatiTecnici/cod_pdr") is None else root.find("DatiTecnici/cod_pdr").text
        matr_mis          = None if root.find("DatiTecnici/matr_mis") is None else root.find("DatiTecnici/matr_mis").text
        data_att_contr    = None if root.find("DatiTecnici/data_att_contr") is None else root.find("DatiTecnici/data_att_contr").text
        vol_annuo_sost    = None if root.find("DatiTecnici/vol_annuo_sost") is None else root.find("DatiTecnici/vol_annuo_sost").text
        classe_gruppo_mis = None if root.find("DatiTecnici/classe_gruppo_mis") is None else root.find("DatiTecnici/classe_gruppo_mis").text
        n_cifre_mis       = None if root.find("DatiTecnici/n_cifre_mis") is None else root.find("DatiTecnici/n_cifre_mis").text


        segn_mis_sost     = None if root.find("DatiLettura/segn_mis_sost") is None else root.find("DatiLettura/segn_mis_sost").text
        tipo_lettura      = None if root.find("DatiLettura/tipo_lettura") is None else root.find("DatiLettura/tipo_lettura").text
        pre_conv          = None if root.find("DatiLettura/pre_conv") is None else root.find("DatiLettura/pre_conv").text
        gruppo_mis_int    = None if root.find("DatiLettura/gruppo_mis_int") is None else root.find("DatiLettura/gruppo_mis_int").text
        coeff_corr        = None if root.find("DatiLettura/coeff_corr") is None else root.find("DatiLettura/coeff_corr").text
        matr_conv         = None if root.find("DatiLettura/matr_conv") is None else root.find("DatiLettura/matr_conv").text
        n_cifre_conv	  = None if root.find("DatiLettura/n_cifre_conv") is None else root.find("DatiLettura/n_cifre_conv").text
        segn_conv	  = None if root.find("DatiLettura/segn_conv") is None else root.find("DatiLettura/segn_conv").text

        data_mis_eff      = None if root.find("DatiLettura/data_mis_eff") is None else root.find("DatiLettura/data_mis_eff").text 
        segn_mis_eff      = None if root.find("DatiLettura/segn_mis_eff") is None else root.find("DatiLettura/segn_mis_eff").text
        segn_conv_eff	  = None if root.find("DatiLettura/segn_conv_eff") is None else root.find("DatiLettura/segn_conv_eff").text


	note		  = None if root.find("note") is None else root.find("note").text
	#print("NOTE:",root.find("note").text)
        note 		  = None if note is None else constants.get_text(note)

        #print("qui")
	result.append( (codice_servizio, cod_flusso, dataElaborazione, pivaUtente, piva_distr, cod_prat_attivazione,
                        cod_pdr, matr_mis, data_att_contr, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, segn_mis_sost,
                        tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff,
                        segn_mis_eff, segn_conv_eff, note, anno, mese, local_file  )  )

        return result



    def reset(self):
        codice_servizio = None
        cod_flusso  = None
        pivaUtente  = None
        piva_distr  = None
        cod_prat_attivazione    = None
        cod_pdr = None
        matr_mis    = None
        data_att_contr  = None
        vol_annuo_sost  = None
        classe_gruppo_mis   = None
        n_cifre_mis = None
        segn_mis_sost   = None
        tipo_lettura    = None
        pre_conv    = None
        gruppo_mis_int  = None
        coeff_corr  = None
        matr_conv   = None
        n_cifre_conv    = None
        segn_conv   = None
        data_mis_eff    = None
        segn_mis_eff    = None
        segn_conv_eff = None
        note    = None

        return codice_servizio, cod_flusso, pivaUtente, piva_distr, cod_prat_attivazione, \
               cod_pdr, matr_mis, data_att_contr, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, segn_mis_sost, \
               tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff, \
               segn_mis_eff, segn_conv_eff, note
