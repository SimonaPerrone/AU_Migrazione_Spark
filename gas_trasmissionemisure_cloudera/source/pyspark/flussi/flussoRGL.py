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

class FlussoRGL:
    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(R|r)(G|g)(L|l)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        print("pattern:{}".format(pattern))
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []   
        result.append( (None, None, dataElaborazione, \
                        None, None, None, \
                        None,None,None, \
                        None,None,None,None,None,None,None,anno, mese, local_file ) )
        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = None
        # Flusso RGL
        if codice_servizio == constants.CODSERVIZIO_RGL and codice_flusso == "0055":
            file_xsd = constants.FILE_XSD_RGL_0055
        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        print ("Thread RGL")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        dfRGL = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoRGL_0055)

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        annomese_rif_udf = udf(
            lambda d: str(os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2])
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        dfRGL = dfRGL.withColumn("t_name_file",  filename_udf(col('local_file')))
        dfRGL = dfRGL.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfRGL = dfRGL.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfRGL = dfRGL.withColumn("d_caricamento",  lit(dataElaborazione))

        dfRGL = dfRGL.withColumn("data_racc", expr("from_unixtime(unix_timestamp(data_racc, 'dd/MM/yyyy'))").cast(StringType()))
        dfRGL = dfRGL.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))
	#dfRGL.show(truncate=False)

	#constants.PATHDFS_RGL="/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_rgl_p"
	#constants.PATHDFS_RGL="/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_rgl_p_compact"
        print ("Write: ", constants.PATHDFS_RGL)
        #dfRGL.write.parquet(constants.PATHDFS_RGL, 'append')
        dfRGL.write.partitionBy(constants.PARTITIONLIST_RGL).parquet(constants.PATHDFS_RGL, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_RGL)
        sc.setLocalProperty("spark.scheduler.pool", None)

    # Ritorna l'elemento
    def getItems(self, root, dataElaborazione, anno, mese, local_file):
        result          = []   

        if root == None:
	    print("Root is NULL")
            result.append( (None, None, dataElaborazione, \
                    None, None, None, \
                    None,None,None, \
                    None,None,None,None,None,None,None,anno, mese, local_file ) )
            return result
 
        codice_servizio = None if root.get("cod_servizio") is None else root.get("cod_servizio")
        cod_flusso      = None if root.get("cod_flusso") is None else root.get("cod_flusso")

        piva_utente     = None if root.find("IdentificativiRichiesta/piva_utente") is None else root.find("IdentificativiRichiesta/piva_utente").text
        piva_distr      = None if root.find("IdentificativiRichiesta/piva_distr") is None else root.find("IdentificativiRichiesta/piva_distr").text 
        mese_comp       = None if root.find("IdentificativiRichiesta/mese_comp") is None else root.find("IdentificativiRichiesta/mese_comp").text
      
        if anno == None or mese == None or piva_distr == None or piva_utente ==None:
	        result.append( (None, None, dataElaborazione, \
                        piva_utente, piva_distr, mese_comp, \
                        None,None,None, \
                        None,None,None,None,None,None,None,"EE", "EE", local_file ) )
	        return result

	if (mese_comp is not None):
		mese_comp = mese_comp.replace("/","")

        DatiPdR = root.findall("DatiPdR")
        if DatiPdR == None: 
            print("Warn", "flusso RGL non valido")
            result.append( (codice_servizio, cod_flusso, dataElaborazione, \
                    piva_utente, piva_distr, mese_comp, \
                    None,None,None, \
                    None,None,None,None,None,None,None,anno, mese, local_file ) )
            return result       


        for item in DatiPdR:
            cod_pdr, matr_mis, matr_conv, data_racc, let_tot_prel, tipo_lettura, mot_rett_lett, vol_ric, periodo_ric, let_tot_conv = self.reset()

            cod_pdr             = None if item.find("cod_pdr")            is None else item.find("cod_pdr").text
            matr_mis            = None if item.find("matr_mis")           is None else item.find("matr_mis").text
            matr_conv           = None if item.find("matr_conv")  	is None else item.find("matr_conv").text
          

            Letture             = item.findall("Lettura")

            if Letture != None: 
                for lettura in Letture:
                    data_racc       = None if lettura.find("data_racc")     is None else lettura.find("data_racc").text
                    let_tot_prel    = None if lettura.find("let_tot_prel")  is None else lettura.find("let_tot_prel").text
                    tipo_lettura    = None if lettura.find("tipo_lettura")  is None else lettura.find("tipo_lettura").text
                    mot_rett_lett   = None if lettura.find("mot_rett_lett")  is None else lettura.find("mot_rett_lett").text
                    vol_ric         = None if lettura.find("vol_ric")  is None else lettura.find("vol_ric").text
                    periodo_ric     = None if lettura.find("periodo_ric")  is None else lettura.find("periodo_ric").text
                    let_tot_conv    = None if lettura.find("let_tot_conv")  is None else lettura.find("let_tot_conv").text

                    result.append( (codice_servizio, cod_flusso, dataElaborazione, \
                                    piva_utente, piva_distr, mese_comp, \
                                    cod_pdr, matr_mis, matr_conv, \
                                    data_racc, let_tot_prel, let_tot_conv, tipo_lettura, mot_rett_lett, vol_ric, periodo_ric, anno, mese, local_file )  )
		    print ((codice_servizio, cod_flusso, dataElaborazione, \
                                    piva_utente, piva_distr, mese_comp, \
                                    cod_pdr, matr_mis, matr_conv, \
                                    data_racc, let_tot_prel, let_tot_conv, tipo_lettura, mot_rett_lett, vol_ric, periodo_ric, anno, mese, local_file )) 

            else:
                print("Warn", "flusso RGL non valido")
            
        return result

    def reset(self):
        cod_pdr   = None
        matr_mis  = None
        matr_conv = None

        data_racc    = None
        let_tot_prel = None
        tipo_lettura = None
        mot_rett_lett= None
        vol_ric      = None
        periodo_ric  = None
        let_tot_conv = None

        return cod_pdr, matr_mis, matr_conv, data_racc, let_tot_prel, tipo_lettura, mot_rett_lett, vol_ric, periodo_ric, let_tot_conv 
