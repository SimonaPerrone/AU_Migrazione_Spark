import constants

import hashlib
import datetime
import os
import re

from pyspark.sql.functions import lit, udf, col
from pyspark.sql.functions import expr

class FlussoDEF:
    DEBUG = True

    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(D|d)(E|e)(F|f)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []
        cod_prat_utente, cod_prat_distr, cod_pdr, matr_mis, data_deco_switch, vol_annuo_sost, classe_gruppo_mis, \
        n_cifre_mis, segn_mis_sost, tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, \
        segn_conv, data_mis_eff, segn_mis_eff, segn_conv_eff, note = self.reset()

        result.append(
            (None, None, dataElaborazione, None, None, cod_prat_utente, cod_prat_distr,
             data_deco_switch, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, matr_mis, cod_pdr, segn_mis_sost,
             tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff,
             segn_mis_eff, segn_conv_eff, note, anno, mese, local_file)
        )

        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = ""
        # Flusso DEF
        if codice_servizio == constants.CODSERVIZIO_DEF and codice_flusso == "0350":
            file_xsd = constants.FILE_XSD_DEF_0350
        elif codice_servizio == constants.CODSERVIZIO_DEF and codice_flusso == "0351":
            file_xsd = constants.FILE_XSD_DEF_0351

        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        print ("Thread DEF")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        dfDEF = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoDEF)

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

	annomese_rif_udf = udf(
            lambda d: str(
                os.path.basename(d).split("_")[2][0:4]
                if (len(os.path.basename(d).split("_")[2]) > 4)
                else os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6]
                                                                   if (len(os.path.basename(d).split("_")[2]) > 4)
                                                                   else os.path.basename(d).split("_")[2][0:2])
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        # data_deco_swith gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        dfDEF = dfDEF.withColumn("t_name_file",  filename_udf(col('local_file')))
        dfDEF = dfDEF.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfDEF = dfDEF.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))
        dfDEF = dfDEF.withColumn("annomese", annomese_udf(col('data_deco_switch')))
        dfDEF = dfDEF.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfDEF = dfDEF.withColumn("d_caricamento",  lit(dataElaborazione))

        print("Partition List: ", constants.PARTITIONLIST_DEF)
        print("Write: ", constants.PATHDFS_DEF)
        dfDEF.write.partitionBy(constants.PARTITIONLIST_DEF).parquet(constants.PATHDFS_DEF, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_DEF)
        sc.setLocalProperty("spark.scheduler.pool", None)

    def getItems(self, root, dataElaborazione, anno, mese, local_file):
        result = []

        if root is None:
            return self.makeError(dataElaborazione, anno, mese, local_file)

        codice_servizio = None if root.get("cod_servizio") is None else root.get("cod_servizio")
        cod_flusso = None if root.get("cod_flusso") is None else root.get("cod_flusso")

        pivaUtente = None if root.find("IdentificativiRichiesta/piva_utente") is None else root.find("IdentificativiRichiesta/piva_utente").text
        piva_distr = None if root.find("IdentificativiRichiesta/piva_distr") is None else root.find("IdentificativiRichiesta/piva_distr").text

        if anno == "" or mese == "" or piva_distr == "" or pivaUtente =="":
            return self.makeError(dataElaborazione, "EE", "EE", local_file)

        cod_prat_utente = None if root.find("IdentificativiRichiesta/cod_prat_utente") is None else root.find("IdentificativiRichiesta/cod_prat_utente").text
        cod_prat_distr = None if root.find("IdentificativiRichiesta/cod_prat_distr") is None else root.find("IdentificativiRichiesta/cod_prat_distr").text


        data_deco_switch = None if root.find("DatiTecnici/data_deco_switch") is None else root.find("DatiTecnici/data_deco_switch").text
        vol_annuo_sost = None if root.find("DatiTecnici/vol_annuo_sost") is None else root.find("DatiTecnici/vol_annuo_sost").text
        classe_gruppo_mis = None if root.find("DatiTecnici/classe_gruppo_mis") is None else root.find("DatiTecnici/classe_gruppo_mis").text
        n_cifre_mis = None if root.find("DatiTecnici/n_cifre_mis") is None else root.find("DatiTecnici/n_cifre_mis").text
        matr_mis = None if root.find("DatiTecnici/matr_mis") is None else root.find("DatiTecnici/matr_mis").text
        cod_pdr = None if root.find("DatiTecnici/cod_pdr") is None else root.find("DatiTecnici/cod_pdr").text

        segn_mis_sost = None if root.find("DatiLettura/segn_mis_sost") is None else root.find("DatiLettura/segn_mis_sost").text
        tipo_lettura = None if root.find("DatiLettura/tipo_lettura") is None else root.find("DatiLettura/tipo_lettura").text
        pre_conv = None if root.find("DatiLettura/pre_conv") is None else root.find("DatiLettura/pre_conv").text
        gruppo_mis_int = None if root.find("DatiLettura/gruppo_mis_int") is None else root.find("DatiLettura/gruppo_mis_int").text
        coeff_corr = None if root.find("DatiLettura/coeff_corr") is None else root.find("DatiLettura/coeff_corr").text
        matr_conv = None if root.find("DatiLettura/matr_conv") is None else root.find("DatiLettura/matr_conv").text
        n_cifre_conv = None if root.find("DatiLettura/n_cifre_conv") is None else root.find("DatiLettura/n_cifre_conv").text
        segn_conv = None if root.find("DatiLettura/segn_conv") is None else root.find("DatiLettura/segn_conv").text
        data_mis_eff = None if root.find("DatiLettura/data_mis_eff") is None else root.find("DatiLettura/data_mis_eff").text
        segn_mis_eff = None if root.find("DatiLettura/segn_mis_eff") is None else root.find("DatiLettura/segn_mis_eff").text
        segn_conv_eff = None if root.find("DatiLettura/segn_conv_eff") is None else root.find("DatiLettura/segn_conv_eff").text

        note = None if root.find("note") is None else "" if root.get("note") is None else root.get("note").text

        result.append(
            (codice_servizio, cod_flusso, dataElaborazione, pivaUtente, piva_distr, cod_prat_utente, cod_prat_distr,
             data_deco_switch, vol_annuo_sost, classe_gruppo_mis, n_cifre_mis, matr_mis, cod_pdr, segn_mis_sost,
             tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv, segn_conv, data_mis_eff,
             segn_mis_eff, segn_conv_eff, note, anno, mese, local_file)
        )

        return result


    def reset(self):
        cod_prat_utente = None
        cod_prat_distr = None
        cod_pdr = None
        matr_mis = None
        data_deco_switch = None
        vol_annuo_sost = None
        classe_gruppo_mis = None
        n_cifre_mis = None
        segn_mis_sost = None
        tipo_lettura = None
        pre_conv = None
        gruppo_mis_int = None
        coeff_corr = None
        matr_conv = None
        n_cifre_conv = None
        segn_conv = None
        data_mis_eff = None
        segn_mis_eff = None
        segn_conv_eff = None
        note = None

        return cod_prat_utente, cod_prat_distr, cod_pdr, matr_mis, data_deco_switch, vol_annuo_sost, classe_gruppo_mis,\
               n_cifre_mis, segn_mis_sost, tipo_lettura, pre_conv, gruppo_mis_int, coeff_corr, matr_conv, n_cifre_conv,\
               segn_conv, data_mis_eff, segn_mis_eff, segn_conv_eff, note

