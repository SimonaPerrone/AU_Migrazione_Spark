import constants
import hashlib
import datetime
import os
import re

from pyspark.sql.functions import lit, udf, col
from pyspark.sql.functions import expr

class FlussoIM1:
    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        #pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(I|i)(M|m)(1)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
	print("filname:{}".format(filename))
        #return bool(re.match(pattern, filename))
	return "IM1" in filename

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []

        cod_servizio, cod_flusso, data_esec_int, cod_prat_distr_ric_ver, rin_rich_ver, piva_distr, piva_utente, \
        cod_prat_distr, cod_pdr, cod_remi, cau_int_mis, cau_int_cor, ident_dati_pre_matr_mis, \
        ident_dati_pre_anno_fabb, ident_dati_pre_matr_conv, ident_dati_pre_n_cifre_mis, \
        ident_dati_pre_n_cifre_conv, ident_dati_pre_let_misuratore, ident_dati_pre_let_correttore, \
        ident_dati_pre_coeff_corr, ident_dati_pre_tipo_mis, ident_dati_pre_causa_stima, \
        ident_dati_post_matr_mis, ident_dati_post_anno_fabb, ident_dati_post_matr_conv, \
        ident_dati_post_n_cifre_mis, ident_dati_post_n_cifre_conv, ident_dati_post_let_misuratore, \
        ident_dati_post_let_correttore, ident_dati_post_coeff_corr, ident_dati_post_classe_gruppo_mis, \
        ident_dati_post_access_punto = self.reset()

        result.append(
            (None, None, dataElaborazione, None, None, cod_servizio, cod_flusso, data_esec_int, cod_prat_distr_ric_ver,
             rin_rich_ver, piva_distr, piva_utente, cod_prat_distr, cod_pdr, cod_remi, cau_int_mis, cau_int_cor,
             ident_dati_pre_matr_mis, ident_dati_pre_anno_fabb, ident_dati_pre_matr_conv, ident_dati_pre_n_cifre_mis,
             ident_dati_pre_n_cifre_conv, ident_dati_pre_let_misuratore, ident_dati_pre_let_correttore,
             ident_dati_pre_coeff_corr, ident_dati_pre_tipo_mis, ident_dati_pre_causa_stima,
             ident_dati_post_matr_mis, ident_dati_post_anno_fabb, ident_dati_post_matr_conv,
             ident_dati_post_n_cifre_mis, ident_dati_post_n_cifre_conv, ident_dati_post_let_misuratore,
             ident_dati_post_let_correttore, ident_dati_post_coeff_corr, ident_dati_post_classe_gruppo_mis,
             ident_dati_post_access_punto,
             anno, mese, local_file)
        )

        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = ""
        # Flusso IM1
        if codice_servizio == constants.CODSERVIZIO_IM1 and codice_flusso == "0306":
            file_xsd = constants.FILE_XSD_IM1
        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        print("Thread IM1")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        df_im1 = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoIM1)

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # data_deco_swith gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        #annomese_rif_udf = udf(
        #    lambda d: str(
        #        os.path.basename(d).split("_")[2][0:4]
        #        if (len(os.path.basename(d).split("_")[2]) > 4)
        #        else os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6]
        #                                                           if (len(os.path.basename(d).split("_")[2]) > 4)
        #                                                           else os.path.basename(d).split("_")[2][0:2])
        #)

	annomese_rif_udf = udf(
            lambda filename: str(
                filename[::-1].split("/")[2][::-1] +""+ filename[::-1].split("/")[1][::-1]  
	    )
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )


        df_im1 = df_im1.withColumn("t_name_file", filename_udf(col('local_file')))
        df_im1 = df_im1.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        df_im1 = df_im1.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))
        df_im1 = df_im1.withColumn("annomese", annomese_udf(col('data_esec_int')))

        data_elaborazione = str(datetime.datetime.now().isoformat())
        df_im1 = df_im1.withColumn("d_caricamento", lit(data_elaborazione))
        df_im1 = df_im1.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

	#df_im1.show()
        print("Write - {}".format(constants.PATHDFS_IM1))
        df_im1.write.partitionBy(constants.PARTITIONLIST_IM1).parquet(constants.PATHDFS_IM1, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_IM1)
        sc.setLocalProperty("spark.scheduler.pool", None)

    def getItems(self, root, dataElaborazione, anno, mese, local_file):
        result = []

	#print("IM1 getImtes")
        if root is None:
            print("Root is NONE")
            return self.makeError(dataElaborazione, anno, mese, local_file)

        codice_servizio = None if root.get("cod_servizio") is None else root.get("cod_servizio")
        cod_flusso = None if root.get("cod_flusso") is None else root.get("cod_flusso")


        pivaUtente = None \
            if root.find("IdentificativiRichiesta/piva_utente") is None \
            else root.find("IdentificativiRichiesta/piva_utente").text

        piva_distr = None \
            if root.find("IdentificativiRichiesta/piva_distr") is None \
            else root.find("IdentificativiRichiesta/piva_distr").text

	
        
        if pivaUtente == "":
            #print("Anno {}, mese {}, piva_distr: {}".format(anno, mese, piva_distr))
            return self.makeError(dataElaborazione, "EE", "EE", local_file)

        identificativiRichiesta_cod_prat_distr = None \
            if root.find("IdentificativiRichiesta/cod_prat_distr") is None \
            else root.find("IdentificativiRichiesta/cod_prat_distr").text

        IdentPdrREMIInt_cod_pdr = None if root.find("IdentPdrREMIInt/cod_pdr") is None else root.find(
            "IdentPdrREMIInt/cod_pdr").text
        IdentPdrREMIInt_cod_remi = None if root.find("IdentPdrREMIInt/cod_remi") is None else root.find(
            "IdentPdrREMIInt/cod_remi").text
        IdentPdrREMIInt_cau_int_mis = None if root.find("IdentPdrREMIInt/cau_int_mis") is None else root.find(
            "IdentPdrREMIInt/cau_int_mis").text
        IdentPdrREMIInt_cau_int_cor = None if root.find("IdentPdrREMIInt/cau_int_cor") is None else root.find(
            "IdentPdrREMIInt/cau_int_cor").text


        IdentDatiPre_matr_mis = None if root.find("IdentDatiPre/matr_mis") is None else root.find(
	    "IdentDatiPre/matr_mis").text
        IdentDatiPre_gruppo_mis = None if root.find("IdentDatiPre/gruppo_mis") is None else root.find(
            "IdentDatiPre/gruppo_mis").text


        IdentDatiPre_anno_fabb = None if root.find("IdentDatiPre/anno_fabb") is None else root.find(
            "IdentDatiPre/anno_fabb").text
        IdentDatiPre_matr_conv = None if root.find("IdentDatiPre/matr_conv") is None else root.find(
            "IdentDatiPre/matr_conv").text
        IdentDatiPre_n_cifre_mis = None if root.find("IdentDatiPre/n_cifre_mis") is None else root.find(
            "IdentDatiPre/n_cifre_mis").text
        IdentDatiPre_n_cifre_conv = None if root.find("IdentDatiPre/n_cifre_conv") is None else root.find(
            "IdentDatiPre/n_cifre_conv").text
        IdentDatiPre_let_misuratore = None if root.find("IdentDatiPre/let_misuratore") is None else root.find(
            "IdentDatiPre/let_misuratore").text
        IdentDatiPre_let_correttore = None if root.find("IdentDatiPre/let_correttore") is None else root.find(
            "IdentDatiPre/let_correttore").text
        IdentDatiPre_coeff_corr = None if root.find("IdentDatiPre/coeff_corr") is None else root.find(
            "IdentDatiPre/coeff_corr").text
        IdentDatiPre_tipo_mis = None if root.find("IdentDatiPre/tipo_mis") is None else root.find(
            "IdentDatiPre/tipo_mis").text
        IdentDatiPre_causa_stima = None if root.find("IdentDatiPre/causa_stima") is None else root.find(
            "IdentDatiPre/causa_stima").text


        IdentDatiPost_matr_mis = None if root.find("IdentDatiPost/matr_mis") is None else root.find(
            "IdentDatiPost/matr_mis").text
        IdentDatiPost_causa_stima = None if root.find("IdentDatiPost/causa_stima") is None else root.find(
            "IdentDatiPost/causa_stima").text

        IdentDatiPost_anno_fabb = None if root.find("IdentDatiPost/anno_fabb") is None else root.find(
            "IdentDatiPost/anno_fabb").text
        IdentDatiPost_matr_conv = None if root.find("IdentDatiPost/matr_conv") is None else root.find(
            "IdentDatiPost/matr_conv").text
        IdentDatiPost_n_cifre_mis = None if root.find("IdentDatiPost/n_cifre_mis") is None else root.find(
            "IdentDatiPost/n_cifre_mis").text
        IdentDatiPost_n_cifre_conv = None if root.find("IdentDatiPost/n_cifre_conv") is None else root.find(
            "IdentDatiPost/n_cifre_conv").text
        IdentDatiPost_let_misuratore = None if root.find("IdentDatiPost/let_misuratore") is None else root.find(
            "IdentDatiPost/let_misuratore").text
        IdentDatiPost_let_correttore = None if root.find("IdentDatiPost/let_correttore") is None else root.find(
            "IdentDatiPost/let_correttore").text
        IdentDatiPost_coeff_corr = None if root.find("IdentDatiPost/coeff_corr") is None else root.find(
            "IdentDatiPost/coeff_corr").text
        IdentDatiPost_classe_gruppo_mis = None if root.find("IdentDatiPost/classe_gruppo_mis") is None else root.find(
            "IdentDatiPost/classe_gruppo_mis").text
        IdentDatiPost_access_punto = None if root.find("IdentDatiPost/access_punto") is None else root.find(
            "IdentDatiPost/access_punto").text

        data_esec_int = None if root.find("data_esec_int") is None else root.find("data_esec_int").text
        cod_prat_distr_ric_ver = None if root.find("cod_prat_distr_ric_ver") is None else root.find(
            "cod_prat_distr_ric_ver").text
        rin_rich_ver = None if root.find("rin_rich_ver") is None else root.find("rin_rich_ver").text


        result.append(
            (codice_servizio, cod_flusso, dataElaborazione, pivaUtente, piva_distr,
             identificativiRichiesta_cod_prat_distr, IdentPdrREMIInt_cod_pdr, IdentPdrREMIInt_cod_remi,
             IdentPdrREMIInt_cau_int_mis, IdentPdrREMIInt_cau_int_cor, IdentDatiPre_gruppo_mis, IdentDatiPre_anno_fabb,
             IdentDatiPre_matr_mis, IdentDatiPre_matr_conv, IdentDatiPre_n_cifre_mis, IdentDatiPre_n_cifre_conv, IdentDatiPre_let_misuratore,
             IdentDatiPre_let_correttore, IdentDatiPre_coeff_corr, IdentDatiPre_tipo_mis, IdentDatiPre_causa_stima,
             IdentDatiPost_matr_mis, IdentDatiPost_anno_fabb, IdentDatiPost_matr_conv,
             IdentDatiPost_causa_stima, IdentDatiPost_n_cifre_mis,
             IdentDatiPost_n_cifre_conv, IdentDatiPost_let_misuratore, IdentDatiPost_let_correttore,
             IdentDatiPost_coeff_corr, IdentDatiPost_classe_gruppo_mis, IdentDatiPost_access_punto,
             data_esec_int, cod_prat_distr_ric_ver, rin_rich_ver,  anno, mese, local_file)
        )

        print("Return: ", result)
        return result

    def reset(self):
        cod_servizio = None
        cod_flusso = None
        data_esec_int = "EE"
        cod_prat_distr_ric_ver = None
        rin_rich_ver = None

        # --IdentificativiRichiesta
        piva_distr = None
        piva_utente = None
        cod_prat_distr = None

        # --IdentPdrREMIInt
        cod_pdr = None
        cod_remi = None
        cau_int_mis = None
        cau_int_cor = None

        # --IdentDatiPre
        ident_dati_pre_matr_mis = None
        ident_dati_pre_anno_fabb = None
        ident_dati_pre_matr_conv = None
        ident_dati_pre_n_cifre_mis = None
        ident_dati_pre_n_cifre_conv = None
        ident_dati_pre_let_misuratore = None
        ident_dati_pre_let_correttore = None
        ident_dati_pre_coeff_corr = None
        ident_dati_pre_tipo_mis = None
        ident_dati_pre_causa_stima = None

        # --IdentDatiPost
        ident_dati_post_matr_mis = None
        ident_dati_post_anno_fabb = None
        ident_dati_post_matr_conv = None
        ident_dati_post_n_cifre_mis = None
        ident_dati_post_n_cifre_conv = None
        ident_dati_post_let_misuratore = None
        ident_dati_post_let_correttore = None
        ident_dati_post_coeff_corr = None
        ident_dati_post_classe_gruppo_mis = None
        ident_dati_post_access_punto = None


        return cod_servizio, cod_flusso, data_esec_int, cod_prat_distr_ric_ver, rin_rich_ver, piva_distr, piva_utente,\
               cod_prat_distr, cod_pdr, cod_remi, cau_int_mis, cau_int_cor, ident_dati_pre_matr_mis, \
               ident_dati_pre_anno_fabb, ident_dati_pre_matr_conv, ident_dati_pre_n_cifre_mis, \
               ident_dati_pre_n_cifre_conv, ident_dati_pre_let_misuratore, ident_dati_pre_let_correttore, \
               ident_dati_pre_coeff_corr, ident_dati_pre_tipo_mis, ident_dati_pre_causa_stima, \
               ident_dati_post_matr_mis, ident_dati_post_anno_fabb, ident_dati_post_matr_conv, \
               ident_dati_post_n_cifre_mis, ident_dati_post_n_cifre_conv, ident_dati_post_let_misuratore, \
               ident_dati_post_let_correttore, ident_dati_post_coeff_corr, ident_dati_post_classe_gruppo_mis, \
               ident_dati_post_access_punto

