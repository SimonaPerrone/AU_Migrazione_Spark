import datetime
import constants
import hashlib
import os
import re
from pyspark.sql.functions import lit, udf, col
from pyspark.sql.functions import expr

class FlussoRMV:
    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        """
        Validazione del file xml
        :param filename: nome del file xml da verificare
        :return: ritorna booleano; True un elemento valido altrimenti False
        """
        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(R|r)(M|m)(V|v)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        """
        Crea un oggetto vuoto. Utilizzare questa funzione quando si genera un errore
        :param data_elaborazione: Data elaborazione
        :param anno: anno
        :param mese: mese
        :param local_file: nome del file da elaborare
        :return: ritorna lista con un elemento vuoto
        """
        result = []

        cod_pdr, cod_prat_attivazione, matr_mis, matr_conv, coeff_corr, \
            progr_anno_term, data_comp, let_tot_prel, let_tot_conv, \
            mot_rett_lett = self.reset()

        result.append(
            (None, None, dataElaborazione, None, None, cod_pdr, cod_prat_attivazione, matr_mis, matr_conv, coeff_corr,
             progr_anno_term, data_comp, let_tot_prel, let_tot_conv, mot_rett_lett, anno, mese, local_file)
        )
        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        """
        Ritorna il nome del file XSD da utilizzare per la validazione
        :param codice_servizio: nome del codice servizio
        :param codice_flusso: nome del codice flusso
        :return: ritorna il nome del file XSD
        """
        file_xsd = ""
        # Flusso RMV
        if codice_servizio == constants.CODSERVIZIO_RMV and codice_flusso == "0400":
            file_xsd = constants.FILE_XSD_RMV_0400
        return file_xsd

    def write(self, rdd, pool_id, sc, sql_ctx):
        """
        Scrittura degli RDD all'interno della tabella
        :param rdd: rdd da elaborare e scrivere
        :param pool_id: identificativo univoco, se si vuole eseguire questa funzione all'interno di un thread
        :param sc: spark context
        :param sql_ctx: sql context
        :return:
        """
        print("Thread RMV")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        df_rmv = sql_ctx.createDataFrame(rdd, schema=constants.schemaFlussoRMV)

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # data_comp gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        annomese_rif_udf = udf(
            lambda d: str(os.path.basename(d).split("_")[2][0:4]
                          if (len(os.path.basename(d).split("_")[2]) > 4)
                          else os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6]
                                if (len(os.path.basename(d).split("_")[2]) > 4)
                                else os.path.basename(d).split("_")[2][0:2])
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        df_rmv = df_rmv.withColumn("t_name_file",  filename_udf(col('local_file')))
        df_rmv = df_rmv.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        df_rmv = df_rmv.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))
        df_rmv = df_rmv.withColumn("annomese", annomese_udf(col('data_comp')))

        date_elaboration = str(datetime.datetime.now().isoformat())
        df_rmv = df_rmv.withColumn("d_caricamento",  lit(date_elaboration))
        df_rmv = df_rmv.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

        print("Write: ", constants.PATHDFS_RMV)
        print("Partitions: ", constants.PARTITIONLIST_RMV)
        df_rmv.write.partitionBy(constants.PARTITIONLIST_RMV).parquet(constants.PATHDFS_RMV, 'append')
        sql_ctx.sql(constants.CMD_REFRESH_RMV)
        sc.setLocalProperty("spark.scheduler.pool", None)

    def getItems(self, root, dataElaborazione, anno, mese, local_file):
        """
        Elaborazione del file xml, recuperando l'elemento da inserire nella tabella
        :param root: elemento principale del nodo xml
        :param date_elaboration: data elaborazione
        :param anno: anno
        :param mese: mese
        :param local_file: nome del file xml
        :return: Ritorna un array degli elementi individuati nel file
        """
        result = []
	print("GetItems RMV")

        if root is None:
            return self.makeError(dataElaborazione, anno, mese, local_file)

        codice_servizio = None if root.get("cod_servizio") is None else root.get("cod_servizio")
        cod_flusso = None if root.get("cod_flusso") is None else root.get("cod_flusso")

        piva_utente = None \
            if root.find("IdentificativiRichiesta/piva_utente") is None \
            else root.find("IdentificativiRichiesta/piva_utente").text

        piva_distr = None \
            if root.find("IdentificativiRichiesta/piva_distr") is None \
            else root.find("IdentificativiRichiesta/piva_distr").text

        if anno == "" or mese == "" or piva_distr == "" or piva_utente == "":
            return self.makeError(dataElaborazione, "EE", "EE", local_file)

        DatiPdR = root.findall("DatiPdR")

        if DatiPdR is None:
            print("Warn", "flusso RMV non valido")
            return self.makeError(dataElaborazione, anno, mese, local_file)

        for item in DatiPdR:

            cod_pdr = None if item.find("cod_pdr") is None else item.find("cod_pdr").text
            cod_prat_attivazione = None \
                if item.find("cod_prat_attivazione") is None \
                else item.find("cod_prat_attivazione").text

            matr_mis = None if item.find("matr_mis") is None else item.find("matr_mis").text
            matr_conv = None if item.find("matr_conv") is None else item.find("matr_conv").text
            coeff_corr = 0 if item.find("coeff_corr") is None else float(item.find("coeff_corr").text)
            progr_anno_term = 0 if item.find("progr_anno_term") is None else float(item.find("progr_anno_term").text)
            data_comp = None if item.find("data_comp") is None else item.find("data_comp").text
            let_tot_prel = 0 if item.find("let_tot_prel") is None else float(item.find("let_tot_prel").text)
            let_tot_conv = 0 if item.find("let_tot_conv") is None else float(item.find("let_tot_conv").text)
            mot_rett_lett = None if item.find("mot_rett_lett") is None else item.find("mot_rett_lett").text

            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr, cod_pdr, cod_prat_attivazione,
                 matr_mis, matr_conv, coeff_corr, progr_anno_term, data_comp, let_tot_prel, let_tot_conv, mot_rett_lett,
                 anno, mese, local_file)
            )

        return result

    def reset(self):
        """
        Reset
        :return: Resetta gli elementi
        """
        cod_pdr = None
        cod_prat_attivazione = None
        matr_mis = None
        matr_conv = None
        coeff_corr = None
        progr_anno_term = None
        data_comp = None
        let_tot_prel = None
        let_tot_conv = None
        mot_rett_lett = None

        return cod_pdr, cod_prat_attivazione, matr_mis, matr_conv, coeff_corr, progr_anno_term, \
            data_comp, let_tot_prel, let_tot_conv, mot_rett_lett

