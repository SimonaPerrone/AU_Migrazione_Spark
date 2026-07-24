import getopt
import sys
import os
import json
from functions.spark_utils import *
from REQs.classes.functions import jobTDS
from pyspark.sql.utils import IllegalArgumentException
import shutil
import datetime
import pyspark.sql.functions as F
import time
from pyspark.sql import Window

recupero = False
# recupero = True
disable_write_csv = False
# disable_write_csv = True


def main(argv):
    version = '1.0'
    directory_input = ""
    mode = 'yarn-client'

    try:
        opts, args = getopt.getopt(argv, "hvi:f:m:", ["input=", "function=", "mode="])
    except getopt.GetoptError as err:
        sys.exit(2)

    # -i, input:        Directory di input
    # -v:               Versione
    # -f, function:     Funzione (indice)

    for opt, arg in opts:
        print(opt, arg)

        if opt == '-h':
            # TODO usage()
            sys.exit()
        elif opt == '-v':
            print('Version: ', version)
            sys.exit()
        elif opt in ('-i', '--input'):
            directory_input = arg
        elif opt in ('-f', '--function'):
            function_input = arg
        elif opt in ('-m', '--mode'):
            mode = arg

    # Verificare se directory_input e' una directory esistente
    print("directory input {}".format(directory_input))
    if not checkDirectoryInput(directory_input):
        print("Error: directory input not exist or not valid")
        sys.exit(1)

    path = os.path.join(os.getcwd(), "../conf/functions.json")
    config_jobs = load_config(path)

    # Recupera l'oggetto function
    # function = switcher(function_input, conf = config_jobs)
    function = jobTDS.JobTDS(config_jobs)

    # Apertura file di input
    directory_input_files = directory_input + "/input.in"
    listFiles = []
    f = open(directory_input_files, "r")
    for x in f:
        directory = x.replace('\n', '')
        try:
            listFiles.append(getFiles(directory))
        except:
            print("la Directory non esiste", directory)

    # flat la lista dei files
    # listFiles = list(set([y for x in listFiles for y in x]))
    listFiles = list(set([y for x in listFiles for y in x if "tds" in y.lower()]))

    # file - text
    list_file_anomalie = []
    list_valid_record_bad = []
    list_valid_record_good = []

    listElaborate = []
    for f in listFiles:
        salto_file = False

        # Controllare se il file e' AMM
        if "AMM" in f:
            print("Salto file; file di output {}".format(f))
            continue

        if ".part" in f:
            print("Salto file; file di output {}".format(f))
            continue

        if not recupero:
            # Controllare se ce' un file di tipo AMM
            check_file = f.replace(".csv", "") + "_AMM"
            for item in listFiles:
                if len(item) > len(check_file):
                    if item[:len(check_file)] == check_file:
                        print("Salto file gia elaborato: {}".format(f))
                        salto_file = True
                        break

        if salto_file:
            continue

        # Verifica validazione file
        result_validazione_file = function.validate_filename(f)

        if result_validazione_file:
            # print ("File valido", f)
            listElaborate.append(f)
            pass
        else:
            # Scrittura del risultato della validazione errata
            # now = datetime.datetime.now()
            # text = "200;Il file non rispetta la struttura prevista - Nome del file non conforme"
            # time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
            # w_filename = f.replace(".csv", "") + "_AMM_" + time_stamp + ".csv"
            code = "200"
            message = "Il file non rispetta la struttura prevista - Nome del file non conforme"

            # l = [("200", "Il file non rispetta la struttura prevista - Nome del file non conforme", f, 0)]
            # dataframe_KO = sqlCtx.createDataFrame(l,function.getSchema_ANOMALIE_FILE())
            # function.save_data_anomalie_file(sqlCtx, dataframe_KO)
            # list_file_anomalie.append((f, text))
            list_valid_record_bad.append((code, message, f, 0))

            # function.save_to_file(w_filename, text)
            print("Header non valido {}".format(f))
            pass

        continue

    if not listElaborate:
        print("Nessun file da elaborare")
        return

    lista_elaborate = []
    for f in listElaborate:
        if "AMM" in f:
            print("Error: file AMM in lista: {}".format(f))
        else:
            lista_elaborate.append(f)

    # print ("List - 1",listElaborate)
    listElaborate = list(set(lista_elaborate))

    print("Lista file elaborare: {}".format(len(listElaborate)))

    # return

    # Creare spark context
    sc, sqlCtx = set_spark_context("GAS - Function: " + function_input, mode)
    sqlCtx.setConf("spark.dynamicAllocation.enabled", "false")
    sqlCtx.setConf("spark.sql.broadcastTimeout", "50000")

    # list_values_files = []

    for f in listElaborate:
        print("Check file: {}".format(f))
        header = "1;" + getHeader(f).encode('utf-8')
        # print ("Header:", header)

        is_valid_header = function.check_definition_record(header)
        print("File: {} header valido {}:".format(f, is_valid_header))

        if not is_valid_header:
            # Scrittura del risultato della validazione errata
            # now = datetime.datetime.now()
            code, message = function.getHeaderCodeError()
            # text = code + ";" + message
            # l = [(code, message, f, 0)]

            # Richiesta del 20190624
            # text = "200;Il file non rispetta la struttura prevista - Nome del file non conforme"
            # time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
            # w_filename = f.replace(".csv", "") + "_AMM_" + time_stamp + ".csv"
            # l = ("200", "Il file non rispetta la struttura prevista - Nome del file non conforme", f, 0)

            # dataframe_KO = sqlCtx.createDataFrame(l,function.getSchema_ANOMALIE_FILE())
            # function.save_data_anomalie_file(sqlCtx, dataframe_KO)
            # list_file_anomalie.append((f, text))
            list_valid_record_bad.append((code, message, f, 0))

            # TODO Attivare
            # function.save_to_file(w_filename, text)
            continue
        list_values_files = elaborate(f)[1:]
        # print ("list_values_files:", list_values_files)

        list_valid_record = []

        print("Validazione record file: {}".format(f))
        for valore in list_values_files:
            valid_record = function.valida_record(str(valore[0]) + ";" + valore[1], f)

            if not valid_record[0]:
                item = (valid_record[3], valid_record[4], f, valid_record[5])
                list_valid_record_bad.append(item)
                # print (item)
            else:
                list_valid_record_good.append(valid_record)

    # print("Lista file anomalie: {}".format(list_file_anomalie))
    # print("Lista file validi: {}".format(list_valid_record_good))
    # print("Lista file non validi: {}".format(list_valid_record_bad))

    # scrittura su tabella hive AMM_TMP
    # scrittura su tabella hive ANOM_TMP
    rdd_good = sc.parallelize(list_valid_record_good)
    rdd_bad = sc.parallelize(list_valid_record_bad)

    print("Count GOOD:{}".format(rdd_good.count()))
    print("Count BAD:{}".format(rdd_bad.count()))
    # return

    dataframe_OK = sqlCtx.createDataFrame(rdd_good, function.getSchema_AMMISSIBILITA_FILE())
    dataframe_KO = sqlCtx.createDataFrame(rdd_bad, function.getSchema_ANOMALIE_FILE())

    # print("Dataframe OK")
    # dataframe_OK.show()
    # print("Dataframe KO")
    # dataframe_KO.show()
    # return

    # Versione Prod
    dataframe_KO.write.parquet('/user/hive/warehouse/settle_gas.db/ANOM_TMP', 'overwrite')
    dataframe_OK.write.parquet('/user/hive/warehouse/settle_gas.db/AMM_TMP', 'overwrite')

    # print("Dataframe OK")
    # dataframe_OK.show()
    # print("Dataframe KO")
    # dataframe_KO.show()
    # print("count: {}".format((count_records - dataframe_OK.count() )))
    # return

    function.clear_table(sqlCtx)

    function.repair_table(sqlCtx, "settle_gas.ANOM_TMP")
    function.repair_table(sqlCtx, "settle_gas.AMM_TMP")
    print("Scrittura hdfs tmp completato")

    function.save_data_anomalie(sqlCtx, dataframe_KO)
    print("Scrittura anomalie 1")

    rcugas_massivo_dataframe, dataframe_rcuazienda, rcugas_connessioni_distr, rcugas_tmp_va1 = function.load_tables(
        sqlCtx)

    print("Controllo duplicati")
    dataframe_OK, dataframe_KO_duplicate = function.get_duplicate(sqlCtx, dataframe_OK)
    function.save_data_anomalie(sqlCtx, dataframe_KO_duplicate)
    print("Scrittura anomalie 2")

    print("check rcu filename")
    dataframe_OK, dataframe_KO_rcu_file = function.check_rcu_filename(sqlCtx, dataframe_OK, dataframe_rcuazienda)
    function.save_data_anomalie(sqlCtx, dataframe_KO_rcu_file)
    print("Scrittura anomalie 3")

    print("check Il PdR e' inesistente")
    dataframe_OK, dataframe_KO_1 = function.get_rcu_1(sqlCtx, dataframe_OK, rcugas_massivo_dataframe)
    function.save_data_anomalie(sqlCtx, dataframe_KO_1)
    print("Scrittura anomalie 4")

    print("check Il PdR non e' attivo")
    dataframe_OK, dataframe_KO_2 = function.get_rcu_2(sqlCtx, dataframe_OK, rcugas_massivo_dataframe)
    function.save_data_anomalie(sqlCtx, dataframe_KO_2)
    print("Scrittura anomalie 5")

    print("check extra")
    dataframe_OK, dataframe_KO_extra = function.extra(sqlCtx, dataframe_OK, rcugas_massivo_dataframe,
                                                      rcugas_connessioni_distr, rcugas_tmp_va1)
    function.save_data_anomalie(sqlCtx, dataframe_KO_extra)
    print("Scrittura anomalie 6")

    print("check rcu UdD")
    dataframe_OK, dataframe_KO_rcu_udd = function.get_rcu_rcu_udd(sqlCtx, dataframe_OK, rcugas_massivo_dataframe)
    function.save_data_anomalie(sqlCtx, dataframe_KO_rcu_udd)
    print("Scrittura anomalie 7")

    function.save_data_ammissiblita(sqlCtx, dataframe_OK)
    print("Salvataggio ammissibilita")

    dataframe_union = function.generate_dataframe_to_csv(sqlCtx)
    dataframe_union = dataframe_union.orderBy('num_riga')
    print("Generazione tabella ammissibilita e anomalie")

    if not disable_write_csv:
        function.save_to_csv_test(sqlCtx, dataframe_union)
        print("Scrittura CSV completata")

    function.save_data_part(sqlCtx, dataframe_union)
    print("Scrittura tabella finale completata")

    function.save_data(sqlCtx, dataframe_union)
    print("Scrittura tabella finale (vecchia versione) completata")

    function.backup_table_tmp(sqlCtx)
    print("Scrittura tabella backup completata")
    return


def debug(function, sqlCtx, dataframe_OK, dataframe_KO):
    function.save_data_ammissiblita(sqlCtx, dataframe_OK)
    dataframe_OK.show()
    dataframe_KO.show()
    return


def getHeader(filename):
    first_line = ""
    with open(filename) as f:
        first_line = f.readline()

    try:
        value = first_line.encode('utf-8').replace("\n", "").replace("\r", "")
    except:
        value = ""

    return value


def elaborate(filename):
    # print ("Elaborate filename:", filename)
    file = open(filename, "r")
    items_result = []

    for index, item in enumerate(file):
        # items_result.append(str(index+1) + ";" + item.replace("\r","").replace("\n",""))
        items_result.append((index + 1, item.replace("\r", "").replace("\n", "")))

    return items_result


def RemoveFolder(directory):
    print("Delete file in directory:", directory)

    for tmp_dir in os.listdir(directory):
        to_delete = os.path.join(directory, tmp_dir)
        os.remove(to_delete)


def expand(row):
    result = []
    file_name = row[0]

    for item in row[1]:
        result.append((row[0], item))

    return result


def stampa(value):
    try:
        print("***** ", value)
    except:
        print("***** EEE")


def load_config(path):
    """
    Load configuration file with all the needed parameters
    """
    with open(path, 'r') as conf_file:
        conf = json.load(conf_file)
    return conf


def checkDirectoryInput(directorySRC):
    result = os.path.isdir(directorySRC)

    return result


def getFiles(directorySRC):
    files = [os.path.join(directorySRC, f) for f in os.listdir(directorySRC)]
    return files


if __name__ == "__main__":
    main(sys.argv[1:])
