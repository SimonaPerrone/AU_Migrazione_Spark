import getopt
import sys
import os
import json
from REQs.classes.functions.switcher import switcher
from functions.spark_utils import *
from pyspark.sql.utils import IllegalArgumentException
import shutil
import datetime
import pyspark.sql.functions as F
import time
from pyspark.sql import Window

# Se Recupero = true vengono prelevati tutti i file, non verr eseguito nessuna verifica
# se sono presenti i file AMM
# recupero=False
recupero = True

# disable_write_csv = true viene disattivata la scrittura sul file system dei file csv
# disable_write_csv=False
disable_write_csv = True


def main(argv):
    version = '1.0'
    directory_input = ""
    mode = 'yarn-client'
    file_list = None

    print("Start Ingestion SAG")

    try:
        opts, args = getopt.getopt(argv, "hvi:f:m:p:e:", ["input=", "function=", "mode=", "files"])
    except getopt.GetoptError as err:
        print("Error: {}".format(err))
        sys.exit(2)

    # -i, input:        Directory di input
    # -v:               Versione
    # -f, function:     Funzione (indice)

    for opt, arg in opts:
        print("{}\t{}".format(opt, arg))

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
        elif opt in ('-p', '--process'):
            part = arg
        elif opt in ('-e', '--files'):
            file_list = arg

    # Verificare se directory_input e' una directory esistente
    if not checkDirectoryInput(directory_input):
        print("Error: directory input not exist or not valid {}".format(directory_input))
        sys.exit(1)

    if not function_input:
        print("Error: Devi indicare il tipo di flusso")
        sys.exit(1)

    if not part:
        print("Error: Utilizzare parametro -p 1 o -p 2")
        sys.exit(1)

    path = os.path.join(os.getcwd(), "../conf/functions.json")
    config_jobs = load_config(path)

    # Recupera l'oggetto function
    function = switcher(function_input, conf=config_jobs)

    # Creare spark context
    sc, sqlCtx = set_spark_context("GAS - Function: " + function_input, mode)
    sqlCtx.setConf("spark.dynamicAllocation.enabled", "false")
    sqlCtx.setConf("spark.sql.broadcastTimeout", "50000")

    if part == '1':
        print('Parte 1')

        if file_list is None:
            print("Elaborazione files into 'input.in'")
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
        else:
            print("Estrazione files da query: {}".format(file_list))
            with open(file_list, 'r') as content_file:
                query_tmp = content_file.read()
            print("Query: {}".format(query_tmp))
            listFiles = sqlCtx.sql(query_tmp).flatMap(lambda d: d).collect() 

        # flat la lista dei files
        # listFiles = [y for x in listFiles for y in x]
        # listFiles = list(set([y for x in listFiles for y in x]))
        listFiles = list(set([y for x in listFiles for y in x if "sag" in y.lower()]))

        # file - text
        list_file_anomalie = []
        list_valid_record_good = []
        list_valid_record_bad = []
        list_valid_record = []

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
                check_file = f.strip(".csv").strip(".CSV") + "_AMM"
                for item in listFiles:
                    if check_file in item:
                        print("Salto file gia elaborato: ", f)
                        salto_file = True
                        break

            if salto_file:
                continue

            # Verifica validazione file
            result_validazione_file = function.validate_filename(f)

            if result_validazione_file:
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

                # function.save_to_file(w_filename, text)

                list_valid_record_bad.append((code, message, f, 0))
                print("File non valido: {}".format(f))
                pass

            continue

        if not listElaborate:
            print("Nessun file da elaborare")
            return

        listElaborate = list(set(listElaborate))
        print("Lista file elaborare: {}".format(len(listElaborate)))

        for f in listElaborate:
            header = "1;" + getHeader(f).encode('utf-8')

            is_valid_header = function.check_definition_record(header)

            print("File: {} valid: {}".format(f, is_valid_header))
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
                # l = [("200", "Il file non rispetta la struttura prevista - Nome del file non conforme", f, 0)]

                # dataframe_KO = sqlCtx.createDataFrame(l,function.getSchema_ANOMALIE_FILE())
                # function.save_data_anomalie_file(sqlCtx, dataframe_KO)
                # list_file_anomalie.append((f, text))

                # function.save_to_file(w_filename, text)
                list_valid_record_bad.append((code, message, f, 0))

                continue

            list_values_files = elaborate(f)[1:]

            for valore in list_values_files:
                valid_record = function.valida_record(str(valore[0]) + ";" + valore[1], f)

                if not valid_record[0]:
                    item = (valid_record[3], valid_record[4], f, valid_record[5])
                    list_valid_record_bad.append(item)
                    # print (item)
                else:
                    list_valid_record_good.append(valid_record)

        rdd_good = sc.parallelize(list_valid_record_good)
        rdd_bad = sc.parallelize(list_valid_record_bad)

        dataframe_OK = sqlCtx.createDataFrame(rdd_good, function.getSchema_AMMISSIBILITA_FILE())
        dataframe_KO = sqlCtx.createDataFrame(rdd_bad, function.getSchema_ANOMALIE_FILE())

        # dataframe_OK.show(truncate=False)
        # dataframe_KO.show(truncate=False)

        # return

        # Versione Prod
        dataframe_KO.write.parquet('/user/hive/warehouse/settle_gas.db/ANOM_TMP_SAG', 'overwrite')
        dataframe_OK.write.parquet('/user/hive/warehouse/settle_gas.db/AMM_TMP_SAG', 'overwrite')

    if part == '2':
        print('Parte 2')
        dataframe_union = function.generate_dataframe_to_csv(sqlCtx)
        function.save_to_csv(sqlCtx, dataframe_union)
        # function.save_data(sqlCtx, dataframe_OK )

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

