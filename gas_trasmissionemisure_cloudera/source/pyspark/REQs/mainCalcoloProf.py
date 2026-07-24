
import getopt
import sys
import os
import json
from REQs.classes.calcoloProfStd.switcher import switcher
from functions.spark_utils import *
from pyspark.sql.utils import IllegalArgumentException


def main(argv):
    version = '1.0'
    directory_input = ""
    mode = 'yarn-client'
    params = []

    # Indica se la variabile function e' un array
    is_function_setted = False
    functions_array = []

    try:
        opts, args = getopt.getopt(argv, "hvi:f:m:p:", ["input=", "function=", "mode=", "params="])
    except getopt.GetoptError as err:
        sys.exit(2)

    # -i, input:        Directory di input
    # -v:               Versione
    # -f, function:     Funzione (indice)

    for opt, arg in opts:
        print (opt,arg)
    
        if opt == '-h':
            #TODO usage()
            sys.exit()
        elif opt == '-v':
            print('Version: ', version)
            sys.exit()
        elif opt in ('-i', '--input'):
            directory_input = arg
        elif opt in ('-f', '--function'):
            #function_input = arg

            # Append la nuova funzione all'interno dell'array
            functions_array.append(arg)

        elif opt in ('-m', '--mode'):
            mode = arg
        elif opt in ('-p', '--params'):
            params.append(arg)
    
    path = os.path.join(os.getcwd(), "../conf/calcoloProfStd.json")
    config_jobs = load_config(path)

    # Creare spark context
    sc, sqlCtx = set_spark_context("GAS - Elaboration", mode)

    # Per ogni funzione esegue il RUN
    for function_input in functions_array:
        # Recupera l'oggetto function
        function = switcher(function_input, conf = config_jobs)
        if (function != None):
            print("##################################\n Run function: " + function_input)
            function.run(sc, sqlCtx, params)
        else:
            print("Error: Function parameter not found")

def stampa(value):
    print("***** ", value)

def load_config(path):
    """
    Load configuration file with all the needed parameters
    """
    with open(path, 'r') as conf_file:
        conf = json.load(conf_file)
    return conf

if __name__ == "__main__":
    main(sys.argv[1:])