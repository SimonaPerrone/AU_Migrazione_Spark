#!/usr/bin/python
import getopt
import sys

import Ingestion
import Params

version = '1.0'


def usage():
    print("""
Version {}\n
    """.format(version))


def main(argv):

    verbose = False
    mode = None
    flusso = None
    query = None
    query_file = None
    anno = None
    mese = None
    giorno = None
    enableTestMode = False

    print("Start Application")

    try:
        # h = Help
        # v = Verbose
        # q = query sql
        # m = mode (local, yarn)
        # f = flusso
        # e = query_file
        # t = test
        params = "hvm:q:f:e:t"
        params_f = ["mode=", "query=", "query_file=", "flusso=", "giorno=", "anno=", "mese="]
        opts, args = getopt.getopt(argv, params, params_f)

    except getopt.GetoptError as err:
        usage()
        print("Err: {}".format(err))
        sys.exit(2)

    for opt, arg in opts:
        print("Param {} = {}".format(opt, arg))

        if opt == '-h':
            usage()
            sys.exit()
        elif opt == '-t':
            enableTestMode = True
        elif opt == '-v':
            verbose = True
        elif opt in ('-m', '--mode'):
            mode = arg
        elif opt in ('-f', '--flusso'):
            flusso = arg
        elif opt in ('-q', '--query'):
            query = arg
        elif opt in ('-e', '--query_file'):
            query_file = arg
        elif opt in '--giorno':
            giorno = arg
        elif opt in '--anno':
            anno = arg
        elif opt in '--mese':
            mese = arg

    params_ingestion = Params.Params(flusso, anno, mese, giorno, query, query_file, mode, verbose, enableTestMode)
    params_ingestion.print_debug()
    ingestion = Ingestion.Ingestion(params_ingestion)
    ingestion.start()
    print("Ingestion Completed")


if __name__ == "__main__":
    main(sys.argv[1:])

