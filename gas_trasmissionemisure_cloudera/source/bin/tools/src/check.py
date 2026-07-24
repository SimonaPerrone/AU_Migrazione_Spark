#!/usr/bin/python

import sys
import os.path

VERSION = 1


def help():
    print("""
        Dato in ingresso un file contenente la lista dei file, il tool verifica 
        la presenza di ogni file all'interno del file system. 
        In output generera' dei file:
        - file DAT: contiene la lista dei file che sono presenti nel file system
        - file ERR: contiene la lista dei file non presenti nel file system
    """)


def usage():
    print("Check file\nVersion:{}\n Usage: python check_files FILENAME_INPUT FILENAME_OUTPUT"
          .format(VERSION))


def check_function(argv):
    if argv[1] == "-h" or argv[1] == "--help":
        help()
        return

    # Recupero nome del file di input
    input_file = argv[1]

    # Recupero nome del file di output
    output_file = argv[2] + ".dat"

    # Genero il nome del file di error
    error_file = argv[2] + ".err"

    # Verifico se il file di input e' presente
    if not os.path.isfile(input_file):
        print("File not exist ({})".format(input_file))
        usage()
        return

    fs_output = open(output_file, "w")
    fs_error = open(error_file, "w")

    cnt = 0
    cnt_exist = 0
    cnt_not_exist = 0
    # Apro il file di input
    with open(input_file) as fp:
        line = fp.readline().replace('\n', '').replace('\r', '')
        while line:
            filename = line.split(",")[-1]
            sys.stdout.write("\rCount:{} \t Exist:{} \t Not Exist:{}".format(cnt, cnt_exist, cnt_not_exist))
            # print("{}".format(filename))
            sys.stdout.flush()
            if os.path.isfile(filename):
                cnt_exist += 1
                fs_output.write("{}\n".format(line))
            else:
                cnt_not_exist += 1
                fs_error.write("{}\n".format(filename))

            # print("Line {}: {}".format(cnt, line.strip()))
            line = fp.readline().replace('\n', '').replace('\r', '')
            cnt += 1

    print("\nFile Outputs: \n{}\n{}".format(output_file, error_file))


if __name__ == "__main__":
    check_function(sys.argv)
