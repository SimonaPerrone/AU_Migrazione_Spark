#!/usr/bin/python

import sys
import os.path
import os
import time
import zipfile
import shutil
import logging

VERSION = 1
PATH = "/mnt/isilonshare1/GAS_INJ/"
loading = ["|", "/", "-", "\\"]
ZIP = ".zip"


def help():
    print("""
        
    """)


def usage():
    print("\nVersion:{}\n Usage: python check_files FILENAME_INPUT FILENAME_OUTPUT"
          .format(VERSION))


def copy_file(filename, file_extension, dir_dest):
    try:
        if file_extension.lower() == ZIP:
            # print("unzip file:{} directory dest:{}".format(filename, dir_dest))
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(dir_dest)
        else:
            # print("copy file:{} directory dest:{}".format(filename, dir_dest))
            shutil.copy2(filename, dir_dest)
    except:
        print("Err: {}".format(filename))


def get_files(filename, file_extension):
    try:
        basename = os.path.dirname(filename)
        if file_extension.lower() == ZIP:
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                return [
                    item
                    for item in zip_ref.namelist()
                ]
        else:
            return ["{}".format(os.path.basename(filename))]

    except:
        print("Err: {}".format(filename))
        return []


def recupero_files(argv):
    if argv[1] == "-h" or argv[1] == "--help":
        usage()
        help()
        return

    # Recupero nome del file di input
    input_file = argv[1]
    output_file = argv[2]
    output_file2 = argv[3]

    # Verifico se il file di input e' presente
    if not os.path.isfile(input_file):
        print("File not exist ({})".format(input_file))
        usage()
        return

    fs_output = open(output_file, "w")
    fs_output_file = open(output_file2, "w")
    fp = open(input_file)
    print("Open file input: {}".format(input_file))
    print("Open file output: {}".format(output_file))

    i = 0
    with open(input_file) as fp:
        line = fp.readline().replace('\n', '').replace('\r', '')
        # print("line:{}".format(line))
        while line:
            #   try
            # print("line:{}".format(line))
            if ".xml" in (line.split(",")[0]).lower():
                dir_dest = "{}/".format(os.path.dirname(line.split(",")[0]))
            else:
                dir_dest = "{}/".format(line.split(",")[0])
            full_filename = line.split(",")[1]
            # print (dir_dest)
            # print (full_filename)

            filename, file_extension = os.path.splitext(full_filename)

            if not os.path.exists(dir_dest):
                os.makedirs(dir_dest)

            for f in get_files(full_filename, file_extension):
                # print("full_filename")
                sys.stdout.write("{}\tNumero file copiati: {}\r".format(loading[i % 4], i))
                sys.stdout.flush()
                fs_output.write(os.path.normpath("{}{}\n".format(dir_dest, f)))
                fs_output_file.write(os.path.normpath("{},{}\n".format(full_filename, dir_dest + "/" + f)))

            copy_file(full_filename, file_extension, dir_dest)

            line = fp.readline().replace('\n', '').replace('\r', '')
            i += 1

    print("Numero file: {}\n".format(i))


if __name__ == "__main__":
    recupero_files(sys.argv)
