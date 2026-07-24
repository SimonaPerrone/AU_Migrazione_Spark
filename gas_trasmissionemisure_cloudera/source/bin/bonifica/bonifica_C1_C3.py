import getopt
import sys
import os
import json
import shutil
import datetime
import time


def main(argv):
    version = '1.0'

    # Apertura file di input
    directorySRC = "/mnt/isilonshare1/TISG_SAG1/bonifica/"
    listFiles = [os.path.join(directorySRC, f) for f in os.listdir(directorySRC)]
    size_files = len(listFiles)
    print("Numero files: {}".format(size_files))
    counter = 1
    for f in listFiles:
        if ".new" in f:
            continue

        file = open(f, "r")
        file_new = open(f + ".new", "w")
        print("File: {}".format(file_new))

        line = file.readline()
        file_new.write(line)
        line = file.readline()
        while line:
            if len(line.strip()) <= 0:
                line = file.readline()
                continue
            items = line.split(";")
            pdr = str(items[0])
            pdr_new = pdr
            if len(pdr) > 11:
                pdr_new = str(items[0].rjust(14, '0'))

            consum = items[2].split(",")[0]
            line_new = line.replace(items[0], pdr_new)
            line_new2 = line_new.replace(items[2], consum)
            # line_new = line.replace(items[0], pdr_new)
            file_new.write(line_new2)
            # file_new.write("\n")
            line = file.readline()

        file.close()
        file_new.close()
        counter = counter + 1


if __name__ == "__main__":
    main(sys.argv[1:])
