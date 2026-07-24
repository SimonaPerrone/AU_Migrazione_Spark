#!/bin/bash
PATH_APP="/mnt/isilonshare1/Software/GAS"

INPUT=$1
OUTPUT=$2
python ${PATH_APP}/bin/tools/src/check.py ${INPUT} ${OUTPUT}
