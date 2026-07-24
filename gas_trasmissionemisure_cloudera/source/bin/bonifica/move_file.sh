#!/bin/bash
SRC=/mnt/isilonshare1/TISG_SAG1/bonifica/
DEST=/mnt/isilonshare1/TISG_SAG1/

cd $SRC

#for f in `ls $SRC/*.new`
for f in `find $SRC/ -name '*.new'`
do
     echo $f 
     mv $f $DEST`echo $(basename $f) | sed -e 's/\..*$//'`.csv
done
