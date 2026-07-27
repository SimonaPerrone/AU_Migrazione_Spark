#!/bin/bash
cd /home/acutest/20190826_GAS/bin
./SAG1_debug.sh -i /home/acutest/20190826_GAS/bin -p 1 &&\
hive -f sag1_verifiche.sql


#cd /mnt/isilonshare1/TISG_SAG1

#./toCSV_v3.sh


cd -
