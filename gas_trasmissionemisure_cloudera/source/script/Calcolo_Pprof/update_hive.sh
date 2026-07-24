hadoop fs -rm -skipTrash /user/hive/warehouse/settle_gas.db/TAB_FATT_TFC
hadoop fs -put ./Output/tfc.csv /user/hive/warehouse/settle_gas.db/TAB_FATT_TFC

hadoop fs -rm -skipTrash /user/hive/warehouse/settle_gas.db/TAB_FATT_C1
hadoop fs -put ./Output/c1.csv /user/hive/warehouse/settle_gas.db/TAB_FATT_C1


hadoop fs -rm -skipTrash /user/hive/warehouse/settle_gas.db/TAB_FATT_T1
hadoop fs -put ./Output/t1.csv /user/hive/warehouse/settle_gas.db/TAB_FATT_T1

hadoop fs -rm -skipTrash /user/hive/warehouse/settle_gas.db/TAB_FATT_ C2C4
hadoop fs -put ./Output/c2c4.csv /user/hive/warehouse/settle_gas.db/TAB_FATT_ C2C4

hadoop fs -rm -skipTrash /user/hive/warehouse/settle_gas.db/ TAB_PARAMETRI_CARATTERISTICI_PROF_PREL
hadoop fs -put ./Input/CPROF/*.csv /user/hive/warehouse/settle_gas.db/ TAB_PARAMETRI_CARATTERISTICI_PROF_PREL

