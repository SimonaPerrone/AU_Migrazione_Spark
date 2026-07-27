create external table ${agg.db}.validated_flows_agg
(
   service string,
   pdr string,
   date timestamp,
   measure double,
   converted double,
   serialNumberMis string,
   serialNumberConv string,
   localFile string,
   dataCaricamento timestamp,
   isValid string,
   outcome string,
   readType string,
   motivation int,
   treatment string,
   codProfilo string,
   nCoeffCor double,
   gruppoMisInt string,
   tPreConv string,
   calcCoeff double,
   idRegioneClimatica string,
   isCorrected boolean,
   segnanteForcingFlag string,
   cauIntMis int,
   cauIntCorr int,
   classeMisuratore string
) partitioned by (session string, executionid bigint)
stored as parquet
location '${agg.output.rootpath}/validated_flows_agg'
