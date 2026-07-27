create external table ${agg.db}.segment_agg
(
    pdr string,
    
    startService string,
    startDate timestamp,
    startMeasure double,
    startConverted double,
    startSerialNumberMis string,
    startSerialNumberConv string,
    startLocalFile string,
    startPivaDistr string,
    startTreatment string,
    startTCodProfilo string,
    startIdRegioneClimatica int,
    startTPreConv string,
    startGruppoMisInt string,
    startCoeff double,
    startSegnante string,
    startSegnanteForcingFlag string,
    
    endService string,
    endDate timestamp,
    endMeasure double,
    endConverted double,
    endSerialNumberMis string,
    endSerialNumberConv string,
    endLocalFile string,
    endPivaDistr string,
    endTreatment string,
    endTCodProfilo string,
    endIdRegioneClimatica int,
    endTPreConv string,
    endGruppoMisInt string,
    endCoeff double,
    endSegnante string,
    endSegnanteForcingFlag string
) partitioned by (session string, executionid bigint)
stored as parquet
location '${agg.output.rootpath}/segment_agg'
