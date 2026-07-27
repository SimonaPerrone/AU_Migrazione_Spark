package it.eng.au.aggiustamentoGas.schema.agg

import it.eng.au.aggiustamentoGas.schema.SchemaEnum
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object SegmentSchema extends SchemaEnum {
  val
  pdr,

  startService,
  startDate,
  startMeasure,
  startConverted,
  startSerialNumberMis,
  startSerialNumberConv,
  startLocalFile,
  startPivaDistr,
  startTreatment,
  startTCodProfilo,
  startIdRegioneClimatica,
  startTPreConv,
  startGruppoMisInt,
  startCoeff,
  startSegnante,
  startSegnanteForcingFlag,

  endService,
  endDate,
  endMeasure,
  endConverted,
  endSerialNumberMis,
  endSerialNumberConv,
  endLocalFile,
  endPivaDistr,
  endTreatment,
  endTCodProfilo,
  endIdRegioneClimatica,
  endTPreConv,
  endGruppoMisInt,
  endCoeff,
  endSegnante,
  endSegnanteForcingFlag
  = Value

  val schema: StructType = StructType(List(
    StructField(pdr, StringType),
    StructField(startService, StringType),
    StructField(startDate, TimestampType),
    StructField(startMeasure, DoubleType),
    StructField(startConverted, DoubleType),
    StructField(startSerialNumberMis, StringType),
    StructField(startSerialNumberConv, StringType),
    StructField(startLocalFile, StringType),
    StructField(startPivaDistr, StringType),
    StructField(startTreatment, StringType),
    StructField(startTCodProfilo, StringType),
    StructField(startIdRegioneClimatica, IntegerType),
    StructField(startTPreConv, StringType),
    StructField(startGruppoMisInt, StringType),
    StructField(startCoeff, DoubleType),
    StructField(startSegnante, StringType),
    StructField(startSegnanteForcingFlag, StringType),

    StructField(endService, StringType),
    StructField(endDate, TimestampType),
    StructField(endMeasure, DoubleType),
    StructField(endConverted, DoubleType),
    StructField(endSerialNumberMis, StringType),
    StructField(endSerialNumberConv, StringType),
    StructField(endLocalFile, StringType),
    StructField(endPivaDistr, StringType),
    StructField(endTreatment, StringType),
    StructField(endTCodProfilo, StringType),
    StructField(endIdRegioneClimatica, IntegerType),
    StructField(endTPreConv, StringType),
    StructField(endGruppoMisInt, StringType),
    StructField(endCoeff, DoubleType),
    StructField(endSegnante, StringType),
    StructField(endSegnanteForcingFlag, StringType)
  ))
}
