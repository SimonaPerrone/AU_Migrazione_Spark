package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.model.TFCMetadata
import it.eng.au.ammissibilitaSettlementGas.utility.Constants.{COD_209, MOTIVAZIONE_ALREADY_TRANSMITTED}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD

object AlreadyTransmittedTFCController {
  def getAlreadyTransmitted(TFCMetadata: RDD[TFCMetadata], alreadyTransmittedTFC: RDD[String]): RDD[TFCMetadata] = {
    if (Properties.isRuleAlreadyTransmittedTFCEnabled) {
      val alreadyTransmittedTFCJoin = alreadyTransmittedTFC.map(TFCFileName => (TFCFileName, ""))
      TFCMetadata
        .keyBy(_.file.getName)
        .leftOuterJoin(alreadyTransmittedTFCJoin)
        .map({ case (tfcName, (tfcMetadata, zipFileName)) =>
          if (zipFileName.isDefined) {
            tfcMetadata.copy(isAlreadyTransmitted = true, isAmmissibile = false,
              statusCode = COD_209, statusMessage = MOTIVAZIONE_ALREADY_TRANSMITTED)
          } else tfcMetadata
        })
    }
    else TFCMetadata
  }
}
