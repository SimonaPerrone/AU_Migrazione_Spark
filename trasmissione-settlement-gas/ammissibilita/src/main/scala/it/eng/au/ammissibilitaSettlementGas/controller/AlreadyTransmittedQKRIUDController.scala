package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.model.QKRIUDMetadata
import it.eng.au.ammissibilitaSettlementGas.utility.Constants._
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD

object AlreadyTransmittedQKRIUDController {
  def getAlreadyTransmitted(QKRIUDMetadata: RDD[QKRIUDMetadata], alreadyTransmittedQKRIUD: RDD[String]): RDD[QKRIUDMetadata] = {
    if (Properties.isRuleAlreadyTransmittedQKRIUDEnabled) {
      val alreadyTransmittedQKRIUDJoin = alreadyTransmittedQKRIUD.map(QKRIUDFileName => (QKRIUDFileName, ""))
      QKRIUDMetadata
        .keyBy(_.file.getName)
        .leftOuterJoin(alreadyTransmittedQKRIUDJoin)
        .map({ case (qkriudName, (qkriudMetadata, zipFileName)) =>
          if (zipFileName.isDefined) {
            qkriudMetadata.copy(isAlreadyTransmitted = true, isAmmissibile = false,
              statusCode = COD_005, statusMessage = MOTIVAZIONE_ALREADY_TRANSMITTED2)
          } else qkriudMetadata
        })
    }
    else QKRIUDMetadata
  }
}
